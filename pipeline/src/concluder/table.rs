use std::{any::Any, cmp::min, sync::Arc};

use arrow_schema::SchemaRef;
use async_stream::try_stream;
use datafusion::{
    catalog::{Session, TableProvider},
    common::exec_datafusion_err,
    datasource::TableType,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, ExecutionMode, ExecutionPlan, PlanProperties,
    },
    scalar::ScalarValue,
};
use futures::TryStreamExt as _;
use tracing::{instrument, Level};

use crate::{concluder::conclusion_events_to_record_batch, schemas::conclusion_events};

use super::ConclusionEvent;

// Default number of rows to query from sqlite table each batch.
const DEFAULT_BATCH_SIZE: i64 = 10_000;

/// A ConclusionFeed provides access to [`ConclusionEvent`]s.
#[async_trait::async_trait]
pub trait ConclusionFeed: std::fmt::Debug + Send + Sync {
    /// Report the maximum highwater_mark.
    async fn max_highwater_mark(&self) -> anyhow::Result<Option<u64>>;
    /// Produce a set of conclusion events up to the limit with an index greater than the highwater_mark
    /// Event must be returned in index order.
    async fn conclusion_events_since(
        &self,
        highwater_mark: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ConclusionEvent>>;
}
#[async_trait::async_trait]
impl<T: ConclusionFeed> ConclusionFeed for Arc<T> {
    async fn max_highwater_mark(&self) -> anyhow::Result<Option<u64>> {
        self.as_ref().max_highwater_mark().await
    }
    async fn conclusion_events_since(
        &self,
        highwater_mark: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ConclusionEvent>> {
        self.as_ref()
            .conclusion_events_since(highwater_mark, limit)
            .await
    }
}

// Implements the [`TableProvider`] trait producing a [`FeedExec`] instance when the table is
// scanned, which in turn calls into the [`ConclusionFeed`] to get the actual events.
#[derive(Debug)]
pub struct ConclusionFeedTable<T> {
    feed: Arc<T>,
    schema: SchemaRef,
    batch_size: i64,
}

impl<T> ConclusionFeedTable<T> {
    pub fn new(feed: Arc<T>) -> Self {
        Self {
            feed,
            schema: conclusion_events(),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }
    #[cfg(test)]
    fn with_batch_size(self, batch_size: i64) -> Self {
        Self { batch_size, ..self }
    }
    fn highwater_mark_from_expr(expr: &Expr) -> Option<u64> {
        let find_highwater_mark = |col: &Expr, lit: &Expr| {
            col.try_as_col()
                .map_or(false, |column| column.name == "index")
                .then(|| {
                    if let Expr::Literal(ScalarValue::UInt64(highwater_mark)) = lit {
                        highwater_mark.to_owned()
                    } else {
                        None
                    }
                })
                .flatten()
        };
        match expr {
            Expr::BinaryExpr(expr) => match expr.op {
                datafusion::logical_expr::Operator::Gt => {
                    find_highwater_mark(expr.left.as_ref(), expr.right.as_ref())
                }
                datafusion::logical_expr::Operator::LtEq => {
                    find_highwater_mark(expr.right.as_ref(), expr.left.as_ref())
                }
                _ => None,
            },
            _ => None,
        }
    }
}
#[async_trait::async_trait]
impl<T: ConclusionFeed + std::fmt::Debug + 'static> TableProvider for ConclusionFeedTable<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }
    #[instrument(skip(self,_state), ret(level = Level::DEBUG))]
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let schema = projection
            .map(|projection| self.schema.project(projection))
            .transpose()?
            .map(Arc::new)
            .unwrap_or_else(|| self.schema.clone());
        Ok(Arc::new(FeedExec {
            feed: self.feed.clone(),
            schema: schema.clone(),
            projection: projection.cloned(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
            highwater_mark: filters
                .iter()
                .filter_map(Self::highwater_mark_from_expr)
                .next()
                .map(|hm| hm as i64)
                .unwrap_or(0),
            limit: limit.map(|l| l as i64),
            batch_size: self.batch_size,
        }))
    }
    #[instrument(skip(self), ret(level = Level::DEBUG))]
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| Self::highwater_mark_from_expr(expr))
            .map(|highwater_mark| {
                if highwater_mark.is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

// Implements the [`ExecutionPlan`] trait in terms of a [`ConclusionFeed`].
// This allows calls to scan the `conclusion_feed` table to be mapped to calls into the
// [`ConclusionFeed`].
#[derive(Debug)]
struct FeedExec<T> {
    feed: Arc<T>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
    highwater_mark: i64,
    limit: Option<i64>,
    batch_size: i64,
}

impl<T> datafusion::physical_plan::DisplayAs for FeedExec<T> {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        // TODO add useful information about predicates etc
        write!(f, "FeedExec")
    }
}

impl<T: ConclusionFeed + std::fmt::Debug + 'static> ExecutionPlan for FeedExec<T> {
    fn name(&self) -> &str {
        "FeedExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let feed = self.feed.clone();
        let projection = self.projection.clone();
        let mut highwater_mark = self.highwater_mark;
        let mut remaining = self.limit;
        let batch_size = self.batch_size;
        let stream = try_stream! {
            loop {
                let limit = remaining.map(|r| min(r, batch_size)).unwrap_or(batch_size);
                if limit == 0 {
                    break
                }
                let events = feed.conclusion_events_since(highwater_mark, limit).await?;
                let count = events.len() as i64;
                let batch = conclusion_events_to_record_batch(&events)?;
                let batch = projection
                    .as_ref()
                    .map(|projection| batch.project(projection))
                    .transpose()?
                    .unwrap_or_else(|| batch);

                yield batch;

                if count < limit {
                    break
                }
                remaining = remaining.map(|r| r - count);
                highwater_mark = events[events.len()-1].index() as i64;
            }
        };
        let stream = stream.map_err(|err: anyhow::Error| exec_datafusion_err!("{err}"));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr as _, sync::Arc};

    use arrow::util::pretty::pretty_format_batches;
    use cid::Cid;
    use datafusion::prelude::SessionContext;
    use expect_test::expect;
    use mockall::predicate;
    use test_log::test;

    use crate::{
        concluder::table::ConclusionFeedTable, tests::MockConclusionFeed, ConclusionData,
        ConclusionEvent, ConclusionInit,
    };
    #[test(tokio::test)]
    async fn batching_no_limit() {
        const BATCH_SIZE: i64 = 3;
        let mut mock_feed = MockConclusionFeed::new();
        mock_feed
            .expect_conclusion_events_since()
            .once()
            .with(predicate::eq(0), predicate::eq(BATCH_SIZE))
            .return_once(|_, _| Ok(events(1, BATCH_SIZE)));
        mock_feed
            .expect_conclusion_events_since()
            .once()
            .with(predicate::eq(3), predicate::eq(BATCH_SIZE))
            .return_once(|_, _| Ok(events(4, BATCH_SIZE)));
        mock_feed
            .expect_conclusion_events_since()
            .once()
            .with(predicate::eq(6), predicate::eq(BATCH_SIZE))
            .return_once(|_, _| Ok(events(7, BATCH_SIZE - 1)));
        let table = ConclusionFeedTable::new(Arc::new(mock_feed)).with_batch_size(BATCH_SIZE);
        let ctx = SessionContext::new();
        let data = pretty_format_batches(
            &ctx.read_table(Arc::new(table))
                .unwrap()
                .select_columns(&["index"])
                .unwrap()
                .collect()
                .await
                .unwrap(),
        )
        .unwrap();
        expect![[r#"
            +-------+
            | index |
            +-------+
            | 1     |
            | 2     |
            | 3     |
            | 4     |
            | 5     |
            | 6     |
            | 7     |
            | 8     |
            +-------+"#]]
        .assert_eq(&data.to_string());
    }

    #[test(tokio::test)]
    async fn batching_with_gt_limit() {
        const BATCH_SIZE: i64 = 3;
        const LIMIT: usize = 5;
        let mut mock_feed = MockConclusionFeed::new();
        mock_feed
            .expect_conclusion_events_since()
            .once()
            .with(predicate::eq(0), predicate::eq(BATCH_SIZE))
            .return_once(|_, _| Ok(events(1, BATCH_SIZE)));
        mock_feed
            .expect_conclusion_events_since()
            .once()
            .with(predicate::eq(3), predicate::eq(LIMIT as i64 - BATCH_SIZE))
            .return_once(|_, _| Ok(events(4, LIMIT as i64 - BATCH_SIZE)));
        let table = ConclusionFeedTable::new(Arc::new(mock_feed)).with_batch_size(BATCH_SIZE);
        let ctx = SessionContext::new();
        let data = pretty_format_batches(
            &ctx.read_table(Arc::new(table))
                .unwrap()
                .select_columns(&["index"])
                .unwrap()
                .limit(0, Some(LIMIT))
                .unwrap()
                .collect()
                .await
                .unwrap(),
        )
        .unwrap();
        expect![[r#"
            +-------+
            | index |
            +-------+
            | 1     |
            | 2     |
            | 3     |
            | 4     |
            | 5     |
            +-------+"#]]
        .assert_eq(&data.to_string());
    }
    #[test(tokio::test)]
    async fn batching_with_lt_limit() {
        const BATCH_SIZE: i64 = 5;
        const LIMIT: usize = 3;
        let mut mock_feed = MockConclusionFeed::new();
        mock_feed
            .expect_conclusion_events_since()
            .once()
            .with(predicate::eq(0), predicate::eq(LIMIT as i64))
            .return_once(|_, _| Ok(events(1, LIMIT as i64)));
        let table = ConclusionFeedTable::new(Arc::new(mock_feed)).with_batch_size(BATCH_SIZE);
        let ctx = SessionContext::new();
        let data = pretty_format_batches(
            &ctx.read_table(Arc::new(table))
                .unwrap()
                .select_columns(&["index"])
                .unwrap()
                .limit(0, Some(LIMIT))
                .unwrap()
                .collect()
                .await
                .unwrap(),
        )
        .unwrap();
        expect![[r#"
            +-------+
            | index |
            +-------+
            | 1     |
            | 2     |
            | 3     |
            +-------+"#]]
        .assert_eq(&data.to_string());
    }
    #[test(tokio::test)]
    async fn batching_with_eq_limit() {
        const BATCH_SIZE: i64 = 3;
        let mut mock_feed = MockConclusionFeed::new();
        mock_feed
            .expect_conclusion_events_since()
            .once()
            .with(predicate::eq(0), predicate::eq(BATCH_SIZE))
            .return_once(|_, _| Ok(events(1, BATCH_SIZE)));
        let table = ConclusionFeedTable::new(Arc::new(mock_feed)).with_batch_size(BATCH_SIZE);
        let ctx = SessionContext::new();
        let data = pretty_format_batches(
            &ctx.read_table(Arc::new(table))
                .unwrap()
                .select_columns(&["index"])
                .unwrap()
                .limit(0, Some(BATCH_SIZE as usize))
                .unwrap()
                .collect()
                .await
                .unwrap(),
        )
        .unwrap();
        expect![[r#"
            +-------+
            | index |
            +-------+
            | 1     |
            | 2     |
            | 3     |
            +-------+"#]]
        .assert_eq(&data.to_string());
    }

    fn events(start_index: u64, count: i64) -> Vec<ConclusionEvent> {
        (0..count)
            .into_iter()
            .map(|i| {
                // Use the same event data as all we care about is the count and index
                ConclusionEvent::Data(ConclusionData {
                    index: start_index + i as u64,
                    event_cid: Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )
                    .unwrap(),
                    init: ConclusionInit {
                        stream_cid: Cid::from_str(
                            "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                        )
                        .unwrap(),
                        stream_type: 3,
                        controller: "did:key:bob".to_string(),
                        dimensions: vec![
                            ("controller".to_string(), b"did:key:bob".to_vec()),
                            ("model".to_string(), b"model".to_vec()),
                        ],
                    },
                    previous: vec![],
                    data: r#"{"metadata":{},"content":{"a":0}}"#.bytes().collect(),
                })
            })
            .collect()
    }
}
