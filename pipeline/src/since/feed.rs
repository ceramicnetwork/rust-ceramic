use std::{any::Any, sync::Arc};

use arrow_schema::SchemaRef;
use async_stream::try_stream;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::exec_datafusion_err,
    datasource::TableType,
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    logical_expr::TableProviderFilterPushDown,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
        ExecutionPlan, Partitioning, PlanProperties,
    },
    prelude::Expr,
    scalar::ScalarValue,
};
use futures::TryStreamExt as _;

// A source for a streaming table.
//
// A call to [`Self::subscribe`] followed by a call to [`Self::since`] must produce all data where "conclusion_event_order" is greater
// than the highwater_mark. Duplicate data may be produced between the two calls.
//
// Subsequent data batches from subscribe must be in increasing "conclusion_event_order" order.
//
// TODO add error handling
#[async_trait]
pub trait FeedTableSource: Clone + std::fmt::Debug + Sync + Send + 'static {
    fn schema(&self) -> SchemaRef;
    // Subscribe to all new data for this table in increasing "conclusion_event_order" order since offset.
    // All received RecordBatches must contain and be ordered by an "conclusion_event_order" u64 column.
    // The projection is a list of column indexes that should be produced.
    async fn subscribe_since(
        &self,
        projection: Option<Vec<usize>>,
        offset: Option<u64>,
        limit: Option<usize>,
    ) -> anyhow::Result<SendableRecordBatchStream>;
}

/// A table that when queried produces an unbounded stream of data.
/// It is assumed that the table contains an "conclusion_event_order" column and new data arrives in increasing
/// "conclusion_event_order" order.
#[derive(Debug)]
pub struct FeedTable<S> {
    source: S,
}
impl<S> FeedTable<S> {
    pub fn new(source: S) -> Self {
        Self { source }
    }
    fn highwater_mark_from_expr(expr: &Expr) -> Option<u64> {
        let find_highwater_mark = |col: &Expr, lit: &Expr| {
            col.try_as_col()
                .is_some_and(|column| column.name == "conclusion_event_order")
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

#[async_trait]
impl<S: FeedTableSource> TableProvider for FeedTable<S> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.source.schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let schema = projection
            .map(|projection| self.schema().project(projection))
            .transpose()?
            .map(Arc::new)
            .unwrap_or_else(|| self.schema());
        Ok(Arc::new(StreamExec {
            source: self.source.clone(),
            projection: projection.cloned(),
            offset: filters
                .iter()
                .filter_map(Self::highwater_mark_from_expr)
                .next()
                .map(|hm| hm as i64)
                .unwrap_or(0),
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Unbounded {
                    requires_infinite_memory: false,
                },
            ),
        }))
    }
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

#[derive(Debug)]
pub struct StreamExec<S> {
    source: S,
    projection: Option<Vec<usize>>,
    offset: i64,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl<S: FeedTableSource> ExecutionPlan for StreamExec<S> {
    fn name(&self) -> &str {
        "StreamExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
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

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let source = self.source.clone();
        let projection = self.projection.clone();
        let offset = self.offset as u64;
        let limit = self.limit;
        let stream = try_stream! {
            let mut stream = source.subscribe_since(projection, Some(offset), limit).await?;
            while let Some(batch) = stream.try_next().await? {
                yield batch
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            // Use the projected schema
            self.schema(),
            stream.map_err(|err: anyhow::Error| exec_datafusion_err!("{err}")),
        )))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

impl<S> datafusion::physical_plan::DisplayAs for StreamExec<S> {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        // TODO add useful information about predicates etc
        write!(f, "StreamExec")
    }
}
