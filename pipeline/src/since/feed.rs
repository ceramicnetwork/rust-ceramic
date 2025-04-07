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
};
use futures::TryStreamExt as _;

// A source for a streaming table.
//
// A call to [`Self::subscribe`] followed by a call to [`Self::since`] must produce all data where "sort_column" is greater
// than the highwater_mark. Duplicate data may be produced between the two calls.
//
// Subsequent data batches from subscribe must be in increasing "sort_column" order.
//
// TODO add error handling
#[async_trait]
pub trait FeedTableSource: Clone + std::fmt::Debug + Sync + Send + 'static {
    fn schema(&self) -> SchemaRef;

    // Subscribe to all new data for this table in increasing `sort_column` order since offset.
    // All received RecordBatches must contain and be ordered by an `sort_column` u64 column.
    // The projection is a list of column indexes that should be produced.
    async fn subscribe_since(
        &self,
        projection: Option<Vec<usize>>,
        filter: Option<Vec<Expr>>,
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
            limit,
            filters: filters.to_vec(),
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
            .map(|expr| match expr {
                Expr::BinaryExpr(_binary_expr) => TableProviderFilterPushDown::Exact,
                _ => TableProviderFilterPushDown::Inexact,
            })
            .collect())
    }
}

#[derive(Debug)]
pub struct StreamExec<S> {
    source: S,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    filters: Vec<Expr>,
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
        let limit = self.limit;
        let filters = self.filters.clone();

        tracing::trace!(?limit, ?filters, "Starting stream execution");

        let stream = try_stream! {
            let mut stream = source.subscribe_since(projection, Some(filters), limit).await?;

            while let Some(batch) = stream.try_next().await? {
                tracing::trace!(
                    rows = batch.num_rows(),
                    columns = batch.num_columns(),
                    "Received batch"
                );


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
