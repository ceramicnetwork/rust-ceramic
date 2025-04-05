use std::{any::Any, sync::Arc};

use arrow::{array::BooleanArray, compute::filter_record_batch};
use arrow_schema::SchemaRef;
use async_stream::try_stream;
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{exec_datafusion_err, DFSchema},
    datasource::TableType,
    error::DataFusionError,
    execution::{context::ExecutionProps, SendableRecordBatchStream},
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

    // Extract predicates that can be pushed down to the source
    fn extract_pushdown_predicates(filters: &[Expr]) -> (Option<u64>, Vec<Expr>) {
        tracing::trace!(?filters, "Extracting pushdown predicates");
        let mut remaining_filters = Vec::new();
        let mut highwater_mark = None;

        for filter in filters {
            if let Some(mark) = Self::highwater_mark_from_expr(filter) {
                highwater_mark = Some(mark);
            } else {
                remaining_filters.push(filter.clone());
            }
        }

        tracing::trace!(?highwater_mark, ?remaining_filters, "Extracted predicates");
        (highwater_mark, remaining_filters)
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
            // we use >= for the offset later so we don't need to adjust the value here. if we get an extra, we ignore it
            Expr::BinaryExpr(expr) => match expr.op {
                datafusion::logical_expr::Operator::Gt
                | datafusion::logical_expr::Operator::GtEq => {
                    find_highwater_mark(expr.left.as_ref(), expr.right.as_ref())
                }
                datafusion::logical_expr::Operator::LtEq
                | datafusion::logical_expr::Operator::Lt => {
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
        let (highwater_mark, remaining_filters) = Self::extract_pushdown_predicates(filters);
        Ok(Arc::new(StreamExec {
            source: self.source.clone(),
            projection: projection.cloned(),
            offset: highwater_mark.map(|hm| hm as i64).unwrap_or(0),
            limit,
            filters: remaining_filters,
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
    offset: i64,
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
        let offset = self.offset as u64;
        let limit = self.limit;
        let filters = self.filters.clone();
        let schema = self.schema().clone();

        tracing::trace!(?offset, ?limit, ?filters, "Starting stream execution");

        let stream = try_stream! {
            let mut stream = source.subscribe_since(projection, Some(offset), limit).await?;

            let filter_exprs = if !filters.is_empty() {
                tracing::trace!(?filters, ?schema, "Creating filter expressions");
                let mut expr_list = Vec::new();
                if let Ok(df_schema) = DFSchema::try_from(schema.clone()) {
                    let props = ExecutionProps::new();
                    for filter in filters {
                        if let Ok(phys_expr) = datafusion::physical_expr::create_physical_expr(
                            &filter,
                            &df_schema,
                            &props,
                        ) {
                            tracing::trace!(?filter, "Created physical expression");
                            expr_list.push(phys_expr);
                        } else {
                            tracing::warn!("Failed to create physical expression: {filter}");
                        }
                    }
                    if expr_list.is_empty() {
                        tracing::trace!("No valid filter expressions created");
                        None
                    } else {
                        tracing::trace!(expr_count = expr_list.len(), "Created filter expressions");
                        Some(expr_list)
                    }
                } else {
                    tracing::warn!("Failed to create DFSchema from Arrow schema");
                    None
                }
            } else {
                None
            };

            tracing::trace!(?filter_exprs, "Starting to stream stream batches");

            while let Some(mut batch) = stream.try_next().await? {
                tracing::trace!(
                    rows = batch.num_rows(),
                    columns = batch.num_columns(),
                    "Received batch"
                );

                // Add filtering details
                if let Some(exprs) = &filter_exprs {
                    tracing::trace!(
                        filter_count = exprs.len(),
                        "Applying filters to batch"
                    );
                    // Evaluate all expressions to get a boolean array
                    let mut mask = None;
                    for expr in exprs {
                        let result = expr.evaluate(&batch)?;
                        tracing::trace!(?expr, "Evaluating expression");
                        let bool_array = result.into_array(batch.num_rows())?;
                        if let Some(array) = bool_array.as_any().downcast_ref::<BooleanArray>() {
                            mask = match mask {
                                Some(current_mask) => Some(arrow::compute::and(&current_mask, array)?),
                                None => Some(array.clone()),
                            };
                        } else {
                            tracing::warn!(?expr, "Expression did not evaluate to boolean array");
                        }
                    }

                    if let Some(mask) = mask {
                        batch = filter_record_batch(&batch, &mask)?;
                        tracing::trace!(
                            rows_after_filter = batch.num_rows(),
                            "Applied filters to batch"
                        );
                    }
                }

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
