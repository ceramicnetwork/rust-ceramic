//! Provides helpers for actors to implement subscriptions since an offset to their data.
//! Actors should implement [`ceramic_actor::Handler`] for the [`SubscribeSinceMsg`] in order to
//! provide a subscription since behavior.
//!
//! Actor handles may implement [`StreamTableSource`] and register a [`StreamTable`] on the [`datafusion::execution::context::SessionContext`] in order to provide query access to the stream.

mod feed;
mod metrics;

use std::sync::Arc;

pub use feed::{FeedTable, FeedTableSource};

use arrow::{
    array::{BooleanArray, RecordBatch, UInt64Array},
    compute::{filter_record_batch, kernels::aggregate},
};
use arrow_schema::SchemaRef;
use async_stream::try_stream;
use ceramic_actor::Message;
use datafusion::{
    common::DFSchema,
    execution::context::ExecutionProps,
    physical_plan::PhysicalExpr,
    prelude::{Expr, SessionContext},
};
use datafusion::{
    common::{cast::as_uint64_array, exec_datafusion_err},
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt as _;

/// Retrieve a subscription to all new rows produced after the offset.
/// This subscription produces an unbounded stream of data.
#[derive(Debug, PartialEq, Eq)]
pub struct SubscribeSinceMsg {
    /// Optional set of columns to fetch. Columns are indicated by their index into the schema of
    /// the actor table.
    pub projection: Option<Vec<usize>>,
    /// Optional filters to apply to the query (typically will include `sort_column > highwater_mark`)
    pub filters: Option<Vec<Expr>>,
    /// Maxium number of rows to return.
    /// When None the subscription is unbounded and never completes.
    pub limit: Option<usize>,
}
impl Message for SubscribeSinceMsg {
    type Result = anyhow::Result<SendableRecordBatchStream>;
}

/// Input parameters for the `rows_since` function.
pub(crate) struct RowsSinceInput<'a> {
    /// Session context for query execution
    pub session_context: &'a SessionContext,
    /// Schema for the record batches
    pub schema: SchemaRef,
    /// Column used for ordering records
    pub order_col: &'a str,
    /// Optional set of columns to fetch by index
    pub projection: Option<Vec<usize>>,
    /// Optional filters to apply
    pub filters: Option<Vec<Expr>>,
    /// Maximum number of rows to return
    pub limit: Option<usize>,
    /// Unbounded stream of subscription updates
    pub subscription: SendableRecordBatchStream,
    /// Finite stream of existing rows since the offset
    pub since: SendableRecordBatchStream,
}

/// Construct a stream of rows since the offset.
///
/// Two streams must be provided to this function:
///     since: a finite stream of rows starting at the offset and containing all known data since
///     subscription: an unbounded stream of rows starting at least where the since stream ended
///     and continuing forever.
///
/// The subscription stream may overlap with the since stream but it must not leave a gap between
/// them. This function will ensure no duplicate rows are produced. Ideally, we'd have the `since` stream
/// apply the filters, but currently it sources all events and we filter after the fact.
///
/// This method is helpful in implementing [`ceramic_actor::Handler`] for the [`SubscribeSinceMsg`]
/// on the actor.
pub fn rows_since(input: RowsSinceInput<'_>) -> anyhow::Result<SendableRecordBatchStream> {
    let RowsSinceInput {
        session_context,
        schema,
        order_col,
        projection,
        filters,
        mut limit,
        mut subscription,
        mut since,
    } = input;
    tracing::trace!(
        ?schema,
        ?order_col,
        ?projection,
        ?limit,
        "Starting rows_since stream"
    );
    let mut offset = None;
    let order_col = order_col.to_owned();
    let schema_cln = schema.clone();
    let execution_props = session_context.state().execution_props().to_owned();
    let stream = try_stream! {
        // Produce existing events
        tracing::trace!("Processing existing events from 'since' stream");
        while let Some(batch) = since.try_next().await? {
            tracing::trace!(rows = batch.num_rows(), ?offset, %order_col, "Processing batch from 'since' stream");
            offset = aggregate::max(as_uint64_array(
                batch.column_by_name(&order_col).ok_or_else(|| {
                    anyhow::anyhow!("ordering column '{order_col}' should exist on record batch")
                })?,
            )?);
            tracing::trace!(?offset, "Updated offset from batch");

            let num_rows = batch.num_rows();
            if num_rows > 0 {
                let projected_batch = project_limit_batch(&projection, &mut limit, batch)?;
                tracing::trace!(
                    input_rows = %num_rows,
                    output_rows = projected_batch.num_rows(),
                    ?limit,
                    "Projected and limited batch from 'since' stream"
                );
                yield projected_batch;
                if let Some(limit) = limit {
                    if limit == 0 {
                        tracing::trace!("Reached row limit in 'since' stream");
                        return
                    }
                }
            }
        }

        let physical_exprs = filters.as_ref().map(|filters| build_physical_exprs(filters, schema_cln.clone(), &execution_props)).transpose()?.flatten();

        // Produce new events as they arrive (make sure to filter before pushing them to caller)
        tracing::trace!("Starting subscription stream processing");
        while let Some(mut batch) = subscription.try_next().await? {
            tracing::trace!(
                rows = batch.num_rows(),
                ?offset,
                "Processing batch from subscription stream"
            );

            // Skip any duplicate events by filtering based on last seen value
            if let Some(o) = offset {
                let order = batch.column_by_name(&order_col).ok_or_else(|| {
                    anyhow::anyhow!("ordering column '{order_col}' should exist on events record batch")
                })?;
                // Make sure to get all rows as we can filter them out later if needed
                let predicate = arrow::compute::kernels::cmp::gt(&order, &UInt64Array::new_scalar(o))?;
                let original_rows = batch.num_rows();
                batch = filter_record_batch(&batch, &predicate)?;
                tracing::trace!(
                    original_rows,
                    filtered_rows = batch.num_rows(),
                    ?o,
                    "Filtered batch by last seen value"
                );
                if batch.num_rows() == 0 {
                    continue;
                }
                // Found new data, no need to filter anymore
                offset = None;
            }

            let num_rows = batch.num_rows();
            if num_rows > 0 {
                if let Some(expr_list) = &physical_exprs {
                    let mut mask = None;
                    for expr in expr_list {
                        let result = expr.evaluate(&batch)?;
                        tracing::trace!(?expr, "Evaluating expression");
                        let bool_array = result.into_array(batch.num_rows())?;
                        if let Some(array) = bool_array.as_any().downcast_ref::<BooleanArray>() {
                            mask = match mask {
                                Some(current_mask) => Some(arrow::compute::and(&current_mask, array)?),
                                None => Some(array.clone()),
                            };
                        } else {
                            Err(anyhow::anyhow!("Failed to evaluate expression as it did not evaluate to boolean array: {}", expr))?;
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
                batch = project_limit_batch(&projection, &mut limit, batch)?;
                tracing::trace!(
                    input_rows = num_rows,
                    output_rows = batch.num_rows(),
                    ?limit,
                    "Projected and limited batch from subscription stream"
                );
                yield batch;
                if let Some(limit) = limit {
                    if limit == 0 {
                        tracing::trace!("Reached row limit in subscription stream");
                        return
                    }
                }
            }
        }
        tracing::trace!("Stream completed");
    };
    let stream = stream.map_err(|err: anyhow::Error| exec_datafusion_err!("{err}"));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

/// Create a greater than expression for comparing event orders
pub(crate) fn gt_expression(order_col: &str, offset: u64) -> Expr {
    datafusion::prelude::col(order_col).gt(datafusion::prelude::lit(offset))
}

fn build_physical_exprs(
    filters: &[Expr],
    schema: SchemaRef,
    props: &ExecutionProps,
) -> anyhow::Result<Option<Vec<Arc<dyn PhysicalExpr>>>> {
    if filters.is_empty() {
        return Ok(None);
    }
    tracing::trace!(?filters, ?schema, "Creating filter expressions");
    let df_schema = match DFSchema::try_from(schema.clone()) {
        Ok(df_schema) => df_schema,
        Err(e) => {
            tracing::warn!(error = ?e, "Failed to create DFSchema from Arrow schema");
            anyhow::bail!("Failed to create DFSchema from Arrow schema: {e}");
        }
    };

    let mut expr_list = Vec::new();
    for filter in filters {
        match datafusion::physical_expr::create_physical_expr(filter, &df_schema, props) {
            Ok(phys_expr) => {
                tracing::trace!(?filter, "Created physical expression");
                expr_list.push(phys_expr);
            }
            Err(e) => {
                tracing::info!(?filter, error=?e, "Failed to create physical expression");
                anyhow::bail!(
                    "Failed to create physical expression for filter: {filter}. Error: {e}"
                );
            }
        }
    }

    tracing::trace!(
        expr_count = expr_list.len(),
        "Created filter expressions and applying to batch"
    );
    Ok(Some(expr_list))
}

fn project_limit_batch(
    projection: &Option<Vec<usize>>,
    limit: &mut Option<usize>,
    batch: RecordBatch,
) -> anyhow::Result<RecordBatch> {
    tracing::trace!(
        ?projection,
        ?limit,
        rows = batch.num_rows(),
        "Starting project_limit_batch"
    );
    let batch = if let Some(projection) = projection {
        batch.project(projection)?
    } else {
        batch
    };

    let num_rows = batch.num_rows();
    let result = if let Some(ref mut limit) = limit {
        if num_rows > *limit {
            tracing::trace!(batch_rows = num_rows, ?limit, "Truncating batch to limit");
            let b = batch.slice(0, *limit);
            *limit = 0;
            b
        } else {
            tracing::trace!(batch_rows = num_rows, ?limit, "Using full batch");
            *limit -= num_rows;
            batch
        }
    } else {
        batch
    };

    tracing::trace!(
        input_rows = num_rows,
        output_rows = result.num_rows(),
        "Completed project_limit_batch"
    );
    Ok(result)
}
