//! Provides helpers for actors to implement subscriptions since an offset to their data.
//! Actors should implement [`ceramic_actor::Handler`] for the [`SubscribeSinceMsg`] in order to
//! provide a subscription since behavior.
//!
//! Actor handles may implement [`StreamTableSource`] and register a [`StreamTable`] on the [`datafusion::execution::context::SessionContext`] in order to provide query access to the stream.

mod stream;

pub use stream::{StreamTable, StreamTableSource};

use arrow::{
    array::{RecordBatch, UInt64Array},
    compute::{filter_record_batch, kernels::aggregate},
};
use arrow_schema::SchemaRef;
use async_stream::try_stream;
use ceramic_actor::Message;
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
    /// Offset into the actor table against the "index" column.
    pub offset: Option<u64>,
    /// Maxium number of rows to return.
    /// When None the subscription is unbounded and never completes.
    pub limit: Option<usize>,
}
impl Message for SubscribeSinceMsg {
    type Result = anyhow::Result<SendableRecordBatchStream>;
}

/// Construct a stream of rows since the offset.
///
/// Two streams must be provided to this function:
///     since: a finite stream of rows starting at the offset and containing all known data since
///     subscription: an unbounded stream of rows starting at least where the since stream ended
///     and continuing forever.
///
/// The subscription stream may overlap with the since stream but it must not leave a gap between
/// them. This function will ensure no duplicate rows are produced.
///
/// This method is helpful in implementing [`ceramic_actor::Handler`] for the [`SubscribeSinceMsg`]
/// on the actor.
pub fn rows_since(
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    mut offset: Option<u64>,
    mut limit: Option<usize>,
    mut subscription: SendableRecordBatchStream,
    mut since: SendableRecordBatchStream,
) -> anyhow::Result<SendableRecordBatchStream> {
    let stream = try_stream! {
        // Produce existing events
        while let Some(batch) = since.try_next().await? {
            offset = aggregate::max(as_uint64_array(
                batch.column_by_name("index").ok_or_else(|| {
                    anyhow::anyhow!("index column should exist on record batch")
                })?,
            )?);

            if batch.num_rows() > 0 {
                yield project_limit_batch(&projection, &mut limit, batch)?;
                if let Some(limit) = limit {
                    if limit == 0 {
                        return
                    }
                }
            }
        }

        // Produce new events as they arrive
        while let Some(mut batch) = subscription.try_next().await?{
            // Skip any duplicate events that arrived in the overlap between the subscription and
            // since streams.
            if let Some(o) = offset {
                let index = batch.column_by_name("index").ok_or_else(|| {
                    anyhow::anyhow!("index column should exist on events record batch")
                })?;
                let predicate = arrow::compute::kernels::cmp::gt(&index, &UInt64Array::new_scalar(o))?;
                batch = filter_record_batch(&batch, &predicate)?;
                if batch.num_rows() == 0 {
                    // Get the next batch as no rows from the current batch were greater than the
                    // offset.
                    continue;
                }
                // We have at least one row that is past the offset.
                // Data is ordered by index so we no longer need to filter based on the
                // offset.
                offset = None;
            }

            if batch.num_rows() > 0 {
                yield project_limit_batch(&projection, &mut limit, batch)?;
                if let Some(limit) = limit {
                    if limit == 0 {
                        return
                    }
                }
            }
        }
    };
    let stream = stream.map_err(|err: anyhow::Error| exec_datafusion_err!("{err}"));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

fn project_limit_batch(
    projection: &Option<Vec<usize>>,
    limit: &mut Option<usize>,
    batch: RecordBatch,
) -> anyhow::Result<RecordBatch> {
    tracing::debug!(?limit, rows = batch.num_rows(), "project_limit_batch");
    let batch = if let Some(projection) = projection {
        batch.project(projection)?
    } else {
        batch
    };
    Ok(if let Some(ref mut limit) = limit {
        if batch.num_rows() > *limit {
            let b = batch.slice(0, *limit);
            *limit = 0;
            b
        } else {
            *limit -= batch.num_rows();
            batch
        }
    } else {
        batch
    })
}
