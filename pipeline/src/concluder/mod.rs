//! The concluder actor is reponsible for populating the `conclusion_events` table.
//! In its current form its is just a wrapper over the sqlite tables in order to expose them as
//! datafusion tables.
mod event;
#[cfg(any(test, feature = "mock"))]
pub mod mock;
mod table;

use std::{sync::Arc, time::Duration};

use arrow::{array::RecordBatch, compute::kernels::aggregate};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use ceramic_actor::{actor_envelope, Actor, ActorHandle, Handler, Message};
use datafusion::{
    common::{cast::as_uint64_array, exec_datafusion_err},
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
    prelude::{col, lit, SessionContext},
};
use futures::TryStreamExt as _;
use shutdown::{Shutdown, ShutdownSignal};
use table::FeedTable;
use tokio::{select, sync::broadcast, task::JoinHandle, time::interval};
use tracing::{debug, error, warn};

use crate::{
    schemas,
    since::{rows_since, StreamTable, StreamTableSource},
    ConclusionFeedSource, PipelineContext, Result, SessionContextRef,
};

// Use the SubscribeSinceMsg so its clear its a message for this actor
pub use crate::since::SubscribeSinceMsg;
pub use event::{
    conclusion_events_to_record_batch, ConclusionData, ConclusionEvent, ConclusionInit,
    ConclusionTime,
};
pub use table::ConclusionFeed;

const CONCLUSION_EVENTS_TABLE: &str = "ceramic.v0.conclusion_events";
const CONCLUSION_EVENTS_STREAM_TABLE: &str = "ceramic.v0.conclusion_events_stream";

/// Concluder is responsible for making conclusions about raw events and publishing
/// conclusion_events.
#[derive(Actor)]
pub struct Concluder {
    ctx: SessionContextRef,
    broadcast_tx: broadcast::Sender<RecordBatch>,
}
impl Concluder {
    /// Create and spawn a new Concluder
    pub async fn spawn_new<F: ConclusionFeed + 'static>(
        size: usize,
        ctx: &PipelineContext,
        feed: ConclusionFeedSource<F>,
        shutdown: Shutdown,
    ) -> Result<(ConcluderHandle, Vec<JoinHandle<()>>)> {
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(1_000);
        let actor = Concluder {
            ctx: ctx.session(),
            broadcast_tx,
        };
        let (handle, task_handle) = Self::spawn(size, actor, shutdown.wait_fut());
        // Register tables
        let last_processed_index = match feed {
            ConclusionFeedSource::Direct(conclusion_feed) => {
                ctx.session().register_table(
                    CONCLUSION_EVENTS_TABLE,
                    Arc::new(FeedTable::new(conclusion_feed.clone())),
                )?;
                conclusion_feed.max_highwater_mark().await?
            }
            #[cfg(test)]
            ConclusionFeedSource::InMemory(table) => {
                assert_eq!(
                    crate::schemas::conclusion_events(),
                    datafusion::catalog::TableProvider::schema(&table)
                );
                ctx.session()
                    .register_table(CONCLUSION_EVENTS_TABLE, Arc::new(table))?;
                let batches = ctx
                    .session()
                    .table(CONCLUSION_EVENTS_TABLE)
                    .await?
                    .aggregate(
                        vec![],
                        vec![datafusion::functions_aggregate::min_max::max(col("index"))
                            .alias("max_index")],
                    )?
                    .collect()
                    .await?;
                batches.get(0).and_then(|batch| {
                    batch.column_by_name("max_index").and_then(|index_col| {
                        as_uint64_array(&index_col).ok().and_then(|index_col| {
                            arrow::array::Array::is_valid(&index_col, 0).then(|| index_col.value(0))
                        })
                    })
                })
            }
        };
        ctx.session()
            .register_table(
                CONCLUSION_EVENTS_STREAM_TABLE,
                Arc::new(StreamTable::new(handle.clone())),
            )
            .expect("should be able to register table");

        // HACK: in a loop poll for new conclusion events.
        // This can go away once all layers below this point are also actors and do push based
        // logic.
        let poll_handle = handle.clone();
        let session = ctx.session();
        let sub_handle = tokio::spawn(async move {
            if let Err(err) = poll_new_events(
                poll_handle,
                session,
                last_processed_index,
                shutdown.wait_fut(),
            )
            .await
            {
                error!(%err, "poll_new_events loop failed")
            } else {
                debug!("poll_new_events task finished");
            }
        });

        Ok((handle, vec![task_handle, sub_handle]))
    }
}

actor_envelope! {
    ConcluderEnvelope,
    ConcluderActor,
    NewEvents => NewEventsMsg,
    SubscribeSince => SubscribeSinceMsg,
    EventsSince => EventsSinceMsg,

}

/// Notify actor of new events
#[derive(Debug)]
pub struct NewEventsMsg {
    /// Events as a record batch, must have the schema of the [`CONCLUSION_EVENTS_TABLE`].
    pub events: RecordBatch,
}
impl Message for NewEventsMsg {
    type Result = ();
}

#[async_trait]
impl Handler<NewEventsMsg> for Concluder {
    async fn handle(&mut self, message: NewEventsMsg) -> <NewEventsMsg as Message>::Result {
        debug!(num_rows = message.events.num_rows(), "new events");
        if let Err(err) = self.broadcast_tx.send(message.events) {
            warn!(%err, "failed to broadcast new conclusion events")
        }
    }
}

#[async_trait]
impl Handler<SubscribeSinceMsg> for Concluder {
    async fn handle(
        &mut self,
        message: SubscribeSinceMsg,
    ) -> <SubscribeSinceMsg as Message>::Result {
        let subscription = self.broadcast_tx.subscribe();
        let ctx = self.ctx.clone();
        rows_since(
            schemas::conclusion_events(),
            message.projection,
            message.offset.clone(),
            message.limit,
            Box::pin(RecordBatchStreamAdapter::new(
                schemas::conclusion_events(),
                tokio_stream::wrappers::BroadcastStream::new(subscription)
                    .map_err(|err| exec_datafusion_err!("{err}")),
            )),
            // Future Optimization can be to send the projection into the events_since call.
            events_since(&ctx, message.offset).await?,
        )
    }
}

async fn poll_new_events(
    handle: ConcluderHandle,
    ctx: SessionContextRef,
    mut last_processed_index: Option<u64>,
    mut shutdown: ShutdownSignal,
) -> anyhow::Result<()> {
    let mut interval = interval(Duration::from_millis(1_000));

    // Poll for new events until shutdown
    loop {
        select! {
            _ = &mut shutdown => {
                break Ok(());
            }
            _ = interval.tick() => {}
        };
        let mut events = select! {
            _ = &mut shutdown => {
                return Ok(());
            },
            events = events_since(&ctx, last_processed_index) => {events?}
        };
        // Consume stream of events since last processed.
        loop {
            let batch = select! {
                _ = &mut shutdown => {
                    return Ok(());
                }
                batch = events.try_next() => {batch}
            };
            match batch {
                Ok(Some(batch)) => {
                    if batch.num_rows() > 0 {
                        // Fetch the highest index from the batch
                        let highest_index = aggregate::max(as_uint64_array(
                            batch.column_by_name("index").ok_or_else(|| {
                                anyhow::anyhow!("index column should exist on events record batch")
                            })?,
                        )?);
                        if let Some(highest_index) = highest_index {
                            last_processed_index = Some(highest_index);
                        }

                        // Send batch to actor
                        select! {
                            _ = &mut shutdown => {
                                return Ok(());
                            },
                            Err(err) = handle.notify(NewEventsMsg { events: batch }) => {
                                warn!(?err,"failed to notify concluder about new events");
                            }
                        };
                    }
                }
                Ok(None) => {
                    break;
                }
                Err(err) => warn!(%err, "failed to poll_next_batch of conclusion_events"),
            };
        }
    }
}

async fn events_since(
    ctx: &SessionContext,
    offset: Option<u64>,
) -> Result<SendableRecordBatchStream> {
    // Fetch the conclusion events DataFrame
    let mut conclusion_events = ctx.table(CONCLUSION_EVENTS_TABLE).await?.select(vec![
        col("index"),
        col("stream_cid"),
        col("stream_type"),
        col("controller"),
        col("dimensions"),
        col("event_cid"),
        col("event_type"),
        col("data"),
        col("previous"),
    ])?;
    if let Some(offset) = offset {
        conclusion_events = conclusion_events.filter(col("index").gt(lit(offset)))?;
    }
    Ok(conclusion_events.execute_stream().await?)
}

/// Request the events since a highwater mark
#[derive(Debug)]
pub struct EventsSinceMsg {
    /// Produce message with an index greater than the highwater_mark.
    pub highwater_mark: u64,
}
impl Message for EventsSinceMsg {
    type Result = anyhow::Result<SendableRecordBatchStream>;
}

#[async_trait]
impl Handler<EventsSinceMsg> for Concluder {
    async fn handle(&mut self, message: EventsSinceMsg) -> <EventsSinceMsg as Message>::Result {
        events_since(&self.ctx, Some(message.highwater_mark)).await
    }
}

#[async_trait]
impl StreamTableSource for ConcluderHandle {
    fn schema(&self) -> SchemaRef {
        crate::schemas::conclusion_events()
    }
    async fn subscribe_since(
        &self,
        projection: Option<Vec<usize>>,
        offset: Option<u64>,
        limit: Option<usize>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        Ok(self
            .send(SubscribeSinceMsg {
                projection,
                offset,
                limit,
            })
            .await??)
    }
}
