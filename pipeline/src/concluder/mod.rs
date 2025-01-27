//! The concluder actor is reponsible for populating the `conclusion_events` table.
//! In its current form its is just a wrapper over the sqlite tables in order to expose them as
//! datafusion tables.
mod event;
mod metrics;
#[cfg(any(test, feature = "mock"))]
pub mod mock;
mod table;

use std::{sync::Arc, time::Duration};

use anyhow::Context as _;
use arrow::{array::RecordBatch, compute::kernels::aggregate};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use ceramic_actor::{actor_envelope, Actor, Handler, Message};
use datafusion::{
    common::{cast::as_uint64_array, exec_datafusion_err},
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
    prelude::{col, lit, SessionContext},
};
use futures::TryStreamExt as _;
use shutdown::{Shutdown, ShutdownSignal};
use table::FeedTable;
use tokio::{
    select,
    sync::broadcast,
    task::JoinHandle,
    time::{interval, MissedTickBehavior},
};
use tracing::{debug, error, info, warn};

use crate::{
    metrics::Metrics,
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
        metrics: Metrics,
        shutdown: Shutdown,
    ) -> Result<(ConcluderHandle, Vec<JoinHandle<()>>)> {
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(size);
        Self::spawn_with(
            size,
            ctx,
            Concluder {
                ctx: ctx.session(),
                broadcast_tx,
            },
            feed,
            metrics,
            shutdown,
        )
        .await
    }
    /// Spawn the concluder with the given actor.
    pub async fn spawn_with<F: ConclusionFeed + 'static>(
        size: usize,
        ctx: &PipelineContext,
        concluder: impl ConcluderActor,
        feed: ConclusionFeedSource<F>,
        metrics: Metrics,
        shutdown: Shutdown,
    ) -> Result<(ConcluderHandle, Vec<JoinHandle<()>>)> {
        let (handle, task_handle) =
            Self::spawn(size, concluder, metrics.clone(), shutdown.wait_fut());
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
                batches.first().and_then(|batch| {
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
                metrics,
                shutdown.wait_fut(),
            )
            .await
            {
                error!(%err, "poll_new_events loop failed")
            } else {
                info!("poll_new_events task finished");
            }
        });

        Ok((handle, vec![task_handle, sub_handle]))
    }
}

actor_envelope! {
    ConcluderEnvelope,
    ConcluderActor,
    ConcluderRecorder,
    NewEvents => NewEventsMsg,
    SubscribeSince => SubscribeSinceMsg,
    EventsSince => EventsSinceMsg,
}

/// Notify actor of new events
#[derive(Debug)]
pub struct NewEventsMsg {
    /// Events as a record batch, must have the schema of the [`crate::schemas::conclusion_events`] table.
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
            message.offset,
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
    metrics: Metrics,
    mut shutdown: ShutdownSignal,
) -> anyhow::Result<()> {
    let mut interval = interval(Duration::from_millis(1_000));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    // Poll for new events until shutdown
    loop {
        metrics.concluder_poll_new_events_loop_count.inc();
        select! {
            _ = &mut shutdown => {
                return Ok(());
            }
            _ = interval.tick() => {}
        };
        debug!(last_processed_index, "events since");
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
                        let highest_index = aggregate::max(
                            as_uint64_array(batch.column_by_name("index").ok_or_else(|| {
                                anyhow::anyhow!("index column should exist on events record batch")
                            })?)
                            .context("index column should be a uint64")?,
                        );
                        if let Some(highest_index) = highest_index {
                            last_processed_index = Some(highest_index);
                        }

                        // Send batch to actor
                        select! {
                            _ = &mut shutdown => {
                                return Ok(());
                            },
                            r = handle.notify(NewEventsMsg { events: batch }) => {
                                if let Err(err) = r {
                                    warn!(?err, "failed to notify concluder about new events");
                                }
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
#[cfg(test)]
mod tests {
    use super::{Concluder, ConcluderHandle, ConclusionEvent, SubscribeSinceMsg};

    use std::{str::FromStr as _, sync::Arc};

    use cid::Cid;
    use futures::TryStreamExt as _;
    use mockall::predicate;
    use object_store::memory::InMemory;
    use prometheus_client::registry::Registry;
    use shutdown::Shutdown;
    use test_log::test;

    use crate::{
        pipeline_ctx,
        tests::{MockConclusionFeed, TestContext},
        ConclusionData, ConclusionFeedSource, ConclusionInit, ConclusionTime, Metrics,
    };

    async fn init(feed: MockConclusionFeed) -> anyhow::Result<TestContext<ConcluderHandle>> {
        let metrics = Metrics::register(&mut Registry::default());
        let shutdown = Shutdown::new();
        let pipeline_ctx = pipeline_ctx(Arc::new(InMemory::new())).await?;
        let (concluder, handles) = Concluder::spawn_new(
            1_000,
            &pipeline_ctx,
            ConclusionFeedSource::Direct(Arc::new(feed)),
            metrics,
            shutdown.clone(),
        )
        .await?;
        Ok(TestContext::new(shutdown, handles, concluder))
    }

    #[test(tokio::test)]
    async fn poll_new_events() {
        // Test that the spawn method setups of polling loop of the conclusion feed that delivers
        // new events to the concluder.
        let mut mock_feed = MockConclusionFeed::new();
        mock_feed
            .expect_max_highwater_mark()
            .once()
            .return_once(|| Ok(None));
        // Return one event at a time so we test that multiple batches can be processed
        mock_feed
            .expect_conclusion_events_since()
            .with(predicate::eq(0), predicate::always())
            // We get two calls from the beginning: One for the poll_new_events loop and
            // one for the subscription call below
            .times(2)
            .returning(|_h, _l| {
                Ok(vec![ConclusionEvent::Data(ConclusionData {
                    index: 1,
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
                })])
            });
        mock_feed
            .expect_conclusion_events_since()
            .with(predicate::eq(1), predicate::always())
            .return_once(|_h, _l| {
                Ok(vec![ConclusionEvent::Time(ConclusionTime {
                    index: 2,
                    event_cid: Cid::from_str(
                        "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
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
                    previous: vec![Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )
                    .unwrap()],
                })])
            });
        mock_feed
            .expect_conclusion_events_since()
            .with(predicate::eq(2), predicate::always())
            .return_once(|_h, _l| {
                Ok(vec! [
                    ConclusionEvent::Data(ConclusionData {
                        index: 3,
                        event_cid: Cid::from_str("baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du")
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
                        previous: vec![
                            Cid::from_str("baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq")
                                .unwrap(),
                            Cid::from_str("baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi")
                                .unwrap(),
                        ],
                        data:
                            r#"{"metadata":{"foo":true},"content":[{"op":"replace", "path": "/a", "value":1}]}"#
                                .bytes()
                                .collect(),
                    }),
                ])

            });
        let ctx = init(mock_feed).await.unwrap();
        let mut subscription = ctx
            .actor_handle
            .send(SubscribeSinceMsg {
                projection: None,
                offset: None,
                limit: Some(3),
            })
            .await
            .unwrap()
            .unwrap();
        // Read subscription so we know when the events have been processed
        while let Some(_) = subscription.try_next().await.unwrap() {}
        // Shutdown ensures the mock expectations have been met
        ctx.shutdown().await.unwrap();
    }
}
