use anyhow::Result;
use async_trait::async_trait;
use ceramic_core::{Cid, NodeId};
use ceramic_sql::sqlite::SqlitePool;
use chrono::{Duration as ChronoDuration, TimeDelta, Utc};
use chrono::{DurationRound, Timelike};
use futures::future::{select, Either, FutureExt};
use futures::pin_mut;
use indexmap::IndexMap;
use std::future::Future;
use std::pin;
use std::{sync::Arc, time::Duration};
use tokio::time::{interval, interval_at, Instant};
use tracing::{error, info};

use crate::high_water_mark_store::HighWaterMarkStore;
use crate::{
    anchor::{AnchorRequest, TimeEventBatch, TimeEventInsertable},
    merkle_tree::{build_merkle_tree, MerkleTree},
    time_event::build_time_events,
    transaction_manager::{RootTimeEvent, TransactionManager},
};

/// ceramic_anchor_service::Store is responsible for fetching AnchorRequests and storing TimeEvents.
#[async_trait]
pub trait Store: Send + Sync {
    /// Store a batch of TimeEvents.
    async fn insert_many(&self, items: Vec<TimeEventInsertable>, informant: NodeId) -> Result<()>;
    /// Get a batch of AnchorRequests.
    async fn events_since_high_water_mark(
        &self,
        informant: NodeId,
        high_water_mark: i64,
        limit: i64,
    ) -> Result<Vec<AnchorRequest>>;
}

/// An AnchorService is responsible for anchoring batches of AnchorRequests and storing TimeEvents generated based on
/// the requests and the anchor proof.
pub struct AnchorService {
    tx_manager: Arc<dyn TransactionManager>,
    event_service: Arc<dyn Store>,
    high_water_mark_store: HighWaterMarkStore,
    node_id: NodeId,
    anchor_interval: Duration,
    anchor_batch_size: u64,
}

impl AnchorService {
    /// Create a new AnchorService.
    pub fn new(
        tx_manager: Arc<dyn TransactionManager>,
        event_service: Arc<dyn Store>,
        pool: SqlitePool,
        node_id: NodeId,
        anchor_interval: Duration,
        anchor_batch_size: u64,
    ) -> Self {
        Self {
            tx_manager,
            event_service,
            high_water_mark_store: HighWaterMarkStore::new(pool),
            node_id,
            anchor_interval,
            anchor_batch_size,
        }
    }

    /// Run the AnchorService:
    /// - Get anchor requests from the AnchorClient
    /// - Deduplicate the anchor requests
    /// - Anchor the batch using a Transaction Manager
    /// - Store the TimeEvents using the AnchorClient
    ///
    /// This function will run indefinitely, or until the process is shutdown.
    pub async fn run(&mut self, shutdown_signal: impl Future<Output = ()>) {
        let shutdown_signal = shutdown_signal.fuse();
        pin_mut!(shutdown_signal);

        info!("anchor service started");

        loop {
            let now = Utc::now();
            let next_tick = now
                .duration_trunc(TimeDelta::from_std(self.anchor_interval).unwrap())
                .unwrap()
                + TimeDelta::from_std(self.anchor_interval).unwrap()
                - ChronoDuration::minutes(3);

            let delay = next_tick - now;
            // durations in rust are always positive.
            // If the delay is negative, it means the next tick is in the past, therefore
            // we should process the next batch immediately.
            if let Ok(delay) = delay.to_std() {
                tokio::time::sleep(delay).await;
            }
            let process_next_batch = self.process_next_batch();
            pin_mut!(process_next_batch);
            match select(process_next_batch, &mut shutdown_signal).await {
                Either::Left((result, _)) => {
                    if let Err(e) = result {
                        error!("Error processing batch: {:?}", e);
                    }
                }
                Either::Right((_, _)) => {
                    break;
                }
            }
        }
        info!("anchor service stopped");
    }

    async fn process_next_batch(&mut self) -> Result<()> {
        // Pass the anchor requests through a deduplication step to avoid anchoring multiple Data Events from the same
        // Stream.
        let high_water_mark = self
            .high_water_mark_store
            .high_water_mark()
            .await
            .expect("error getting high water mark from database");
        // Get the next batch of anchor requests
        let anchor_requests: Vec<AnchorRequest> = match self
            .event_service
            .events_since_high_water_mark(
                self.node_id,
                high_water_mark,
                self.anchor_batch_size as i64,
            )
            .await
        {
            Ok(requests) => IndexMap::<Cid, AnchorRequest>::from_iter(
                requests.into_iter().map(|request| (request.id, request)),
            )
            .into_values()
            .collect(),
            Err(e) => {
                return Err(e);
            }
        };
        if anchor_requests.is_empty() {
            info!("no requests to anchor");
            return Ok(());
        }
        // Anchor the batch to the CAS. This may block for a long time.
        match self.anchor_batch(anchor_requests.as_slice()).await {
            Ok(time_event_batch) => {
                if let Err(e) = self.store_time_events(time_event_batch).await {
                    error!("error writing time events: {:?}", e);
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Anchor a batch of requests using a Transaction Manager:
    /// - Build a MerkleTree from the anchor requests
    /// - Anchor the root of the tree and obtain a proof from the Transaction Manager
    /// - Build TimeEvents from the anchor requests and the proof
    ///
    /// This function will block until the proof is obtained from the Transaction Manager.
    pub async fn anchor_batch(&self, anchor_requests: &[AnchorRequest]) -> Result<TimeEventBatch> {
        let MerkleTree {
            root_cid,
            nodes: local_merkle_nodes,
            count,
        } = build_merkle_tree(anchor_requests)?;
        let RootTimeEvent {
            proof,
            detached_time_event,
            mut remote_merkle_nodes,
        } = self.tx_manager.anchor_root(root_cid).await?;
        let time_events = build_time_events(anchor_requests, &detached_time_event, count)?;
        remote_merkle_nodes.extend(local_merkle_nodes);
        Ok(TimeEventBatch {
            merkle_nodes: remote_merkle_nodes,
            proof,
            raw_time_events: time_events,
        })
    }

    async fn store_time_events(&self, time_event_batch: TimeEventBatch) -> Result<()> {
        let new_high_water_mark = time_event_batch
            .raw_time_events
            .events
            .last()
            .expect("should have at least one event in the batch")
            .0
            .resume_token;

        match time_event_batch.try_to_insertables() {
            Ok(insertables) => {
                // Update the high water mark
                self.event_service
                    .insert_many(insertables, self.node_id)
                    .await?;

                Ok(self
                    .high_water_mark_store
                    .insert_high_water_mark(new_high_water_mark)
                    .await?)
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use ceramic_core::NodeId;
    use ceramic_sql::sqlite::SqlitePool;
    use expect_test::expect_file;
    use tokio::{sync::broadcast, time::sleep};

    use super::AnchorService;
    use crate::{MockAnchorEventService, MockCas};

    #[tokio::test]
    async fn test_anchor_service_run() {
        let tx_manager = Arc::new(MockCas);
        let event_service = Arc::new(MockAnchorEventService::new(10));
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let node_id = NodeId::random().0;
        let anchor_interval = Duration::from_millis(1);
        let anchor_batch_size = 1000000;
        let mut anchor_service = AnchorService::new(
            tx_manager,
            event_service.clone(),
            pool,
            node_id,
            anchor_interval,
            anchor_batch_size,
        );
        let (shutdown_signal_tx, mut shutdown_signal) = broadcast::channel::<()>(1);
        // let mut shutdown_signal = shutdown_signal_rx.resubscribe();
        Some(tokio::spawn(async move {
            anchor_service
                .run(async move {
                    let _ = shutdown_signal.recv().await;
                })
                .await
        }));
        while event_service.events.lock().unwrap().is_empty() {
            sleep(Duration::from_millis(1)).await;
        }
        expect_file!["./test-data/test_anchor_service_run.txt"]
            .assert_debug_eq(&event_service.events.lock().unwrap());
        shutdown_signal_tx.send(()).unwrap();
    }
}
