use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use cid::Cid;
use indexmap::IndexMap;
use tokio::time::interval;
use tracing::{error, info};

use ceramic_event::anchor::{AnchorRequest, TimeEventBatch};

use crate::{
    merkle_tree::{build_merkle_tree, MerkleTree},
    time_event::build_time_events,
    transaction_manager::{Receipt, TransactionManager},
};

#[async_trait]
pub trait AnchorClient: Send + Sync {
    async fn get_anchor_requests(&self) -> Result<Vec<AnchorRequest>>;
    async fn put_time_events(&self, batch: TimeEventBatch) -> Result<()>;
}

pub struct AnchorService {
    tx_manager: Arc<dyn TransactionManager>,
    anchor_client: Arc<dyn AnchorClient>,
    batch_linger_time: Duration,
}

impl AnchorService {
    pub fn new(
        anchor_client: Arc<dyn AnchorClient>,
        tx_manager: Arc<dyn TransactionManager>,
        batch_linger_time: Duration,
    ) -> Self {
        Self {
            anchor_client,
            tx_manager,
            batch_linger_time,
        }
    }

    pub async fn run(&mut self) {
        let mut interval = interval(self.batch_linger_time);
        loop {
            interval.tick().await;

            // Pass the anchor requests through a deduplication step to avoid anchoring multiple Data Events from the
            // same Stream.
            let anchor_requests: Vec<AnchorRequest> =
                match self.anchor_client.get_anchor_requests().await {
                    Ok(requests) => IndexMap::<Cid, AnchorRequest>::from_iter(
                        requests.into_iter().map(|request| (request.id, request)),
                    )
                    .into_values()
                    .collect(),
                    Err(e) => {
                        error!("error getting anchor requests: {:?}", e);
                        continue;
                    }
                };
            if anchor_requests.is_empty() {
                info!("no requests to anchor");
                continue;
            }
            // Anchor the batch to the CAS. This may block for a long time.
            match self.anchor_batch(anchor_requests.as_slice()).await {
                Ok(time_event_batch) => {
                    if let Err(e) = self.anchor_client.put_time_events(time_event_batch).await {
                        error!("error storing time events: {:?}", e);
                    }
                }
                Err(e) => {
                    error!("error anchoring batch: {:?}", e);
                }
            }
        }
    }

    pub async fn anchor_batch(&self, anchor_requests: &[AnchorRequest]) -> Result<TimeEventBatch> {
        let MerkleTree {
            root_cid,
            nodes,
            count,
        } = build_merkle_tree(anchor_requests)?;
        let Receipt {
            proof,
            detached_time_event,
            mut remote_merkle_nodes,
        } = self.tx_manager.make_proof(root_cid).await?;
        let time_events = build_time_events(anchor_requests, &detached_time_event, count)?;
        remote_merkle_nodes.extend(nodes);
        Ok(TimeEventBatch {
            merkle_nodes: remote_merkle_nodes,
            proof,
            time_events,
        })
    }
}
