use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

use crate::{
    anchor::{AnchorRequest, TimeEventBatch},
    merkle_tree::{build_merkle_tree, MerkleTree},
    time_event::build_time_events,
    transaction_manager::{RootTimeEvent, TransactionManager},
};

/// ceramic_anchor_service::Store is responsible for fetching AnchorRequests and storing TimeEvents.
#[async_trait]
pub trait Store: Send + Sync {
    /// Get a batch of AnchorRequests.
    async fn local_sourced_data_events(&self) -> Result<Vec<AnchorRequest>>;
    /// Store a batch of TimeEvents.
    async fn put_time_events(&self, batch: TimeEventBatch) -> Result<()>;
}

/// An AnchorService is responsible for anchoring batches of AnchorRequests and storing TimeEvents generated based on
/// the requests and the anchor proof.
pub struct AnchorService {
    tx_manager: Arc<dyn TransactionManager>,
    _event_service: Arc<dyn Store>,
    _batch_linger_time: Duration,
}

impl AnchorService {
    /// Create a new AnchorService.
    pub fn new(
        tx_manager: Arc<dyn TransactionManager>,
        _event_service: Arc<dyn Store>,
        _batch_linger_time: Duration,
    ) -> Self {
        Self {
            tx_manager,
            _event_service,
            _batch_linger_time,
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
            nodes,
            count,
        } = build_merkle_tree(anchor_requests)?;
        let RootTimeEvent {
            proof,
            detached_time_event,
            mut remote_merkle_nodes,
        } = self.tx_manager.anchor_root(root_cid).await?;
        let time_events = build_time_events(anchor_requests, &detached_time_event, count)?;
        remote_merkle_nodes.extend(nodes);
        Ok(TimeEventBatch {
            merkle_nodes: remote_merkle_nodes,
            proof,
            time_events,
        })
    }
}
