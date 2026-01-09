use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use ceramic_core::Cid;
use ceramic_event::unvalidated::AnchorProof;

use crate::anchor::MerkleNodes;

/// Chain inclusion data from the blockchain transaction.
/// This contains the data needed to verify the anchor proof against the blockchain,
/// and is persisted to avoid redundant RPC calls during pipeline validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainInclusionData {
    /// The chain ID in CAIP-2 format (e.g., "eip155:100")
    pub chain_id: String,
    /// The transaction hash (e.g., "0x...")
    pub transaction_hash: String,
    /// The transaction input data (function selector + root hash, e.g., "0x97ad09eb...")
    pub transaction_input: String,
    /// The block hash containing the transaction
    pub block_hash: String,
    /// The block timestamp (Unix timestamp in seconds)
    pub timestamp: u64,
}

/// A struct containing a blockchain proof CID, the path prefix to the CID in the anchored Merkle tree and the
/// corresponding Merkle tree nodes.
pub struct RootTimeEvent {
    /// the proof data from the remote anchoring service
    pub proof: AnchorProof,
    /// the path through the remote Merkle tree
    pub detached_time_event: DetachedTimeEvent,
    /// the Merkle tree nodes from the remote anchoring service
    pub remote_merkle_nodes: MerkleNodes,
    /// Chain inclusion data for self-anchored events (None for remote CAS)
    pub chain_inclusion: Option<ChainInclusionData>,
}

impl std::fmt::Debug for RootTimeEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut merkle_tree_nodes: Vec<_> = self
            .remote_merkle_nodes
            .iter()
            .map(|(k, v)| format!("{:?}: {:?}", k, v))
            .collect();
        merkle_tree_nodes.sort();
        f.debug_struct("Receipt")
            .field("proof", &self.proof)
            .field("detached_time_event", &self.detached_time_event)
            .field("remote_merkle_nodes", &merkle_tree_nodes)
            .field("chain_inclusion", &self.chain_inclusion)
            .finish()
    }
}

/// A detached time event containing the path through the Merkle tree and the CID of the anchor proof block. This can be
/// used to build Time Events.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DetachedTimeEvent {
    /// The path through the Merkle Tree
    pub path: String,
    /// The CID of the anchor proof block
    pub proof: Cid,
}

impl std::fmt::Debug for DetachedTimeEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DetachedTimeEvent")
            .field("path", &self.path)
            .field("proof", &format!("{:?}", &self.proof))
            .finish()
    }
}

/// Interface for the transaction manager that anchors a root CID and returns a corresponding detached time event.
#[async_trait]
pub trait TransactionManager: Send + Sync {
    /// Anchors a root CID and returns a corresponding detached time event.
    async fn anchor_root(&self, root: Cid) -> Result<RootTimeEvent>;
}
