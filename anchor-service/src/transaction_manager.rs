use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use ceramic_core::Cid;
use ceramic_event::{anchor::MerkleNodes, unvalidated::Proof};

/// A receipt containing a blockchain proof CID, the path prefix to the CID in the anchored Merkle tree and the
/// corresponding Merkle tree nodes.
pub struct Receipt {
    pub proof: Proof,
    pub detached_time_event: DetachedTimeEvent,
    pub remote_merkle_nodes: MerkleNodes,
}

impl std::fmt::Debug for Receipt {
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
            .finish()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DetachedTimeEvent {
    pub path: String,
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

/// Interface for the transaction manager that accepts a root CID and returns a proof.
#[async_trait]
pub trait TransactionManager: Send + Sync {
    /// Accepts a root CID and returns a proof.
    async fn make_proof(&self, root: Cid) -> Result<Receipt>;
}
