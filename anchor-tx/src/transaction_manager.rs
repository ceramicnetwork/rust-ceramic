use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use ceramic_core::Cid;
use ceramic_event::unvalidated::{MerkleNode, Proof};

/// A receipt containing a blockchain proof CID, the path prefix to the CID in the anchored Merkle tree and the
/// corresponding Merkle tree nodes.
pub struct Receipt {
    pub proof: Proof,
    pub detached_time_event: DetachedTimeEvent,
    pub remote_merkle_nodes: HashMap<Cid, MerkleNode>,
}

impl std::fmt::Debug for Receipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receipt")
            .field("proof", &self.proof)
            .field("detached_time_event", &self.detached_time_event)
            .field(
                "blocks",
                &self
                    .remote_merkle_nodes
                    .iter()
                    .map(|(k, v)| format!("{:?}: [{:?}, {:?}]", k, v[0], v[1]))
                    .collect::<Vec<_>>(),
            )
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
            .field("proof", &self.proof.to_string())
            .finish()
    }
}

/// Interface for the transaction manager that accepts a root CID and returns a proof.
#[async_trait]
pub trait TransactionManager {
    /// Accepts a root CID and returns a proof.
    async fn make_proof(&self, root: Cid) -> Result<Receipt>;
}
