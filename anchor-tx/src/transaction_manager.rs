use anyhow::Result;
use async_trait::async_trait;
use cid::Cid;
use serde::{Deserialize, Serialize};

use ceramic_core::DagCborIpfsBlock;

/// A receipt containing a blockchain proof CID, the path prefix to the CID in the anchored Merkle tree and the
/// corresponding Merkle tree nodes.
pub struct Receipt {
    pub proof_block: DagCborIpfsBlock,
    pub path_prefix: Option<String>,
    pub blocks: Vec<DagCborIpfsBlock>,
}

impl std::fmt::Debug for Receipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receipt")
            .field("proof_block", &self.proof_block)
            .field("path_prefix", &self.path_prefix.clone().unwrap_or_default())
            .field("blocks", &self.blocks)
            .finish()
    }
}

/// A block containing a blockchain proof.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ProofBlock {
    pub chain_id: String,
    pub root: Cid,
    pub tx_hash: Cid,
    pub tx_type: String,
}

/// Interface for the transaction manager that accepts a root CID and returns a proof.
#[async_trait]
pub trait TransactionManager {
    /// Accepts a root CID and returns a proof.
    async fn make_proof(&self, root: Cid) -> Result<Receipt>;
}
