use anyhow::Result;
use cid::Cid;
use multihash_codetable::{Code, MultihashDigest};
use serde::{Deserialize, Serialize};

use ceramic_core::DagCborIpfsBlock;

pub struct Receipt {
    pub proof_cid: Cid,
    pub path_prefix: Option<String>,
    pub blocks: Vec<DagCborIpfsBlock>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ProofBlock {
    pub chain_id: String,
    pub root: Cid,
    pub tx_hash: Cid,
    pub tx_type: String,
}

/// Interface for the transaction manager that accepts a root CID and returns a proof.
pub trait TransactionManager: Send + Sync {
    /// Accepts a root CID and returns a proof.
    async fn make_proof(&self, root: Cid) -> Result<Receipt>;
}

pub struct MockCas;
impl TransactionManager for MockCas {
    async fn make_proof(&self, root: Cid) -> Result<Receipt> {
        let mock_data = b"mock txHash";
        let mock_hash = MultihashDigest::digest(&Code::Sha2_256, mock_data);
        let mock_tx_hash = Cid::new_v1(0x00, mock_hash);
        let mock_proof_block = ProofBlock {
            chain_id: "mock chain id".to_string(),
            root,
            tx_hash: mock_tx_hash,
            tx_type: "mock tx type".to_string(),
        };
        let mock_proof: DagCborIpfsBlock = serde_ipld_dagcbor::to_vec(&mock_proof_block)?.into();
        let mock_path = "".to_owned();
        Ok(Receipt {
            proof_cid: mock_proof.cid,
            path_prefix: Some(mock_path),
            blocks: vec![mock_proof],
        })
    }
}
