use anyhow::Result;
use async_trait::async_trait;
use cid::Cid;
use multihash_codetable::{Code, MultihashDigest};

use ceramic_core::DagCborIpfsBlock;

use crate::transaction_manager::ProofBlock;
use crate::{Receipt, TransactionManager};

pub struct MockCas;
#[async_trait]
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
            proof_block: mock_proof.clone(),
            path_prefix: Some(mock_path),
            blocks: vec![mock_proof],
        })
    }
}
