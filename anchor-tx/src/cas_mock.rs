use anyhow::Result;
use async_trait::async_trait;
use multihash_codetable::{Code, MultihashDigest};

use ceramic_core::{Cid, DagCborIpfsBlock};
use ceramic_event::unvalidated::Proof;

use crate::{DetachedTimeEvent, Receipt, TransactionManager};

pub struct MockCas;
#[async_trait]
impl TransactionManager for MockCas {
    async fn make_proof(&self, root_cid: Cid) -> Result<Receipt> {
        let mock_data = b"mock txHash";
        let mock_hash = MultihashDigest::digest(&Code::Sha2_256, mock_data);
        let mock_tx_hash = Cid::new_v1(0x00, mock_hash);
        let mock_proof = Proof {
            chain_id: "mock chain id".to_string(),
            root: root_cid,
            tx_hash: mock_tx_hash,
            tx_type: "mock tx type".to_string(),
        };
        let mock_proof_block: DagCborIpfsBlock = serde_ipld_dagcbor::to_vec(&mock_proof)?.into();
        Ok(Receipt {
            proof: mock_proof,
            detached_time_event: DetachedTimeEvent {
                path: "".to_string(),
                proof: mock_proof_block.cid,
            },
            remote_merkle_nodes: Default::default(),
        })
    }
}
