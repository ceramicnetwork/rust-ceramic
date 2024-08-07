use anyhow::Result;
use async_trait::async_trait;

use ceramic_core::{Cid, DagCborIpfsBlock};
use ceramic_event::unvalidated::Proof;

use crate::{DetachedTimeEvent, Receipt, TransactionManager};

pub struct MockCas;
#[async_trait]
impl TransactionManager for MockCas {
    async fn make_proof(&self, root_cid: Cid) -> Result<Receipt> {
        let mock_proof = Proof {
            chain_id: "mock chain id".to_string(),
            // mock cas always put the prev cid as the root of its tree.
            root: root_cid,
            // for mock cas root in tx_hash is root == tx_hash
            tx_hash: root_cid,
            tx_type: "mock tx type".to_string(),
        };
        let mock_proof_block: DagCborIpfsBlock = serde_ipld_dagcbor::to_vec(&mock_proof)?.into();
        Ok(Receipt {
            proof: mock_proof,
            detached_time_event: DetachedTimeEvent {
                // mock cas always put the prev cid as the root of its tree.
                path: "".to_string(),
                proof: mock_proof_block.cid,
            },
            remote_merkle_nodes: Default::default(),
        })
    }
}
