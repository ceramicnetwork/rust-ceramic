use anyhow::Result;
use async_trait::async_trait;
use multihash_codetable::{Code, MultihashDigest};

use ceramic_anchor_service::{AnchorClient, DetachedTimeEvent, Receipt, TransactionManager};
use ceramic_core::{Cid, EventId, SerdeIpld};
use ceramic_event::{
    anchor::{AnchorRequest, TimeEventBatch},
    unvalidated::Proof,
};

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
        let proof: Cid = mock_proof.to_cid()?;
        Ok(Receipt {
            proof: mock_proof,
            detached_time_event: DetachedTimeEvent {
                // mock cas always put the prev cid as the root of its tree.
                path: "".to_string(),
                proof,
            },
            remote_merkle_nodes: Default::default(),
        })
    }
}

#[derive(Debug)]
pub struct MockAnchorClient {
    pub anchor_req_count: u64,
}

impl MockAnchorClient {
    #[allow(dead_code)]
    pub(crate) fn new(anchor_req_count: u64) -> Self {
        Self { anchor_req_count }
    }

    fn int64_cid(&self, i: u64) -> Cid {
        let data = i.to_be_bytes();
        let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
        Cid::new_v1(0x00, hash)
    }
}

#[async_trait]
impl AnchorClient for MockAnchorClient {
    async fn get_anchor_requests(&self) -> Result<Vec<AnchorRequest>> {
        Ok((0..self.anchor_req_count)
            .map(|n| AnchorRequest {
                init: self.int64_cid(n),
                prev: self.int64_cid(n),
                order_key: EventId::try_from(
                    hex::decode("ce010500ba25076d730241e745cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86").unwrap()).unwrap(),
                row_id: n as i64,
            })
            .collect())
    }

    async fn put_time_events(&self, _batch: TimeEventBatch) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use expect_test::expect_file;
    use std::sync::Arc;
    use std::time::Duration;

    use ceramic_anchor_service::AnchorService;

    #[tokio::test]
    async fn test_anchor_batch() {
        let anchor_client = Arc::new(MockAnchorClient::new(10));
        let anchor_requests = anchor_client.get_anchor_requests().await.unwrap();
        let anchor_service =
            AnchorService::new(anchor_client, Arc::new(MockCas), Duration::from_secs(1));
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await
            .unwrap();
        expect_file!["./test-data/test_anchor_batch.test.txt"].assert_debug_eq(&all_blocks);
    }
}
