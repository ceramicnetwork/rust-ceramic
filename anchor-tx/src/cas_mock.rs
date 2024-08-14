use anyhow::Result;
use async_trait::async_trait;
use multihash_codetable::{Code, MultihashDigest};

use ceramic_anchor_service::{
    AnchorRequest, DetachedTimeEvent, Receipt, Store, TimeEventBatch, TransactionManager,
};
use ceramic_core::{Cid, SerializeExt};
use ceramic_event::unvalidated::Proof;

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
        let proof = mock_proof.to_cid().unwrap();
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
impl Store for MockAnchorClient {
    async fn local_sourced_data_events(&self) -> Result<Vec<AnchorRequest>> {
        Ok((0..self.anchor_req_count)
            .map(|n| AnchorRequest {
                id: self.int64_cid(n),
                prev: self.int64_cid(n),
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
        let anchor_requests = anchor_client.local_sourced_data_events().await.unwrap();
        let anchor_service =
            AnchorService::new(Arc::new(MockCas), anchor_client, Duration::from_secs(1));
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await
            .unwrap();
        expect_file!["./test-data/test_anchor_batch.test.txt"].assert_debug_eq(&all_blocks);
    }
}
