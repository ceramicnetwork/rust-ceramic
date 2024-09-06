use anyhow::Result;
use async_trait::async_trait;
use multihash_codetable::{Code, MultihashDigest};

use ceramic_core::{Cid, SerializeExt};
use ceramic_event::unvalidated::Proof;

use crate::{
    AnchorRequest, DetachedTimeEvent, RootTimeEvent, Store, TimeEventBatch, TransactionManager,
};

/// MockCas is a mock implementation of the Ceramic Anchor Service (CAS).
pub struct MockCas;

#[async_trait]
impl TransactionManager for MockCas {
    async fn anchor_root(&self, root_cid: Cid) -> Result<RootTimeEvent> {
        let mock_proof = Proof::new(
            "mock chain id".to_string(),
            root_cid,
            root_cid,
            "mock tx type".to_string(),
        );
        let proof = mock_proof.to_cid().unwrap();
        Ok(RootTimeEvent {
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

/// MockAnchorClient is a mock implementation of a Store.
#[derive(Debug)]
pub struct MockAnchorClient {
    /// Number of anchor requests to generate
    pub anchor_req_count: u64,
}

impl MockAnchorClient {
    // #[allow(dead_code)]
    /// Create a new MockAnchorClient with the given number of anchor requests.
    pub fn new(anchor_req_count: u64) -> Self {
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

    use crate::AnchorService;

    #[tokio::test]
    async fn test_anchor_batch_with_10_requests() {
        let anchor_client = Arc::new(MockAnchorClient::new(10));
        let anchor_requests = anchor_client.local_sourced_data_events().await.unwrap();
        let anchor_service =
            AnchorService::new(Arc::new(MockCas), anchor_client, Duration::from_secs(1));
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await
            .unwrap();
        expect_file!["./test-data/test_anchor_batch_with_10_requests.test.txt"]
            .assert_debug_eq(&all_blocks);
    }

    #[tokio::test]
    async fn test_anchor_batch_with_pow2_requests() {
        let anchor_client = Arc::new(MockAnchorClient::new(16));
        let anchor_requests = anchor_client.local_sourced_data_events().await.unwrap();
        let anchor_service =
            AnchorService::new(Arc::new(MockCas), anchor_client, Duration::from_secs(1));
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await
            .unwrap();
        expect_file!["./test-data/test_anchor_batch_with_pow2_requests.test.txt"]
            .assert_debug_eq(&all_blocks);
    }

    #[tokio::test]
    async fn test_anchor_batch_with_more_than_pow2_requests() {
        let anchor_client = Arc::new(MockAnchorClient::new(18));
        let anchor_requests = anchor_client.local_sourced_data_events().await.unwrap();
        let anchor_service =
            AnchorService::new(Arc::new(MockCas), anchor_client, Duration::from_secs(1));
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await
            .unwrap();
        expect_file!["./test-data/test_anchor_batch_with_more_than_pow2_requests.test.txt"]
            .assert_debug_eq(&all_blocks);
    }

    #[tokio::test]
    async fn test_anchor_batch_with_less_than_pow2_requests() {
        let anchor_client = Arc::new(MockAnchorClient::new(15));
        let anchor_requests = anchor_client.local_sourced_data_events().await.unwrap();
        let anchor_service =
            AnchorService::new(Arc::new(MockCas), anchor_client, Duration::from_secs(1));
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await
            .unwrap();
        expect_file!["./test-data/test_anchor_batch_with_less_than_pow2_requests.test.txt"]
            .assert_debug_eq(&all_blocks);
    }

    #[tokio::test]
    async fn test_anchor_batch_with_0_requests() {
        let anchor_client = Arc::new(MockAnchorClient::new(0));
        let anchor_requests = anchor_client.local_sourced_data_events().await.unwrap();
        let anchor_service =
            AnchorService::new(Arc::new(MockCas), anchor_client, Duration::from_secs(1));
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await;
        expect_file!["./test-data/test_anchor_batch_with_0_requests.test.txt"]
            .assert_debug_eq(&all_blocks);
    }

    #[tokio::test]
    async fn test_anchor_batch_with_1_request() {
        let anchor_client = Arc::new(MockAnchorClient::new(1));
        let anchor_requests = anchor_client.local_sourced_data_events().await.unwrap();
        let anchor_service =
            AnchorService::new(Arc::new(MockCas), anchor_client, Duration::from_secs(1));
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await
            .unwrap();
        expect_file!["./test-data/test_anchor_batch_with_1_request.test.txt"]
            .assert_debug_eq(&all_blocks);
    }
}
