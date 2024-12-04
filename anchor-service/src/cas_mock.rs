use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use multihash_codetable::{Code, MultihashDigest};

use ceramic_core::{Cid, EventId, NodeId, SerializeExt};
use ceramic_event::unvalidated::AnchorProof;

use crate::{
    AnchorRequest, DetachedTimeEvent, RootTimeEvent, Store, TimeEventInsertable, TransactionManager,
};

/// MockCas is a mock implementation of the Ceramic Anchor Service (CAS).
pub struct MockCas;

#[async_trait]
impl TransactionManager for MockCas {
    async fn anchor_root(&self, root_cid: Cid) -> Result<RootTimeEvent> {
        let mock_proof = AnchorProof::new(
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
pub struct MockAnchorEventService {
    /// Number of anchor requests to generate
    pub anchor_req_count: u64,
    /// the events that have been sent to the mock
    pub events: Mutex<Vec<TimeEventInsertable>>,
}

impl MockAnchorEventService {
    // #[allow(dead_code)]
    /// Create a new MockAnchorClient with the given number of anchor requests.
    pub fn new(anchor_req_count: u64) -> Self {
        Self {
            anchor_req_count,
            events: Default::default(),
        }
    }

    fn int64_cid(&self, i: u64) -> Cid {
        let data = i.to_be_bytes();
        let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
        Cid::new_v1(0x00, hash)
    }
}

#[async_trait]
impl Store for MockAnchorEventService {
    async fn insert_many(&self, items: Vec<TimeEventInsertable>, _informant: NodeId) -> Result<()> {
        self.events.lock().unwrap().extend(items);
        Ok(())
    }
    /// Get a batch of AnchorRequests.
    async fn events_since_high_water_mark(
        &self,
        _informant: NodeId,
        highwater: i64,
        limit: i64,
    ) -> Result<Vec<AnchorRequest>> {
        // The high water mark is the last row in the table.
        // The call to events_since_high_water_mark generate the next event (n+1)
        // to the anchor_req_count which is the last event in the mock data.
        let ints = ((highwater as u64 + 1)..=self.anchor_req_count).take(limit as usize);
        Ok(ints.map(|n| AnchorRequest {
                id: self.int64_cid(n),
                prev: self.int64_cid(n),
                event_id: EventId::try_from(
                    hex::decode("ce010500ba25076d730241e745cc7c072ff729ea683b751701711220f4ef7ec208944d257025408bb647949e6b72930520bc80f34d8bfbafd2643d86").unwrap()).unwrap(),
                resume_token: n as i64,
            })
            .collect())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use ceramic_core::NodeKey;
    use ceramic_sql::sqlite::SqlitePool;
    use expect_test::expect_file;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::AnchorService;

    #[tokio::test]
    async fn test_anchor_batch_with_10_requests() {
        let anchor_client = Arc::new(MockAnchorEventService::new(10));
        let anchor_requests = anchor_client
            .events_since_high_water_mark(NodeKey::random().id(), 0, 1_000_000)
            .await
            .unwrap();
        let anchor_service = AnchorService::new(
            Arc::new(MockCas),
            anchor_client,
            SqlitePool::connect_in_memory().await.unwrap(),
            NodeKey::random().id(),
            Duration::from_secs(1),
            10,
        );
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await
            .unwrap();
        expect_file!["./test-data/test_anchor_batch_with_10_requests.test.txt"]
            .assert_debug_eq(&all_blocks);
    }

    #[tokio::test]
    async fn test_anchor_batch_with_pow2_requests() {
        let anchor_client = Arc::new(MockAnchorEventService::new(16));
        let anchor_requests = anchor_client
            .events_since_high_water_mark(NodeKey::random().id(), 0, 1_000_000)
            .await
            .unwrap();
        let anchor_service = AnchorService::new(
            Arc::new(MockCas),
            anchor_client,
            SqlitePool::connect_in_memory().await.unwrap(),
            NodeKey::random().id(),
            Duration::from_secs(1),
            16,
        );
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await
            .unwrap();
        expect_file!["./test-data/test_anchor_batch_with_pow2_requests.test.txt"]
            .assert_debug_eq(&all_blocks);
    }

    #[tokio::test]
    async fn test_anchor_batch_with_more_than_pow2_requests() {
        let anchor_client = Arc::new(MockAnchorEventService::new(18));
        let anchor_requests = anchor_client
            .events_since_high_water_mark(NodeKey::random().id(), 0, 1_000_000)
            .await
            .unwrap();
        let anchor_service = AnchorService::new(
            Arc::new(MockCas),
            anchor_client,
            SqlitePool::connect_in_memory().await.unwrap(),
            NodeKey::random().id(),
            Duration::from_secs(1),
            18,
        );
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await
            .unwrap();
        expect_file!["./test-data/test_anchor_batch_with_more_than_pow2_requests.test.txt"]
            .assert_debug_eq(&all_blocks);
    }

    #[tokio::test]
    async fn test_anchor_batch_with_less_than_pow2_requests() {
        let anchor_client = Arc::new(MockAnchorEventService::new(15));
        let anchor_requests = anchor_client
            .events_since_high_water_mark(NodeKey::random().id(), 0, 1_000_000)
            .await
            .unwrap();
        let anchor_service = AnchorService::new(
            Arc::new(MockCas),
            anchor_client,
            SqlitePool::connect_in_memory().await.unwrap(),
            NodeKey::random().id(),
            Duration::from_secs(1),
            15,
        );
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await
            .unwrap();
        expect_file!["./test-data/test_anchor_batch_with_less_than_pow2_requests.test.txt"]
            .assert_debug_eq(&all_blocks);
    }

    #[tokio::test]
    async fn test_anchor_batch_with_0_requests() {
        let anchor_client = Arc::new(MockAnchorEventService::new(0));
        let anchor_requests = anchor_client
            .events_since_high_water_mark(NodeKey::random().id(), 0, 1_000_000)
            .await
            .unwrap();
        let anchor_service = AnchorService::new(
            Arc::new(MockCas),
            anchor_client,
            SqlitePool::connect_in_memory().await.unwrap(),
            NodeKey::random().id(),
            Duration::from_secs(1),
            1,
        );
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await;
        expect_file!["./test-data/test_anchor_batch_with_0_requests.test.txt"]
            .assert_debug_eq(&all_blocks);
    }

    #[tokio::test]
    async fn test_anchor_batch_with_1_request() {
        let anchor_client = Arc::new(MockAnchorEventService::new(1));
        let anchor_requests = anchor_client
            .events_since_high_water_mark(NodeKey::random().id(), 0, 1_000_000)
            .await
            .unwrap();
        let anchor_service = AnchorService::new(
            Arc::new(MockCas),
            anchor_client,
            SqlitePool::connect_in_memory().await.unwrap(),
            NodeKey::random().id(),
            Duration::from_secs(1),
            1,
        );
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await
            .unwrap();
        expect_file!["./test-data/test_anchor_batch_with_1_request.test.txt"]
            .assert_debug_eq(&all_blocks);
    }
}
