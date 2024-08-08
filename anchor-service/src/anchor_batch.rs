use std::{sync::Arc, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use cid::Cid;
use indexmap::IndexMap;
use tokio::time::interval;
use tracing::error;

use ceramic_anchor_tx::{Receipt, TransactionManager};

use crate::{
    merkle_tree::{build_merkle_tree, MerkleTree},
    time_event::{build_time_events, TimeEventBatch},
};

#[async_trait]
pub trait AnchorClient: Send + Sync {
    async fn get_anchor_requests(&self) -> Vec<AnchorRequest>;
    async fn put_time_events(&self, batch: TimeEventBatch) -> Result<()>;
}

// AnchorRequest request for a Time Event
pub struct AnchorRequest {
    pub id: Cid,   // The CID of the Stream
    pub prev: Cid, // The CID of the Event to be anchored
}

pub struct AnchorService {
    tx_manager: Arc<dyn TransactionManager>,
    anchor_client: Arc<dyn AnchorClient>,
    batch_linger_time: Duration,
}

impl AnchorService {
    pub fn new(
        anchor_client: impl AnchorClient + 'static,
        tx_manager: impl TransactionManager + 'static,
        batch_linger_time: Duration,
    ) -> Self {
        Self {
            anchor_client: Arc::new(anchor_client),
            tx_manager: Arc::new(tx_manager),
            batch_linger_time,
        }
    }

    pub async fn run(&mut self) {
        let mut interval = interval(self.batch_linger_time);
        loop {
            interval.tick().await;

            // Pass the anchor requests through a deduplication step to avoid anchoring multiple Data Events from the
            // same Stream.
            let anchor_requests: Vec<AnchorRequest> = IndexMap::<Cid, AnchorRequest>::from_iter(
                self.anchor_client
                    .get_anchor_requests()
                    .await
                    .into_iter()
                    .map(|request| (request.id, request)),
            )
            .into_values()
            .collect();

            // Anchor the batch to the CAS. This may block for a long time.
            match self.anchor_batch(anchor_requests.as_slice()).await {
                Ok(time_event_batch) => {
                    if let Err(e) = self.anchor_client.put_time_events(time_event_batch).await {
                        error!("error storing time events: {:?}", e);
                    }
                }
                Err(e) => {
                    error!("error anchoring batch: {:?}", e);
                }
            }
        }
    }

    async fn anchor_batch(&self, anchor_requests: &[AnchorRequest]) -> Result<TimeEventBatch> {
        let MerkleTree {
            root_cid,
            nodes,
            count,
        } = build_merkle_tree(anchor_requests)?;
        let Receipt {
            proof,
            detached_time_event,
            mut remote_merkle_nodes,
        } = self.tx_manager.make_proof(root_cid).await?;
        let time_events = build_time_events(anchor_requests, &detached_time_event, count)?;
        remote_merkle_nodes.extend(nodes);
        Ok(TimeEventBatch {
            merkle_nodes: remote_merkle_nodes,
            proof,
            time_events,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use expect_test::expect_file;
    use multihash_codetable::{Code, MultihashDigest};
    use ring::signature::Ed25519KeyPair;

    use ceramic_anchor_tx::{MockCas, RemoteCas};
    use ceramic_core::{ed25519_key_pair_from_secret, Cid};

    use crate::{
        anchor_batch::{AnchorClient, AnchorRequest, AnchorService},
        time_event::TimeEventBatch,
    };

    #[derive(Debug)]
    struct MockAnchorClient {
        pub anchor_req_count: u64,
    }

    fn node_private_key() -> Ed25519KeyPair {
        ed25519_key_pair_from_secret(
            std::env::var("NODE_PRIVATE_KEY")
                .unwrap_or(
                    "f80264c02abf947a7bd4f24fc799168a21cdea5b9d3a8ce8f63801785a4dff7299af4"
                        .to_string(),
                )
                .as_str(),
        )
        .unwrap()
    }

    impl MockAnchorClient {
        fn new(anchor_req_count: u64) -> Self {
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
        async fn get_anchor_requests(&self) -> Vec<AnchorRequest> {
            (0..self.anchor_req_count)
                .map(|n| AnchorRequest {
                    id: self.int64_cid(n),
                    prev: self.int64_cid(n),
                })
                .collect()
        }

        async fn put_time_events(&self, _batch: TimeEventBatch) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_anchor_batch() {
        let anchor_client = MockAnchorClient::new(10);
        let anchor_requests = anchor_client.get_anchor_requests().await;
        let anchor_service = AnchorService::new(anchor_client, MockCas, Duration::from_secs(1));
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await
            .unwrap();
        expect_file!["./test-data/test_anchor_batch.test.txt"].assert_debug_eq(&all_blocks);
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "test-network"), ignore)]
    async fn test_anchor_batch_with_cas() {
        let anchor_client = MockAnchorClient::new(10);
        let anchor_requests = anchor_client.get_anchor_requests().await;
        let remote_cas = RemoteCas::new(
            node_private_key(),
            "https://cas-dev.3boxlabs.com".to_owned(),
        );
        let anchor_service = AnchorService::new(anchor_client, remote_cas, Duration::from_secs(1));
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await
            .unwrap();
        expect_file!["./test-data/test_anchor_batch_with_cas.test.txt"]
            .assert_debug_eq(&all_blocks);
    }
}
