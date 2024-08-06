use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use cid::Cid;
use indexmap::IndexMap;
use tokio::time::interval;

use ceramic_anchor_tx::{Receipt, TransactionManager};
use ceramic_event::unvalidated::{Proof, RawTimeEvent};

use crate::{
    merkle_tree::{build_merkle_tree, MerkleTree},
    time_event::build_time_events,
};

#[async_trait]
pub trait AnchorClient {
    async fn get_anchor_requests(&self) -> Vec<AnchorRequest>;
    async fn put_time_events(&self, batch: TimeEventBatch) -> Result<()>;
}

// AnchorRequest request for a Time Event
pub struct AnchorRequest {
    pub id: Cid,   // The CID of the Stream
    pub prev: Cid, // The CID of the Event to be anchored
}

pub struct TimeEventBatch {
    merkle_tree_nodes: HashMap<Cid, Vec<Cid>>,
    proof: Proof,
    time_events: Vec<RawTimeEvent>,
}

pub struct AnchorService {
    tx_manager: Box<dyn TransactionManager>,
    anchor_client: Box<dyn AnchorClient>,
    batch_linger_time: Duration,
}

impl AnchorService {
    pub fn new(
        tx_manager: impl TransactionManager + 'static,
        anchor_client: impl AnchorClient + 'static,
        batch_linger_time: Duration,
    ) -> Self {
        Self {
            tx_manager: Box::new(tx_manager),
            anchor_client: Box::new(anchor_client),
            batch_linger_time,
        }
    }

    pub async fn anchor_loop(&mut self) -> Result<()> {
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
            let time_event_batch = self.anchor_batch(&anchor_requests.as_slice()).await?;

            self.anchor_client.put_time_events(time_event_batch).await?;
        }
    }

    async fn anchor_batch(&mut self, anchor_requests: &[AnchorRequest]) -> Result<TimeEventBatch> {
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
            merkle_tree_nodes: remote_merkle_nodes,
            proof,
            time_events,
        })
    }
}

#[cfg(test)]
mod tests {
    use expect_test::expect_file;
    use multihash_codetable::{Code, MultihashDigest};

    use ceramic_anchor_tx::{MockCas, RemoteCas};
    use ceramic_core::{Cid, DagCborIpfsBlock};

    use crate::anchor_batch::AnchorService;
    //
    //     let mut anchor_service = AnchorService::new(
    //     your_transaction_manager,
    //     1_000_000, // Max batch size of 1 million
    //     Duration::from_secs(300) // 5 minutes batch linger time
    // );
    //
    // // In a loop or scheduled task:
    // loop {
    // let anchor_requests = get_1000_anchor_requests_from_database(); // This should return an iterator or vec of AnchorRequest
    //
    // if let Some(blocks) = anchor_service.add_requests(anchor_requests).await? {
    // // Process the blocks (write to database, etc.)
    // for block in blocks {
    // write_block_to_database(block).await?;
    // }
    // }
    //
    // // Wait for a few minutes before the next poll
    // tokio::time::sleep(Duration::from_mins(5)).await;
    // }
    //     // fn intu64_cid(i: u64) -> Cid {
    //     let data = i.to_be_bytes();
    //     let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
    //     Cid::new_v1(0x00, hash)
    // }
    //
    //     async fn get_anchor_requests(&self) -> RequestStream {
    //         // Stream anchor requests
    //         (0..self.anchor_req_count)
    //             .map(|n| AnchorRequest {
    //                 id: intu64_cid(n),
    //                 prev: intu64_cid(n),
    //             })
    //             .into()
    //     }

    // #[tokio::test]
    // async fn test_anchor_batch() {
    //     let anchor_service =
    //         AnchorService::new(Box::new(MockAnchorClient::new(10)), Box::new(MockCas));
    //     let all_blocks = anchor_service.anchor_batch().await.unwrap();
    //     expect_file!["./test-data/test_anchor_batch.test.txt"].assert_debug_eq(&all_blocks);
    // }
    //
    // #[tokio::test]
    // #[ignore]
    // async fn test_anchor_batch_with_cas() {
    //     let anchor_client = MockAnchorClient::new(10);
    //     let remote_cas = RemoteCas::new(
    //         node_private_key(),
    //         "https://cas-dev.3boxlabs.com".to_owned(),
    //     );
    //     let anchor_service = AnchorService::new(Box::new(anchor_client), Box::new(remote_cas));
    //     let all_blocks = anchor_service.anchor_batch().await.unwrap();
    //     expect_file!["./test-data/test_anchor_batch_with_cas.test.txt"]
    //         .assert_debug_eq(&all_blocks);
    // }

    // use super::*;
    // use ceramic_anchor_tx::{MockCas, RemoteCas};
    // use ceramic_core::ed25519_key_pair_from_secret;
    // use cid::Cid;
    // use expect_test::expect_file;
    // use multihash_codetable::{Code, MultihashDigest};
    // use ring::signature::Ed25519KeyPair;
    //

    // #[tokio::test]
    // #[ignore]
    // async fn test_anchor_batch_with_cas() {
    //     let anchor_client = MockAnchorClient::new(10);
    //     let anchor_service = AnchorService::new(
    //         Box::new(anchor_client),
    //         Box::new(MockCas {}),
    //         10,
    //         Duration::from_secs(60),
    //     );
    //     let all_blocks = anchor_service.anchor_batch().await.unwrap();
    //     expect_file!["./test-data/time-event-blocks.test.txt"].assert_debug_eq(&all_blocks);
    // }

    //
    //
    // // Wait for a few minutes before the next poll
    // tokio::time::sleep(Duration::from_mins(5)).await;
    // }
    //     // fn intu64_cid(i: u64) -> Cid {
    //     let data = i.to_be_bytes();
    //     let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
    //     Cid::new_v1(0x00, hash)
    // }
    //
    //     async fn get_anchor_requests(&self) -> RequestStream {
    //         // Stream anchor requests
    //         (0..self.anchor_req_count)
    //             .map(|n| AnchorRequest {
    //                 id: intu64_cid(n),
    //                 prev: intu64_cid(n),
    //             })
    //             .into()
    //     }

    // #[tokio::test]
    // async fn test_anchor_batch() {
    //     let anchor_service =
    //         AnchorService::new(Box::new(MockAnchorClient::new(10)), Box::new(MockCas));
    //     let all_blocks = anchor_service.anchor_batch().await.unwrap();
    //     expect_file!["./test-data/test_anchor_batch.test.txt"].assert_debug_eq(&all_blocks);
    // }
    //
    // #[tokio::test]
    // #[ignore]
    // async fn test_anchor_batch_with_cas() {
    //     let anchor_client = MockAnchorClient::new(10);
    //     let remote_cas = RemoteCas::new(
    //         node_private_key(),
    //         "https://cas-dev.3boxlabs.com".to_owned(),
    //     );
    //     let anchor_service = AnchorService::new(Box::new(anchor_client), Box::new(remote_cas));
    //     let all_blocks = anchor_service.anchor_batch().await.unwrap();
    //     expect_file!["./test-data/test_anchor_batch_with_cas.test.txt"]
    //         .assert_debug_eq(&all_blocks);
    // }

    // use super::*;
    // use ceramic_anchor_tx::{MockCas, RemoteCas};
    // use ceramic_core::ed25519_key_pair_from_secret;
    // use cid::Cid;
    // use expect_test::expect_file;
    // use multihash_codetable::{Code, MultihashDigest};
    // use ring::signature::Ed25519KeyPair;
    //

    // #[tokio::test]
    // #[ignore]
    // async fn test_anchor_batch_with_cas() {
    //     let anchor_client = MockAnchorClient::new(10);
    //     let anchor_service = AnchorService::new(
    //         Box::new(anchor_client),
    //         Box::new(MockCas {}),
    //         10,
    //         Duration::from_secs(60),
    //     );
    //     let all_blocks = anchor_service.anchor_batch().await.unwrap();
    //     expect_file!["./test-data/time-event-blocks.test.txt"].assert_debug_eq(&all_blocks);
    // }
}
