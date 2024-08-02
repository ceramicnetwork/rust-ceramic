use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use cid::Cid;
use tokio::time::sleep;

use ceramic_anchor_tree::{build_merkle_tree, build_time_events, AnchorRequest};
use ceramic_anchor_tx::TransactionManager;
use ceramic_core::DagCborIpfsBlock;

// Loop:
// 1.
// Call build_tree:
//   - Take an iterator of AnchorRequest
//   - Returns a RootCount.
// 2. Out-of-band, convert RootCount to an anchor proof.
// 3. Call build_time_events:
//   - Take an iterator for the same AnchorRequests as before (should be the same AnchorRequests in the same order)
//   - Take the proof CID from step 2.
//   - Take the count from the RootCount.
//   - Takes std::sync::mpsc::Sender<DagCborIpfsBlock> to send the TimeEvents to.

#[async_trait]
pub trait AnchorClient {
    // 1. Get the requests to anchor
    async fn get_requests(&self) -> Vec<AnchorRequest>;
    // 2. Validate the proof
    async fn validate_proof(&self, proof: &DagCborIpfsBlock) -> Result<()>;
    // 3. Put the Merkle Tree nodes
    async fn put_blocks(&self, blocks: &[DagCborIpfsBlock]) -> Result<()>;
    // 4. Process the Time Events using the Time Event CIDs and the previously stored blocks
    async fn process_time_events(&self, time_event_cids: &[Cid]) -> Result<()>;
}

struct AnchorService {
    anchor_client: Box<dyn AnchorClient>,
    tx_manager: Box<dyn TransactionManager>,
}

impl AnchorService {
    pub fn new(
        anchor_client: Box<dyn AnchorClient>,
        tx_manager: Box<dyn TransactionManager>,
    ) -> Self {
        Self {
            anchor_client,
            tx_manager,
        }
    }

    pub async fn anchor_loop(&self) {
        loop {
            // let result = self.anchor_client.
            //     anchor_batch(anchor_request, , &self.tx_manager).await; // call anchor batch.
            sleep(Duration::from_secs(300)).await; // sleep for 5 minutes.
        }
    }

    pub async fn anchor_batch(&self) -> Result<Vec<DagCborIpfsBlock>> {
        let anchor_batch = self.anchor_client.get_requests().await;
        // Create a channel to send all the blocks to. This is a sink for the blocks that only gets used once the Time
        // Events are successfully created.
        // build the local tree from the CIDs, calculate the root and put the block into the sink.
        let merkle_tree = build_merkle_tree(anchor_batch.iter()).await?;
        // perform the transaction and build the proof Receipt.
        let receipt = self.tx_manager.make_proof(merkle_tree.root_cid).await?;
        self.anchor_client
            .validate_proof(&receipt.proof_block)
            .await?;

        // make and sink the time events.
        let time_events = build_time_events(
            anchor_batch.iter(),
            receipt.proof_block.cid,
            receipt.path_prefix,
            merkle_tree.count,
        )?;

        let time_event_cids = time_events
            .iter()
            .map(|block| block.cid)
            .collect::<Vec<_>>();
        let all_blocks = receipt
            .blocks
            .into_iter()
            .chain(merkle_tree.nodes.into_iter())
            .chain(time_events.into_iter())
            .collect::<Vec<_>>();
        self.anchor_client.put_blocks(&all_blocks).await?;

        // Now that all the blocks have been sent, we can send the Time Events to the anchor client.
        self.anchor_client
            .process_time_events(&time_event_cids)
            .await?;

        Ok(all_blocks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ceramic_anchor_tx::{MockCas, RemoteCas};
    use ceramic_core::ed25519_key_pair_from_secret;
    use cid::Cid;
    use expect_test::expect_file;
    use multihash_codetable::{Code, MultihashDigest};
    use ring::signature::Ed25519KeyPair;

    #[derive(Debug)]
    struct MockAnchorClient {
        pub anchor_req_count: u64,
        pub blocks: Vec<DagCborIpfsBlock>,
        pub cids: Vec<Cid>,
    }

    impl MockAnchorClient {
        fn new(anchor_req_count: u64) -> Self {
            Self {
                anchor_req_count,
                blocks: vec![],
                cids: vec![],
            }
        }

        fn int64_cid(&self, i: u64) -> Cid {
            let data = i.to_be_bytes();
            let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
            Cid::new_v1(0x00, hash)
        }
    }

    #[async_trait]
    impl AnchorClient for MockAnchorClient {
        async fn get_requests(&self) -> Vec<AnchorRequest> {
            (0..self.anchor_req_count)
                .map(|n| AnchorRequest {
                    id: self.int64_cid(n),
                    prev: self.int64_cid(n),
                })
                .collect()
        }

        async fn validate_proof(&self, proof: &DagCborIpfsBlock) -> Result<()> {
            Ok(())
        }

        async fn put_blocks(&self, blocks: &[DagCborIpfsBlock]) -> Result<()> {
            Ok(())
        }

        async fn process_time_events(&self, _time_event_cids: &[Cid]) -> Result<()> {
            Ok(())
        }
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

    #[tokio::test]
    async fn test_anchor_batch() {
        let remote_cas = RemoteCas::new(
            node_private_key(),
            "https://cas-dev.3boxlabs.com".to_owned(),
        );
        let anchor_service =
            AnchorService::new(Box::new(MockAnchorClient::new(10)), Box::new(remote_cas));
        let all_blocks = anchor_service.anchor_batch().await.unwrap();
        expect_file!["./test-data/time-event-blocks.test.txt"].assert_debug_eq(&all_blocks);
    }

    #[tokio::test]
    #[ignore]
    async fn test_anchor_batch_with_cas() {
        let anchor_client = MockAnchorClient::new(10);
        let anchor_service = AnchorService::new(Box::new(anchor_client), Box::new(MockCas {}));
        let all_blocks = anchor_service.anchor_batch().await.unwrap();
        expect_file!["./test-data/time-event-blocks.test.txt"].assert_debug_eq(&all_blocks);
    }
}
