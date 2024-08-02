use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use cid::Cid;
use futures::stream::{self, Stream, StreamExt};
use indexmap::IndexMap;
use tokio::time::timeout;

use ceramic_anchor_tree::{AnchorRequest, MerkleMountainRange, MerkleTree, TimeEvent};
use ceramic_anchor_tx::{Receipt, TransactionManager};
use ceramic_core::DagCborIpfsBlock;

pub type RequestStream = Box<dyn Stream<Item = AnchorRequest> + Send + Unpin + 'static>;
pub type BlockStream = Box<dyn Stream<Item = DagCborIpfsBlock> + Send + Unpin + 'static>;

#[async_trait]
pub trait AnchorClient {
    fn get_anchor_requests(&self) -> RequestStream;
    async fn validate_anchor_proof(&self, proof: &DagCborIpfsBlock) -> Result<()>;
}

pub struct AnchorService {
    anchor_client: Arc<dyn AnchorClient>,
    tx_manager: Arc<dyn TransactionManager>,
    max_batch_size: u64,
    max_linger_time: Duration,
    poll_interval: Duration,
}

impl AnchorService {
    pub fn new(
        anchor_client: Arc<dyn AnchorClient>,
        tx_manager: Arc<dyn TransactionManager>,
        max_batch_size: u64,
        max_linger_time: Duration,
        poll_interval: Duration,
    ) -> Self {
        Self {
            anchor_client,
            tx_manager,
            max_batch_size,
            max_linger_time,
            poll_interval,
        }
    }

    pub fn generate_blocks(
        &self,
    ) -> Box<(dyn futures::Stream<Item = DagCborIpfsBlock> + std::marker::Send + Unpin + 'static)>
    {
        stream::unfold(
            self.anchor_client.get_anchor_requests(),
            move |mut request_stream| async move {
                match self.anchor_batch(&mut request_stream).await {
                    Ok(Some(blocks)) => {
                        let stream: BlockStream = Box::new(Box::pin(stream::iter(blocks)));
                        Some((stream, request_stream))
                    }
                    Ok(None) => {
                        tokio::time::sleep(self.poll_interval).await;
                        let stream: BlockStream = Box::new(Box::pin(stream::empty()));
                        Some((stream, request_stream))
                    }
                    Err(e) => {
                        anyhow!("Error anchoring batch: {:?}", e);
                        let stream: BlockStream = Box::new(Box::pin(stream::empty()));
                        Some((stream, request_stream))
                    }
                }
            },
        )
        .flatten()
    }

    async fn anchor_batch(
        &self,
        request_stream: &mut RequestStream,
    ) -> Result<Option<Vec<DagCborIpfsBlock>>> {
        let mut anchor_requests = IndexMap::new();

        loop {
            match timeout(self.max_linger_time, request_stream.next()).await {
                Ok(Some(anchor_request)) => {
                    anchor_requests.insert(anchor_request.id, anchor_request.prev);
                    if anchor_requests.len() >= (self.max_batch_size as usize) {
                        break;
                    }
                }
                Ok(None) | Err(_) => {
                    break;
                }
            }
        }

        if anchor_requests.is_empty() {
            return Ok(None);
        }

        let mut mmr = MerkleMountainRange::new();
        mmr.append(&anchor_requests.values().collect())?;

        let merkle_tree = mmr.build_merkle_tree()?;
        let receipt = self.tx_manager.make_proof(merkle_tree.root_cid).await?;
        self.anchor_client
            .validate_anchor_proof(&receipt.proof_block)
            .await?;

        let blocks: Vec<_> = merkle_tree
            .next_blocks()
            .flat_map(|result| result.into_iter().flatten())
            .enumerate()
            .map(|(i, node)| {
                if i == 0 {
                    if let Some((index, _, anchor_request)) = anchor_requests.get_full(&node.cid) {
                        build_time_event(anchor_request, &receipt, &merkle_tree, index as u64)
                    } else {
                        Err(anyhow!("Anchor request not found for CID: {:?}", node.cid))
                    }
                } else {
                    Ok(node.clone())
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Some(blocks))
    }
}

pub fn build_time_event(
    anchor_request: &AnchorRequest,
    receipt: &Receipt,
    merkle_tree: &MerkleTree,
    index: u64,
) -> Result<DagCborIpfsBlock> {
    let path = merkle_tree.index_to_path(index)?;
    let time_event = TimeEvent {
        id: anchor_request.id,
        prev: anchor_request.prev,
        proof: receipt.proof_block.cid,
        path: match &receipt.path_prefix {
            // the path should not have a `/` at the beginning or end e.g. 0/1/0
            Some(prefix) => format!("{}/{}", prefix, path),
            None => path,
        },
    };
    Ok(serde_ipld_dagcbor::to_vec(&time_event)?.into())
}

#[cfg(test)]
mod tests {
    // use super::*;
    // use ceramic_anchor_tx::{MockCas, RemoteCas};
    // use ceramic_core::ed25519_key_pair_from_secret;
    // use cid::Cid;
    // use expect_test::expect_file;
    // use multihash_codetable::{Code, MultihashDigest};
    // use ring::signature::Ed25519KeyPair;
    //
    // #[derive(Debug)]
    // struct MockAnchorClient {
    //     pub anchor_req_count: u64,
    //     pub blocks: Vec<DagCborIpfsBlock>,
    //     pub cids: Vec<Cid>,
    // }
    //
    // impl MockAnchorClient {
    //     fn new(anchor_req_count: u64) -> Self {
    //         Self {
    //             anchor_req_count,
    //             blocks: vec![],
    //             cids: vec![],
    //         }
    //     }
    //
    //     fn int64_cid(&self, i: u64) -> Cid {
    //         let data = i.to_be_bytes();
    //         let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
    //         Cid::new_v1(0x00, hash)
    //     }
    // }
    //
    // #[async_trait]
    // impl AnchorClient for MockAnchorClient {
    //     async fn get_anchor_requests(&self) -> Vec<AnchorRequest> {
    //         (0..self.anchor_req_count)
    //             .map(|n| AnchorRequest {
    //                 id: self.int64_cid(n),
    //                 prev: self.int64_cid(n),
    //             })
    //             .collect()
    //     }
    //
    //     async fn validate_anchor_proof(&self, proof: &DagCborIpfsBlock) -> Result<()> {
    //         Ok(())
    //     }
    // }
    //
    // fn node_private_key() -> Ed25519KeyPair {
    //     ed25519_key_pair_from_secret(
    //         std::env::var("NODE_PRIVATE_KEY")
    //             .unwrap_or(
    //                 "f80264c02abf947a7bd4f24fc799168a21cdea5b9d3a8ce8f63801785a4dff7299af4"
    //                     .to_string(),
    //             )
    //             .as_str(),
    //     )
    //     .unwrap()
    // }
    //
    // #[tokio::test]
    // async fn test_anchor_batch() {
    //     let remote_cas = RemoteCas::new(
    //         node_private_key(),
    //         "https://cas-dev.3boxlabs.com".to_owned(),
    //     );
    //     let anchor_service = AnchorService::new(
    //         Box::new(MockAnchorClient::new(10)),
    //         Box::new(remote_cas),
    //         10,
    //         Duration::from_secs(60),
    //     );
    //     let all_blocks = anchor_service.anchor_batch().await.unwrap();
    //     expect_file!["./test-data/time-event-blocks.test.txt"].assert_debug_eq(&all_blocks);
    // }
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
