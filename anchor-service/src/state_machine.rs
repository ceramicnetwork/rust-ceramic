use anyhow::Result;

use ceramic_anchor_tree::{build_merkle_tree, build_time_events, AnchorRequest};
use ceramic_anchor_tx::{Receipt, TransactionManager};
use ceramic_core::DagCborIpfsBlock;
// use tokio::time::sleep;

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

// pub async fn anchor_batch(){
//     loop{
//         let anchor_request: Vec<AnchorRequest> = todo!(); // get anchor requests for batch
//         let result = anchor_batch(anchor_request, time_event_block_sink, tx_manager).await; // call anchor batch.
//         sleep(Duration::from_secs(300)); // sleep for 5 minutes.
//     }
// }

pub async fn anchor_batch(
    anchor_requests: Vec<AnchorRequest>,
    time_event_block_sink: std::sync::mpsc::Sender<DagCborIpfsBlock>,
    tx_manager: impl TransactionManager + 'static,
) -> Result<()> {
    // build the local tree from the CIDs, calculate the root and put the block into the sink.
    let root_count = build_merkle_tree(anchor_requests.iter(), time_event_block_sink.clone())
        .await
        .unwrap();
    // perform the transaction and build the proof Receipt.
    let Receipt {
        proof_cid,
        path_prefix,
        blocks,
    } = tx_manager.make_proof(root_count.root).await?;
    // sink the proof block and path blocks.
    // this call may have a transaction polling loop here for hours.
    for block in blocks {
        time_event_block_sink.send(block)?;
    }
    // make and sink the time events.
    build_time_events(
        anchor_requests.iter(),
        proof_cid,
        path_prefix,
        root_count.count,
        time_event_block_sink.clone(),
    )?;
    // TODO: Pretend we wrote out all the blocks
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ceramic_anchor_tx::RemoteCas;
    use cid::Cid;
    use expect_test::{expect, expect_file};
    use multihash_codetable::{Code, MultihashDigest};
    use std::sync::mpsc::channel;

    fn int64_cid(i: i64) -> Cid {
        let data = i.to_be_bytes();
        let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
        Cid::new_v1(0x00, hash)
    }

    fn mock_anchor_requests(count: i64) -> (i64, Vec<AnchorRequest>) {
        (
            count,
            (0..count)
                .map(|n| AnchorRequest {
                    id: int64_cid(n),
                    prev: int64_cid(n),
                })
                .collect(),
        )
    }
    // {"id":"318e8da1-481a-48e6-8fa5-2de57845b732","status":"PENDING","cid":"bafyreieqd2axydivliztnumdtxzlygqffagbrq7ddw7jyiw5jjl62cdzpq","streamId":"k2t6wzu5p07hh5hakjz640047w0vm6xtfny15z1kx3r9i8wdbvd8n0w969901o","message":"Request is pending.","createdAt":1722383636955,"updatedAt":1722383636955}
    #[tokio::test]
    #[ignore]
    async fn test_anchor() {
        let (_, anchor_requests) = mock_anchor_requests(10);
        let (time_event_block_sink_tx, time_event_block_sink_rx) = channel();
        let remote_cas = RemoteCas::new(
            std::env::var("NODE_DID")
                .unwrap_or("did:key:z6MkueF19qChpGQJBJXcXjfoM1MYCwC167RMwUiNWXXvEm1M".to_string()),
            hex::decode(std::env::var("NODE_PRIVATE_KEY").unwrap_or(
                "4c02abf947a7bd4f24fc799168a21cdea5b9d3a8ce8f63801785a4dff7299af4".to_string(),
            ))
            .unwrap()
            .try_into()
            .unwrap(),
            "https://cas-dev.3boxlabs.com".to_owned(),
        );
        anchor_batch(anchor_requests, time_event_block_sink_tx, remote_cas)
            .await
            .unwrap();
        // Pull all the blocks out of the channel
        let time_event_blocks: Vec<DagCborIpfsBlock> = time_event_block_sink_rx.iter().collect();
        expect_file!["./test-data/time-event-blocks.test.txt"].assert_debug_eq(&time_event_blocks);
    }
}
