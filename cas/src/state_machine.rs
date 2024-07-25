use crate::block_builder::build_time_events;
use crate::block_builder::{
    build_time_event, build_tree, AnchorRequest, DagCborIpfsBlock, RootCount,
};
use crate::transaction_manager::make_proof;
use cid::Cid;
use multihash_codetable::{Code, MultihashDigest};
use serde::{Deserialize, Serialize};
use std::sync::mpsc::channel;

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

async fn anchor_loop() {
    loop {
        let anchor_requests: Vec<AnchorRequest> = vec![];
        let (sender, _receiver) = channel();
        let root_count = build_tree(anchor_requests.iter(), sender.clone())
            .await
            .unwrap();
        let Ok((proof_cid, path_prefix, blocks)) = make_proof(root_count.root).await else {
            continue;
        };
        for block in blocks {
            sender.send(block).unwrap();
        }
        build_time_events(
            anchor_requests.iter(),
            proof_cid,
            Some(path_prefix),
            root_count.count,
            sender.clone(),
        )
        .unwrap();
        // TODO: Pretend we wrote out all the blocks
    }
}
