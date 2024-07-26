use std::sync::mpsc::channel;

use ceramic_anchor_tree::{build_merkle_tree, build_time_events, AnchorRequest};
use ceramic_anchor_tx::{MockCas, RemoteCas, TransactionManager};

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

pub async fn anchor_loop(tx_manager: impl TransactionManager + Send + Sync + 'static) {
    loop {
        let anchor_requests: Vec<AnchorRequest> = vec![];
        let (sender, _receiver) = channel();
        let root_count = build_merkle_tree(anchor_requests.iter(), sender.clone())
            .await
            .unwrap();
        let Ok((proof_cid, path_prefix, blocks)) = tx_manager.make_proof(root_count.root).await
        else {
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
