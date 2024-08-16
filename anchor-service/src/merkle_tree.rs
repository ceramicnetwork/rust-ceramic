use anyhow::{anyhow, Result};
use ceramic_core::{Cid, SerdeIpld};

use ceramic_event::anchor::{AnchorRequest, MerkleNode, MerkleNodes};

pub struct MerkleTree {
    pub root_cid: Cid,
    pub nodes: MerkleNodes,
    pub count: u64,
}

pub fn build_merkle_tree(anchor_requests: &[AnchorRequest]) -> Result<MerkleTree> {
    if anchor_requests.is_empty() {
        return Err(anyhow!("no requests to anchor"));
    }
    // The roots of the sub-trees with full power of 2 nodes.
    // They are in the places in the array where the 1s are in the count u64.
    let mut peaks: [Option<Cid>; 64] = [None; 64];
    let mut count: u64 = 0;
    let mut nodes = MerkleNodes::default();

    // insert all the `anchor_request.prev`s into the peaks.
    for anchor_request in anchor_requests {
        count += 1;
        // Ref: https://eprint.iacr.org/2021/038.pdf
        // []
        // [cid1]
        // [none, (cid1, cid2)]
        // [cid3, (cid1, cid2)]
        // [none, none, ((cid1, cid2), (cid3, cid4))]
        // [cid5, none, ((cid1, cid2), (cid3, cid4))]
        // [none, (cid5, cid6), ((cid1, cid2), (cid3, cid4))]
        // [cid7, (cid5, cid6), ((cid1, cid2), (cid3, cid4))]
        // [none, none, none, (((cid1, cid2), (cid3, cid4)), ((cid5, cid6), (cid7, cid8))]
        let mut new_node_cid: Cid = anchor_request.prev;
        let mut place_value = 0;
        while place_value < peaks.len() {
            // take removes the old node from the place_value.
            match peaks[place_value].take() {
                Some(old_node_cid) => {
                    // If there was a node in that place we add new_node and old_node
                    let merged_node: MerkleNode;
                    (new_node_cid, merged_node) = merge_nodes(old_node_cid, new_node_cid)?;
                    // store the node in the nodes
                    nodes.insert(new_node_cid, merged_node);
                    place_value += 1;
                    // we carry the new node to the next place_value
                }
                None => {
                    // If there was no node in the place_value put the new node there
                    peaks[place_value] = Some(new_node_cid);
                    // nothing to carry we are done adding.
                    break;
                }
            }
        }
    }

    // Roll up the peaks into a root.
    // Since each tree is larger then all the preceding trees combined
    // we just walk the Somes in the peeks putting the last peek on the left.
    let mut peaks_iter = peaks.into_iter().flatten();
    let mut right_cid = peaks_iter.next().expect("should never be empty");
    for left_cid in peaks_iter {
        let merged_node: MerkleNode;
        (right_cid, merged_node) = merge_nodes(left_cid, right_cid)?;
        nodes.insert(right_cid, merged_node);
    }
    // e.g. 13 = 0b1110
    //                root
    //         /                    \
    //     /      \            /        \
    //    / \    /  \       /     \     / \
    //   /\  /\ / \ / \   / \   /   \  12 13
    //  0 1 2 3 4 5 6 7   8 9  10   11
    Ok(MerkleTree {
        root_cid: right_cid,
        count,
        nodes,
    })
}

/// Accepts the CIDs of two blocks and returns the CID of the CBOR list that includes both CIDs.
pub(crate) fn merge_nodes(left: Cid, right: Cid) -> Result<(Cid, MerkleNode)> {
    let merkle_node = vec![Some(left), Some(right)];
    Ok((merkle_node.to_cid()?, merkle_node))
}
