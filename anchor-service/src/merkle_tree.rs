use crate::anchor::{AnchorRequest, MerkleNode, MerkleNodes};
use anyhow::{anyhow, Result};
use ceramic_core::SerializeExt;
use cid::Cid;

pub struct MerkleTree {
    pub root_cid: Cid,
    pub nodes: MerkleNodes,
    pub count: u64,
}

/// Make a tree using Merkle mountain range
/// Ref: https://eprint.iacr.org/2021/038.pdf
pub fn build_merkle_tree(anchor_requests: &[AnchorRequest]) -> Result<MerkleTree> {
    // For size zero trees return an error
    if anchor_requests.is_empty() {
        return Err(anyhow!("no requests to anchor"));
    }
    // The roots of the sub-trees with full power of 2 trees.
    // They are in the places in the array where the 1s are in the count u64.
    // e.g. 13 = 0b1110
    //                root
    //         /                    \
    //     /      \            /        \
    //    / \    /  \       /     \     / \
    //   /\  /\ / \ / \   / \   /   \  12 13
    //  0 1 2 3 4 5 6 7   8 9  10   11
    //
    // here the peeks are [none, 12..13, 8..11, 0..7]
    // place values         1's,    2's,   4's,  8's
    let mut peaks: Vec<Option<Cid>> = vec![None; 64];

    // The nodes in the Merkle Map[node_cid, [left_cid, right_cid]]
    let mut nodes = MerkleNodes::default();

    // insert all the `anchor_request.prev`s into the peaks.
    for anchor_request in anchor_requests {
        let mut new_node_cid: Cid = anchor_request.prev;
        for peek in peaks.iter_mut() {
            // walk the place values
            match peek {
                None => {
                    // if the place values peek is empty put the cid there
                    *peek = Some(new_node_cid);
                    // we found a place to put it. we are done
                    break;
                }
                Some(old_node_cid) => {
                    // if the place values peek is occupied add the old cid to the new cid and carry to the next place values peek
                    let merged_node: MerkleNode;
                    (new_node_cid, merged_node) = merge_nodes(*old_node_cid, new_node_cid)?;
                    // remember the generated nodes
                    nodes.insert(new_node_cid, merged_node);
                    // clear the place value peek we took old node from
                    *peek = None;
                }
            }
        }
    }

    // Roll up the peaks into a root.
    // Since each tree is larger then all the preceding trees combined
    // we just walk the non empty peeks putting the last peek on the left.
    let mut peaks_iter = peaks.into_iter().flatten();
    let mut right_cid = peaks_iter.next().expect("should never be empty");
    for left_cid in peaks_iter {
        let merged_node: MerkleNode;
        (right_cid, merged_node) = merge_nodes(left_cid, right_cid)?;
        nodes.insert(right_cid, merged_node);
    }

    Ok(MerkleTree {
        root_cid: right_cid,
        count: anchor_requests.len() as u64,
        nodes,
    })
}

/// Accepts the CIDs of two blocks and returns the CID of the CBOR list that includes both CIDs.
pub(crate) fn merge_nodes(left: Cid, right: Cid) -> Result<(Cid, MerkleNode)> {
    let merkle_node = vec![Some(left), Some(right)];
    Ok((merkle_node.to_cid()?, merkle_node))
}
