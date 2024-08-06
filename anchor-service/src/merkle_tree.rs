use std::collections::HashMap;

use anyhow::{anyhow, Result};
use cid::Cid;

use ceramic_core::DagCborIpfsBlock;
use ceramic_event::unvalidated::MerkleNode;

use crate::anchor_batch::AnchorRequest;

pub struct MerkleTree {
    pub root_cid: Cid,
    pub nodes: HashMap<Cid, MerkleNode>,
    pub count: u64,
}

pub fn build_merkle_tree(anchor_requests: &[AnchorRequest]) -> Result<MerkleTree> {
    if anchor_requests.is_empty() {
        return Err(anyhow!("no requests to anchor"));
    }
    let mut peaks: [Option<Cid>; 64] = [None; 64];
    let mut count: u64 = 0;
    let mut nodes: HashMap<Cid, MerkleNode> = HashMap::new();
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
            match peaks[place_value].take() {
                Some(old_node_cid) => {
                    let merged_node: MerkleNode;
                    (new_node_cid, merged_node) = merge_nodes(old_node_cid, new_node_cid)?;
                    nodes.insert(new_node_cid, merged_node);
                    place_value += 1;
                }
                None => {
                    peaks[place_value] = Some(new_node_cid);
                    break;
                }
            }
        }
    }

    let mut peaks_iter = peaks.into_iter().flatten();
    let mut right_cid = peaks_iter.next().expect("should never be empty");
    for left_cid in peaks_iter {
        let merged_node: MerkleNode;
        (right_cid, merged_node) = merge_nodes(left_cid, right_cid)?;
        nodes.insert(right_cid, merged_node);
    }
    Ok(MerkleTree {
        root_cid: right_cid,
        count,
        nodes,
    })
}

/// Accepts the CIDs of two blocks and returns the CID of the CBOR list that includes both CIDs.
pub(crate) fn merge_nodes(left: Cid, right: Cid) -> Result<(Cid, MerkleNode)> {
    let merkle_node = vec![left, right];
    let block: DagCborIpfsBlock = serde_ipld_dagcbor::to_vec(&merkle_node)?.into();
    Ok((block.cid, merkle_node))
}
