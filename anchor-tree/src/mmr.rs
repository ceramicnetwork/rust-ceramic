use std::borrow::Cow;

use anyhow::{anyhow, Result};
use cid::Cid;

use ceramic_core::DagCborIpfsBlock;

use crate::merkle_tree::MerkleTree;

pub struct MerkleMountainRange {
    peaks: [Option<Cid>; 64],
    count: u64,
    nodes: Vec<DagCborIpfsBlock>,
}

impl Default for MerkleMountainRange {
    fn default() -> Self {
        Self {
            peaks: [None; 64],
            count: 0,
            nodes: Vec::new(),
        }
    }
}

impl MerkleMountainRange {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn append(&mut self, cids: &[Cid]) -> Result<()> {
        for cid in cids {
            self.push(*cid)?;
        }

        Ok(())
    }

    pub fn push(&mut self, cid: Cid) -> Result<()> {
        self.count += 1;
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
        let mut new_node_cid = cid;
        let mut place_value = 0;

        while place_value < self.peaks.len() {
            match self.peaks[place_value].take() {
                Some(old_node_cid) => {
                    let merged_node = merge_nodes(&old_node_cid, Some(&new_node_cid))?;
                    self.nodes.push(merged_node.clone());
                    new_node_cid = merged_node.cid;
                    place_value += 1;
                }
                None => {
                    self.peaks[place_value] = Some(new_node_cid);
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn build_merkle_tree(&self) -> Result<MerkleTree> {
        let mut right_cid: Option<Cid> = None;
        let mut peak_nodes = Vec::new();

        for left_cid in self.peaks.iter().flatten().rev() {
            let merged_node = merge_nodes(left_cid, right_cid.as_ref())?;
            peak_nodes.push(merged_node.clone());
            right_cid = Some(merged_node.cid);
        }

        match right_cid {
            Some(root_cid) => Ok(MerkleTree {
                root_cid,
                count: self.count,
                nodes: Cow::Borrowed(&self.nodes),
                peak_nodes,
            }),
            None => Err(anyhow!("no peaks in mmr")),
        }
    }
}
