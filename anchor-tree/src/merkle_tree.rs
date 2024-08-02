use std::borrow::Cow;

use anyhow::{anyhow, Result};
use cid::Cid;

use ceramic_core::DagCborIpfsBlock;

pub struct MerkleTree<'a> {
    pub root_cid: Cid,
    pub count: u64,
    pub nodes: Cow<'a, [DagCborIpfsBlock]>,
    pub peak_nodes: Vec<DagCborIpfsBlock>,
}

impl<'a> MerkleTree<'a> {
    pub fn next_blocks(&'a self) -> AtMostOnceTraversal<'a> {
        AtMostOnceTraversal {
            tree: self,
            current_leaf: 0,
        }
    }

    // This function only returns unseen intermediate nodes and will NOT return the leaf node at the supplied index.
    pub fn get_unseen_path_nodes(&self, index: u64) -> Result<Vec<&DagCborIpfsBlock>> {
        let path = self.index_to_path(index)?;
        let path_parts: Vec<&str> = path.split('/').collect();

        let mut unseen_nodes = Vec::new();
        let mut current_index = 0;
        let mut is_right_child = false;

        for &direction in &path_parts {
            let node = self
                .nodes
                .get(current_index as usize)
                .ok_or_else(|| anyhow!("node not found at index {}", current_index))?;

            if is_right_child {
                unseen_nodes.push(node);
            }

            match direction {
                "0" => {
                    current_index = 2 * current_index + 1;
                    is_right_child = false;
                }
                "1" => {
                    current_index = 2 * current_index + 2;
                    is_right_child = true;
                }
                _ => {
                    return Err(anyhow!(
                        "invalid direction in path: {}, {}",
                        path,
                        direction
                    ))?;
                }
            }
        }

        // Add relevant unseen peak nodes
        let mut current_size = index + 1;
        let mut peak_index = 0;
        while current_size > 0 {
            let peak_size = current_size.next_power_of_two() / 2;
            if current_size >= peak_size {
                if peak_index >= self.peak_nodes.len() {
                    return Err(anyhow!("peak node not found at index {}", peak_index));
                }
                unseen_nodes.push(&self.peak_nodes[peak_index]);
                current_size -= peak_size;
            }
            peak_index += 1;
        }

        Ok(unseen_nodes)
    }

    pub fn highest_peak_index(&self, index: u64) -> usize {
        let mut size = index + 1;
        let mut peak_index = 0;
        while size > 1 {
            let next_power_of_two = size.next_power_of_two();
            if size != next_power_of_two {
                size -= next_power_of_two / 2;
                peak_index += 1;
            } else {
                break;
            }
        }
        peak_index
    }

    pub fn path_iter(&self) -> impl Iterator<Item = Result<Vec<&DagCborIpfsBlock>>> + '_ {
        (0..self.count).map(move |index| self.get_unseen_path_nodes(index))
    }
}

pub struct AtMostOnceTraversal<'a> {
    tree: &'a MerkleTree<'a>,
    current_leaf: u64,
}

impl<'a> Iterator for AtMostOnceTraversal<'a> {
    type Item = Result<Vec<&'a DagCborIpfsBlock>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_leaf >= self.tree.count {
            return None;
        }

        let result = self.tree.get_unseen_path_nodes(self.current_leaf);
        self.current_leaf += 1;
        Some(result)
    }
}
