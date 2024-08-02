use anyhow::{anyhow, Result};
use ceramic_core::DagCborIpfsBlock;
use cid::Cid;
use serde::{Deserialize, Serialize};

// AnchorRequest request for a Time Event
pub struct AnchorRequest {
    pub id: Cid,   // The CID of the Stream
    pub prev: Cid, // The CID of the Event to be anchored
}

#[derive(Debug)]
pub struct MerkleTree {
    pub root_cid: Cid,
    pub count: u64,
    pub nodes: Vec<DagCborIpfsBlock>,
}

/// Accepts the CIDs of two blocks and returns the CID of the CBOR list that includes both CIDs.
fn merge_nodes(left: &Cid, right: Option<&Cid>) -> Result<DagCborIpfsBlock> {
    let merkle_node = vec![Some(*left), right.cloned()];
    Ok(serde_ipld_dagcbor::to_vec(&merkle_node)?.into())
}

pub fn build_time_events<'a, I>(
    anchor_requests: I,
    proof: Cid,
    path_prefix: Option<String>,
    count: u64,
) -> Result<Vec<DagCborIpfsBlock>>
where
    I: Iterator<Item = &'a AnchorRequest>,
{
    let mut time_events: Vec<DagCborIpfsBlock> = vec![];
    for (index, anchor_request) in anchor_requests.into_iter().enumerate() {
        let time_event = build_time_event(
            anchor_request.id,
            anchor_request.prev,
            proof,
            path_prefix.clone(),
            index.try_into().unwrap(),
            count,
        )?;
        time_events.push(serde_ipld_dagcbor::to_vec(&time_event).unwrap().into());
    }
    Ok(time_events)
}

/// Fetch unanchored CIDs from the Anchor Request Store and builds a Merkle tree from them.
pub async fn build_merkle_tree<'a, I>(anchor_requests: I) -> Result<MerkleTree>
where
    I: Iterator<Item = &'a AnchorRequest>,
{
    let mut peaks: [Option<Cid>; 64] = [None; 64];
    let mut count: u64 = 0;
    let mut nodes: Vec<DagCborIpfsBlock> = vec![];
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
                    let merged_node = merge_nodes(&old_node_cid, Some(&new_node_cid))?;
                    nodes.push(merged_node.clone());
                    new_node_cid = merged_node.cid;
                    place_value += 1;
                }
                None => {
                    peaks[place_value] = Some(new_node_cid);
                    break;
                }
            }
        }
    }
    let mut right_cid: Option<Cid> = None;
    for left_cid in peaks.iter().flatten() {
        let merged_node = merge_nodes(left_cid, right_cid.as_ref())?;
        nodes.push(merged_node.clone());
        right_cid = Some(merged_node.cid);
    }
    match right_cid {
        Some(root_cid) => Ok(MerkleTree {
            root_cid,
            count,
            nodes,
        }),
        None => Err(anyhow!("no CIDs in iterator")),
    }
}

fn index_to_path(index: u64, length: u64) -> Result<String> {
    // we want to find the path to the index in a tree length.
    // first find the sub-tree then append the path in the sub-tree
    //
    // e.g. 5, 6
    // 14 = 0b1110
    // 10 = 0b1010
    //               [root]
    //         /                  [\]
    //     /      \           [/]      \
    //    / \    /  \       /    [\]    / \
    //   /\  /\ / \ / \   / \  [/]  \  12 13
    //  0 1 2 3 4 5 6 7   8 9 [10]  11
    //
    // find the tree the index is in.
    // MSB of 14 is 8; 10 > 8; -= 8; go right "/1"
    // MSB of 6 is 4; 2 !> 4; -= 4; go left "/0"
    // append the remaining bits of index as path in the sub-tree.
    // 2 is 0b10 so right "/1" then left "/0"
    // final {"path": "1/0/1/0"} for index 10 of length 14.

    if index > length {
        return Err(anyhow!("index({}) > length({})", index, length));
    }
    let mut index = index;
    let mut length = length;

    let mut path = String::new();
    while length != 0 {
        let top_power_of_2 = 1 << (63 - length.leading_zeros());
        if index < top_power_of_2 {
            // the index is in the left tree
            path += "0/";
            break;
        } else {
            // the index is in the right tree
            path += "1/";
            length -= top_power_of_2;
            index -= top_power_of_2;
        }
    }
    for bit in format!("{:b}", index).chars() {
        path += &format!("{}/", bit);
    }
    Ok(path
        .strip_suffix('/')
        .expect("path ends with /")
        .to_string())
}

/// TimeEvent is a single TimeEvent anchored to the chain.
#[derive(Serialize, Deserialize, Debug)]
pub struct TimeEvent {
    pub id: Cid,
    pub prev: Cid,
    pub proof: Cid,
    pub path: String,
}

pub fn build_time_event(
    id: Cid,
    prev: Cid,
    proof: Cid,
    path_prefix: Option<String>,
    index: u64,
    count: u64,
) -> Result<TimeEvent> {
    let time_event = TimeEvent {
        id,
        prev,
        proof,
        path: match path_prefix {
            // the path should not have a `/` at the beginning or end e.g. 0/1/0
            Some(path_prefix) => format!("{}/{}", path_prefix, index_to_path(index, count)?),
            None => index_to_path(index, count)?,
        },
    };
    Ok(time_event)
}

/// Tests to ensure that the merge function is working as expected.
#[cfg(test)]
mod tests {
    use super::*;
    use expect_test::expect;
    use multihash_codetable::{Code, MultihashDigest};

    #[test]
    fn test_merge() {
        let left = "bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq"
            .try_into()
            .unwrap();
        let right = Some(
            "bagcqcerat32m2zad2xwu5pvsegaj2fa7x3nwnbvwqzvjy3usgc4af7ldkpgq"
                .try_into()
                .unwrap(),
        );
        expect![[r#"
            DagCborIpfsBlock {
                cid: "bafyreiabkbt7ctfodcqv7lv4vti5a7a4hxhsujbdgtdrjdpe2i27e4yiyy",
                data: "82d82a58260001850112207ac18e1235f2f7a84548eeb543a33f8979eb88566a2dfc3d9596c55a683b7517d82a58260001850112209ef4cd6403d5ed4ebeb221809d141fbedb6686b6866a9c6e9230b802fd6353cd",
            }
        "#]].assert_debug_eq(&merge_nodes(&left, right.as_ref()).unwrap());
    }

    fn int128_cid(i: i128) -> Cid {
        let data = i.to_be_bytes();
        let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
        Cid::new_v1(0x00, hash)
    }

    fn mock_anchor_requests() -> (i128, Vec<AnchorRequest>) {
        let count = 20;
        (
            count,
            (0..count)
                .map(|n| AnchorRequest {
                    id: int128_cid(n),
                    prev: int128_cid(n),
                })
                .collect(),
        )
    }

    #[tokio::test]
    async fn test_time_event() {
        let id =
            Cid::try_from("baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq").unwrap();
        let prev =
            Cid::try_from("baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq").unwrap();
        let proof =
            Cid::try_from("bafyreidq247kfkizr3k6wlvx43lt7gro2dno7vzqepmnqt26agri4opzqu").unwrap();
        let index = 500_000;
        let count = 999_999;
        let time_event = build_time_event(id, prev, proof, Some("".to_owned()), index, count);
        expect![[r#"Ok(TimeEvent { id: Cid(baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq), prev: Cid(baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq), proof: Cid(bafyreidq247kfkizr3k6wlvx43lt7gro2dno7vzqepmnqt26agri4opzqu), path: "/0/1/1/1/1/0/1/0/0/0/0/1/0/0/1/0/0/0/0/0" })"#]]
        .assert_eq(&format!("{:?}", time_event));
    }

    #[tokio::test]
    async fn test_time_events() {
        let (count, anchor_requests) = mock_anchor_requests();
        let proof =
            Cid::try_from("bafyreicv5owrmctiwt3qhfpto6bs4ld3bxcqwxxjttpfvvzcqljr7bfmum").unwrap();

        let result = build_time_events(
            anchor_requests.iter(),
            proof,
            Some("".to_owned()),
            count.try_into().unwrap(),
        );
        expect![[r#"
            Ok(
                (),
            )
        "#]]
        .assert_debug_eq(&result);
    }

    #[tokio::test]
    async fn test_index_to_path() {
        // '1/'  10 > 8, 14
        // '1/0/' 2 > 4, 6
        // '1/0/' 0b10
        // '1/0/1/0'
        expect![[r#"
            Ok(
                "1/0/1/0",
            )
        "#]]
        .assert_debug_eq(&index_to_path(10, 14));

        // '0/' 500_000 < 524288, 1_000_000
        // '0/' 0b1111010000100100000
        // '0/1/1/1/1/0/1/0/0/0/0/1/0/0/1/0/0/0/0/0/'
        expect![[r#"
            Ok(
                "0/1/1/1/1/0/1/0/0/0/0/1/0/0/1/0/0/0/0/0",
            )
        "#]]
        .assert_debug_eq(&index_to_path(500_000, 1_000_000));

        // '1/'        999_999 > 524288,  1_000_000
        // '1/1/'      475_711 > 262_144, 475_712
        // '1/1/1/'    213_567 > 131072,  213_568
        // '1/1/1/1/'   82_495 > 65_536,  82_496
        // '1/1/1/1/1/' 16_959 > 16_384,  16_960
        // '1/1/1/1/1/1/'  575 > 512,     576
        // '1/1/1/1/1/1/0/' 63 !> 64,     64
        // '1/1/1/1/1/1/0/' 0b111111
        // '1/1/1/1/1/1/0/1/1/1/1/1/1/'
        expect![[r#"
        Ok(
            "1/1/1/1/1/1/0/1/1/1/1/1/1",
        )
    "#]]
        .assert_debug_eq(&index_to_path(999_999, 1_000_000));
    }
}
