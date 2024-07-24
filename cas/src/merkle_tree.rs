use std::iter::StepBy;

use anyhow::{anyhow, Result};

use cid::Cid;
use ipld_core::{codec::Codec, ipld::Ipld};
use multihash_codetable::{Code, MultihashDigest};
use serde_ipld_dagcbor::codec::DagCborCodec;

use ceramic_store::SqlitePool;

/// Accepts the CIDs of two blocks and returns the CID of the CBOR list that includes both CIDs.
fn merge_nodes(left: &Cid, right: Option<&Cid>) -> Cid {
    let merkle_node = vec![Some(*left), right.cloned()];
    let x: Vec<u8> = serde_ipld_dagcbor::to_vec(&merkle_node).unwrap();
    // todo: push the merge tree node.
    Cid::new_v1(
        <DagCborCodec as Codec<Ipld>>::CODE,
        Code::Sha2_256.digest(&x),
    )
}

#[derive(Debug)]
struct RootCount {
    root: Cid,
    count: u64,
}

/// Fetch unanchored CIDs from the Anchor Request Store and builds a Merkle tree from them.
async fn build_tree<I>(cids: I) -> Result<RootCount>
where
    I: IntoIterator<Item = Cid>,
{
    let mut peaks: [Option<Cid>; 64] = [None; 64];
    let mut count: u64 = 0;
    for cid in cids.into_iter() {
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
        let mut new_node: Cid = cid;
        let mut place_value = 0;
        while place_value < peaks.len() {
            match peaks[place_value].take() {
                Some(old_node) => {
                    let merged_node = merge_nodes(&old_node, Some(&new_node));
                    new_node = merged_node;
                    place_value += 1;
                }
                None => {
                    peaks[place_value] = Some(new_node);
                    break;
                }
            }
        }
    }
    let mut right: Option<Cid> = None;
    for left in peaks.iter().flatten() {
        right = Some(merge_nodes(left, right.as_ref()));
    }
    match right {
        Some(right) => Ok(RootCount {
            root: right,
            count: count,
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
    Ok(path.strip_suffix("/").expect("path ends with /").to_string())
}

#[derive(Debug)]
struct TimeEvent {
    pub id: Cid,
    pub prev: Cid,
    pub proof: Cid,
    pub path: String,
}

fn build_time_event(id: Cid, prev: Cid, proof: Cid, index: u64, count:u64) -> Result<TimeEvent> {
    Ok(TimeEvent{
        id,
        prev,
        proof,
        path: index_to_path(index, count)?
    })
}

struct AnchorRequest {
    pub id: Cid,
    pub prev: Cid,
}

fn build_time_events<I>(anchor_requests: I, proof: Cid, count:u64) -> Result<()>
where
    I: IntoIterator<Item = AnchorRequest>,
{
    for (index, anchor_request) in anchor_requests.into_iter().enumerate() {
        let time_event = TimeEvent{
            id: anchor_request.id,
            prev: anchor_request.prev,
            proof: proof,
            path: index_to_path(index.try_into().unwrap(), count)?
        };
        // todo push time_event 
    }
    Ok(())
}

/// Tests to ensure that the merge function is working as expected.
#[cfg(test)]
mod tests {
    use super::*;
    use ceramic_store::{Error, Migrations, SqlitePool};
    use cid::CidGeneric;
    use expect_test::expect;
    use std::fmt::{self, Debug};

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
        let expected = "bafyreiabkbt7ctfodcqv7lv4vti5a7a4hxhsujbdgtdrjdpe2i27e4yiyy"
            .try_into()
            .unwrap();
        assert_eq!(merge_nodes(&left, right.as_ref()), expected);
    }

    // /// Test to create an in-memory AnchorRequestStore and ensure that it can be used to store and retrieve anchor requests.
    // #[tokio::test]
    // async fn test_anchor_request_store() {
    //     let pool = SqlitePool::connect(
    //         "/Users/mz/Documents/3Box/GitHub/rust-ceramic/anchor_db",
    //         Migrations::Apply,
    //     )
    //     .await
    //     .unwrap();
    //     let store = SqliteEventStore::new(pool.clone()).await.unwrap();
    //     // Insert 10 random CIDs into the anchor request store
    //     // let mut cids = Vec::new();
    //     // {
    //     //     let mut tx = pool.writer().begin().await.map_err(Error::from).unwrap();
    //     //     for _ in 0..10 {
    //     //         let cid = random_cid();
    //     //         println!("{:?}", cid);
    //     //         store.put_anchor_request(&cid, &mut tx).await.unwrap();
    //     //         cids.push(cid);
    //     //     }
    //     //     tx.commit().await.map_err(Error::from).unwrap();
    //     // }
    //     let cids = [
    //         "baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq",
    //         "baeabeib5kixfpd3hy26p3fd6d6chc3m46rb7iue7pob57uzgq5rx4p7qa4",
    //         "baeabeifpl5wb2fthawbu5bxt2b53ttlesvwzltrfednkyhzny2qbhcbsva",
    //         "baeabeicvef53ez5xe7vjxiy7d6ks5yidzfljm74le5vjjz6g54leoqhywq",
    //         "baeabeiemn6xvw72ijvroevm3oru2l67q52ypzys3kk24b4jgjbyqjr4ane",
    //         "baeabeiffomshvohlqhp5xqkipnsmxwz7iqpnzwx4ezdiz6myi3tjlb4wuq",
    //         "baeabeidzeapsosp2skecx4kp3eseow2moeeakvan7y4fsc4iniuozhmsaq",
    //         "baeabeicpotw4izgzuhapyr54dudj67l3axcblfna65fseaskvadz45hmca",
    //         "baeabeibdujbnmrcqj2yiozvfizowdth2sjuorzg23gk5ce4k5tdxf2axzu",
    //         "baeabeihmjgwf7omjvat6mp4mhqlwpr4ax54twc7nznarc2o3cgyya6hj74",
    //     ]
    //     .iter()
    //     .map(|&cid| cid.try_into().unwrap())
    //     .collect::<Vec<Cid>>();

    //     {
    //         let mut tx = pool.writer().begin().await.map_err(Error::from).unwrap();
    //         for cid in cids.iter() {
    //             store.put_anchor_request(cid, &mut tx).await.unwrap();
    //         }
    //         tx.commit().await.map_err(Error::from).unwrap();
    //     }

    //     assert_eq!(
    //         build_tree(&store).await.unwrap(),
    //         Some(
    //             "bafyreiaeimcmwpxxz7ds4wwbmqeobn2fop7dcyw3fufs6egoy33mhckaeu"
    //                 .try_into()
    //                 .unwrap()
    //         )
    //     );
    // }

    fn random_cid() -> Cid {
        let mut data = [0u8; 8];
        rand::Rng::fill(&mut ::rand::thread_rng(), &mut data);
        let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
        Cid::new_v1(0x00, hash)
    }

    fn int128_cid(i: i128) -> Cid {
        let data = i.to_be_bytes();
        let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
        Cid::new_v1(0x00, hash)
    }

    #[tokio::test]
    async fn test_hash_root() {
        let cids: Vec<Cid> = (1..1_000_000_i128).map(int128_cid).into_iter().collect();
        let result = build_tree(cids).await;
        expect!["Ok(RootCount { root: Cid(bafyreidq247kfkizr3k6wlvx43lt7gro2dno7vzqepmnqt26agri4opzqu), count: 999999 })"]
        .assert_eq(&format!("{:?}", &result));
    }

    #[tokio::test]
    async fn test_time_event() {
        let id = Cid::try_from("baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq").unwrap();
        let prev = Cid::try_from("baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq").unwrap();
        let proof = Cid::try_from("bafyreidq247kfkizr3k6wlvx43lt7gro2dno7vzqepmnqt26agri4opzqu").unwrap();
        let index = 500_000;
        let count = 999_999;
        let time_event = build_time_event(id, prev, proof, index, count);
        expect![[r#"Ok(TimeEvent { id: Cid(baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq), prev: Cid(baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq), proof: Cid(bafyreidq247kfkizr3k6wlvx43lt7gro2dno7vzqepmnqt26agri4opzqu), path: "0/1/1/1/1/0/1/0/0/0/0/1/0/0/1/0/0/0/0/0" })"#]]
        .assert_eq(&format!("{:?}", time_event));
    }

    #[tokio::test]
    async fn test_time_events() {
        let count = 1_000_000_i128;
        let anchor_requests: Vec<AnchorRequest> = (0..count)
            .map(|n| AnchorRequest {
                id: int128_cid(n),
                prev: int128_cid(n),
            })
            .into_iter()
            .collect();
        let proof = Cid::try_from("bafyreidq247kfkizr3k6wlvx43lt7gro2dno7vzqepmnqt26agri4opzqu").unwrap();

        build_time_events(anchor_requests, proof, count.try_into().unwrap());
    }

    #[tokio::test]
    async fn test_index_to_path(){
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
