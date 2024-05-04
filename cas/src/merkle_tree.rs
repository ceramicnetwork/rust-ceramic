use anyhow::Result;

use cid::Cid;
use ipld_core::{codec::Codec, ipld::Ipld};
use multihash_codetable::{Code, MultihashDigest};
use serde_ipld_dagcbor::codec::DagCborCodec;

use ceramic_store::{SqliteEventStore, SqlitePool};

/// Accepts the CIDs of two blocks and returns the CID of the CBOR list that includes both CIDs.
fn merge_nodes(left: &Cid, right: Option<&Cid>) -> Cid {
    let merkle_node = vec![Some(*left), right.cloned()];
    let x: Vec<u8> = serde_ipld_dagcbor::to_vec(&merkle_node).unwrap();
    Cid::new_v1(
        <DagCborCodec as Codec<Ipld>>::CODE,
        Code::Sha2_256.digest(&x),
    )
}

/// Fetch unanchored CIDs from the Anchor Request Store and builds a Merkle tree from them.
async fn build_tree(event_store: &SqliteEventStore) -> Result<Option<Cid>> {
    let cids = event_store.scan_anchor_requests(100).await?;

    let mut peaks: [Option<Cid>; 64] = [None; 64];
    for cid in &cids {
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
        let mut new_node: Cid = *cid;
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
    Ok(right)
}

/// Tests to ensure that the merge function is working as expected.
#[cfg(test)]
mod tests {
    use super::*;
    use ceramic_store::{Error, Migrations, SqliteEventStore, SqlitePool};

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

    /// Test to create an in-memory AnchorRequestStore and ensure that it can be used to store and retrieve anchor requests.
    #[tokio::test]
    async fn test_anchor_request_store() {
        let pool = SqlitePool::connect(
            "/Users/mz/Documents/3Box/GitHub/rust-ceramic/anchor_db",
            Migrations::Apply,
        )
        .await
        .unwrap();
        let store = SqliteEventStore::new(pool.clone()).await.unwrap();
        // Insert 10 random CIDs into the anchor request store
        // let mut cids = Vec::new();
        // {
        //     let mut tx = pool.writer().begin().await.map_err(Error::from).unwrap();
        //     for _ in 0..10 {
        //         let cid = random_cid();
        //         println!("{:?}", cid);
        //         store.put_anchor_request(&cid, &mut tx).await.unwrap();
        //         cids.push(cid);
        //     }
        //     tx.commit().await.map_err(Error::from).unwrap();
        // }
        let cids = [
            "baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq",
            "baeabeib5kixfpd3hy26p3fd6d6chc3m46rb7iue7pob57uzgq5rx4p7qa4",
            "baeabeifpl5wb2fthawbu5bxt2b53ttlesvwzltrfednkyhzny2qbhcbsva",
            "baeabeicvef53ez5xe7vjxiy7d6ks5yidzfljm74le5vjjz6g54leoqhywq",
            "baeabeiemn6xvw72ijvroevm3oru2l67q52ypzys3kk24b4jgjbyqjr4ane",
            "baeabeiffomshvohlqhp5xqkipnsmxwz7iqpnzwx4ezdiz6myi3tjlb4wuq",
            "baeabeidzeapsosp2skecx4kp3eseow2moeeakvan7y4fsc4iniuozhmsaq",
            "baeabeicpotw4izgzuhapyr54dudj67l3axcblfna65fseaskvadz45hmca",
            "baeabeibdujbnmrcqj2yiozvfizowdth2sjuorzg23gk5ce4k5tdxf2axzu",
            "baeabeihmjgwf7omjvat6mp4mhqlwpr4ax54twc7nznarc2o3cgyya6hj74",
        ]
        .iter()
        .map(|&cid| cid.try_into().unwrap())
        .collect::<Vec<Cid>>();

        {
            let mut tx = pool.writer().begin().await.map_err(Error::from).unwrap();
            for cid in cids.iter() {
                store.put_anchor_request(cid, &mut tx).await.unwrap();
            }
            tx.commit().await.map_err(Error::from).unwrap();
        }

        assert_eq!(
            build_tree(&store).await.unwrap(),
            Some(
                "bafyreiaeimcmwpxxz7ds4wwbmqeobn2fop7dcyw3fufs6egoy33mhckaeu"
                    .try_into()
                    .unwrap()
            )
        );
    }

    fn random_cid() -> Cid {
        let mut data = [0u8; 8];
        rand::Rng::fill(&mut ::rand::thread_rng(), &mut data);
        let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
        Cid::new_v1(0x00, hash)
    }
}
