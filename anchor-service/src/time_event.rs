use anyhow::{anyhow, Result};
use cid::Cid;

use crate::anchor_batch::AnchorRequest;
use ceramic_anchor_tx::DetachedTimeEvent;
use ceramic_event::unvalidated::RawTimeEvent;

pub fn build_time_events(
    anchor_requests: &[AnchorRequest],
    detached_time_event: &DetachedTimeEvent,
    count: u64,
) -> Result<Vec<RawTimeEvent>> {
    anchor_requests
        .iter()
        .enumerate()
        .map(|(index, anchor_request)| {
            build_time_event(
                &anchor_request.id,
                &anchor_request.prev,
                &detached_time_event.proof,
                detached_time_event.path.as_str(),
                index.try_into()?,
                count,
            )
        })
        .collect()
}

pub fn build_time_event(
    id: &Cid,
    prev: &Cid,
    proof_cid: &Cid,
    remote_path: &str,
    index: u64,
    count: u64,
) -> Result<RawTimeEvent> {
    let local_path = index_to_path(index, count)?;
    Ok(RawTimeEvent {
        id: *id,
        prev: *prev,
        proof: *proof_cid,
        path: format!("{}/{}", remote_path, local_path)
            .trim_matches('/')
            .to_owned(),
    })
}

pub fn index_to_path(index: u64, count: u64) -> Result<String> {
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
    if index >= count {
        return Err(anyhow!("index({}) >= count({})", index, count));
    }

    let mut path = String::new();
    let mut length = count;
    let mut index = index;

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

    Ok(path.trim_end_matches('/').to_string())
}

/// Tests to ensure that the merge function is working as expected.
#[cfg(test)]
mod tests {
    use super::*;
    use expect_test::expect;
    use multihash_codetable::{Code, MultihashDigest};

    fn intu64_cid(i: u64) -> Cid {
        let data = i.to_be_bytes();
        let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
        Cid::new_v1(0x00, hash)
    }

    fn mock_anchor_requests() -> (u64, Vec<AnchorRequest>) {
        let count = 20;
        (
            count,
            (0..count)
                .map(|n| AnchorRequest {
                    id: intu64_cid(n),
                    prev: intu64_cid(n),
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
        let time_event = build_time_event(&id, &prev, &proof, "", index, count);
        expect![[r#"Ok(RawTimeEvent { id: "baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq", prev: "baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq", proof: "bafyreidq247kfkizr3k6wlvx43lt7gro2dno7vzqepmnqt26agri4opzqu", path: "0/1/1/1/1/0/1/0/0/0/0/1/0/0/1/0/0/0/0/0" })"#]]
            .assert_eq(&format!("{:?}", time_event));
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
