use anyhow::{anyhow, Result};

use ceramic_event::unvalidated::RawTimeEvent;

use crate::{
    anchor::{AnchorRequest, RawTimeEvents},
    DetachedTimeEvent,
};

pub fn build_time_events(
    anchor_requests: &[AnchorRequest],
    detached_time_event: &DetachedTimeEvent,
    count: u64,
) -> Result<RawTimeEvents> {
    let events = anchor_requests
        .iter()
        .enumerate()
        .map(|(index, anchor_request)| {
            let local_path = index_to_path(index.try_into()?, count)?;
            let remote_path = detached_time_event.path.as_str();
            Ok((
                anchor_request.clone(),
                RawTimeEvent::new(
                    anchor_request.id,
                    anchor_request.prev,
                    detached_time_event.proof,
                    format!("{}/{}", remote_path, local_path)
                        .trim_matches('/')
                        .to_owned(),
                ),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(RawTimeEvents { events })
}

pub fn index_to_path(index: u64, count: u64) -> Result<String> {
    // we want to find the path to the index in a tree length.
    // first find the sub-tree then append the path in the sub-tree
    //
    // e.g. index_to_path(index: 10, count: 14) => Ok("1/0/1/0")
    //
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

    let mut path: Vec<_> = Vec::new();
    let mut length = count;
    let mut index = index;

    // The purpose of this while loop is to figure out which subtree the index is in.
    let mut top_power_of_2 = Default::default();
    while length > 0 {
        top_power_of_2 = 1 << (63 - length.leading_zeros());
        if top_power_of_2 == length {
            break;
        }
        if index < top_power_of_2 {
            // the index is in the left tree
            path.push('0');
            break;
        } else {
            // the index is in the right tree
            path.push('1');
            length -= top_power_of_2;
            index -= top_power_of_2;
        }
    }

    // The purpose of this for loop is to figure out the specified index's location in the subtree.
    // Adding the top power of two forces the binary of the number to always start with 1. We can then subtract the
    // top power of two to strip the leading 1. This leaves us with all the leading zeros.
    path.append(
        format!("{:b}", index + top_power_of_2)[1..]
            .chars()
            .collect::<Vec<_>>()
            .as_mut(),
    );
    Ok(path
        .iter()
        .map(|c| c.to_string())
        .collect::<Vec<_>>()
        .join("/"))
}

/// Tests to ensure that the merge function is working as expected.
#[cfg(test)]
mod tests {
    use super::*;
    use ceramic_core::{Cid, EventId, Network, SerializeExt};
    use expect_test::expect;

    #[tokio::test]
    async fn test_time_event() {
        let id =
            Cid::try_from("baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq").unwrap();
        let prev =
            Cid::try_from("baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq").unwrap();
        let proof =
            Cid::try_from("bafyreidq247kfkizr3k6wlvx43lt7gro2dno7vzqepmnqt26agri4opzqu").unwrap();
        let detached_time_event = DetachedTimeEvent {
            path: "".to_owned(),
            proof,
        };
        let order_key = EventId::new(
            &Network::Mainnet,
            "model",
            &multibase::decode("kh4q0ozorrgaq2mezktnrmdwleo1d")
                .unwrap()
                .1,
            "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9",
            &id,
            &prev,
        );

        let anchor_requests = vec![AnchorRequest {
            id,
            prev,
            event_id: order_key,
            resume_token: 0,
        }];
        let time_event = build_time_events(&anchor_requests, &detached_time_event, 1);
        expect![[r#"{"events":[[{"id":{"/":"baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq"},"prev":{"/":"baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq"},"event_id":{"/":{"bytes":"zgEFALolB21zAkHnRcx8By/3KeodWhecAQASILT8B/C/HM+qN30lL5Rt0RZy4TcU9J/bE9/Wod4dWhec"}},"resume_token":0},{"id":{"/":"baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq"},"prev":{"/":"baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq"},"proof":{"/":"bafyreidq247kfkizr3k6wlvx43lt7gro2dno7vzqepmnqt26agri4opzqu"},"path":""}]]}"#]]
            .assert_eq(core::str::from_utf8(time_event.unwrap().to_json().unwrap().as_bytes()).unwrap());
    }

    #[tokio::test]
    async fn test_index_to_path() {
        // index: 0, count: 1
        expect![""].assert_eq(&index_to_path(0, 1).unwrap());

        // index: 0 - 1, count: 2
        expect!["0"].assert_eq(&index_to_path(0, 2).unwrap());
        expect!["1"].assert_eq(&index_to_path(1, 2).unwrap());

        // index: 0 - 2, count: 3
        expect!["0/0"].assert_eq(&index_to_path(0, 3).unwrap());
        expect!["0/1"].assert_eq(&index_to_path(1, 3).unwrap());
        expect!["1"].assert_eq(&index_to_path(2, 3).unwrap());

        // index 0 - 3, count: 4
        expect!["0/0"].assert_eq(&index_to_path(0, 4).unwrap());
        expect!["0/1"].assert_eq(&index_to_path(1, 4).unwrap());
        expect!["1/0"].assert_eq(&index_to_path(2, 4).unwrap());
        expect!["1/1"].assert_eq(&index_to_path(3, 4).unwrap());

        // '1/'  10 > 8, 14
        // '1/0/' 2 > 4, 6
        // '1/0/' 0b10
        // '1/0/1/0'
        expect!["1/0/1/0"].assert_eq(&index_to_path(10, 14).unwrap());

        // '0/' 500_000 < 524288, 1_000_000
        // '0/' 0b1111010000100100000
        // '0/1/1/1/1/0/1/0/0/0/0/1/0/0/1/0/0/0/0/0/'
        expect!["0/1/1/1/1/0/1/0/0/0/0/1/0/0/1/0/0/0/0/0"]
            .assert_eq(&index_to_path(500_000, 1_000_000).unwrap());

        // '1/'        999_999 > 524288,  1_000_000
        // '1/1/'      475_711 > 262_144, 475_712
        // '1/1/1/'    213_567 > 131072,  213_568
        // '1/1/1/1/'   82_495 > 65_536,  82_496
        // '1/1/1/1/1/' 16_959 > 16_384,  16_960
        // '1/1/1/1/1/1/'  575 > 512,     576
        // '1/1/1/1/1/1/0/' 63 !> 64,     64
        // '1/1/1/1/1/1/0/' 0b111111
        // '1/1/1/1/1/1/0/1/1/1/1/1/1/'
        expect!["1/1/1/1/1/1/1/1/1/1/1/1"].assert_eq(&index_to_path(999_999, 1_000_000).unwrap());
        expect!["0/0/0/0/0/0/0/0/0/0/0/0/0/0/0/0/0/0/0/0"]
            .assert_eq(&index_to_path(0, 1_000_000).unwrap());
        expect!["0/0/0/0/0/0/0/0/0/0/0/0/0/0/0/0/0/0/0/1"]
            .assert_eq(&index_to_path(1, 1_000_000).unwrap());
    }
}
