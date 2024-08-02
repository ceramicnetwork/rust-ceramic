use cid::Cid;
use serde::{Deserialize, Serialize};

// AnchorRequest request for a Time Event
pub struct AnchorRequest {
    pub id: Cid,   // The CID of the Stream
    pub prev: Cid, // The CID of the Event to be anchored
}

// pub fn build_time_events<'a, I>(
//     anchor_requests: I,
//     proof: Cid,
//     path_prefix: Option<String>,
//     merkle_tree: &MerkleTree,
// ) -> Result<Vec<DagCborIpfsBlock>>
// where
//     I: Iterator<Item = &'a AnchorRequest>,
// {
//     let mut time_events: Vec<DagCborIpfsBlock> = vec![];
//     for (index, anchor_request) in anchor_requests.into_iter().enumerate() {
//         let time_event = build_time_event(
//             anchor_request.id,
//             anchor_request.prev,
//             proof,
//             path_prefix.clone(),
//             index as u64,
//             merkle_tree,
//         )?;
//         time_events.push(serde_ipld_dagcbor::to_vec(&time_event).unwrap().into());
//     }
//     Ok(time_events)
// }

/// TimeEvent is a single TimeEvent anchored to the chain.
#[derive(Serialize, Deserialize, Debug)]
pub struct TimeEvent {
    pub id: Cid,
    pub prev: Cid,
    pub proof: Cid,
    pub path: String,
}

/// Tests to ensure that the merge function is working as expected.
#[cfg(test)]
mod tests {
    use super::*;
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
                    idx: n,
                    id: intu64_cid(n),
                    prev: intu64_cid(n),
                })
                .collect(),
        )
    }

    // #[tokio::test]
    // async fn test_time_event() {
    //     let id =
    //         Cid::try_from("baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq").unwrap();
    //     let prev =
    //         Cid::try_from("baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq").unwrap();
    //     let proof =
    //         Cid::try_from("bafyreidq247kfkizr3k6wlvx43lt7gro2dno7vzqepmnqt26agri4opzqu").unwrap();
    //     let index = 500_000;
    //     let count = 999_999;
    //     let time_event = build_time_event(id, prev, proof, Some("".to_owned()), index, count);
    //     expect![[r#"Ok(TimeEvent { id: Cid(baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq), prev: Cid(baeabeifu7qd7bpy4z6vdo7jff6kg3uiwolqtofhut7nrhx6wuhpb2wqxtq), proof: Cid(bafyreidq247kfkizr3k6wlvx43lt7gro2dno7vzqepmnqt26agri4opzqu), path: "/0/1/1/1/1/0/1/0/0/0/0/1/0/0/1/0/0/0/0/0" })"#]]
    //     .assert_eq(&format!("{:?}", time_event));
    // }

    // #[tokio::test]
    // async fn test_time_events() {
    //     let (count, anchor_requests) = mock_anchor_requests();
    //     let proof =
    //         Cid::try_from("bafyreicv5owrmctiwt3qhfpto6bs4ld3bxcqwxxjttpfvvzcqljr7bfmum").unwrap();
    //
    //     let result = build_time_events(
    //         anchor_requests.iter(),
    //         proof,
    //         Some("".to_owned()),
    //         count.try_into().unwrap(),
    //     );
    //     expect![[r#"
    //         Ok(
    //             (),
    //         )
    //     "#]]
    //     .assert_debug_eq(&result);
    // }
}
