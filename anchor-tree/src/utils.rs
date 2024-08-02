use anyhow::anyhow;
use anyhow::Result;
use cid::Cid;

use ceramic_core::DagCborIpfsBlock;

/// Accepts the CIDs of two blocks and returns the CID of the CBOR list that includes both CIDs.
fn merge_nodes(left: &Cid, right: Option<&Cid>) -> Result<DagCborIpfsBlock> {
    let merkle_node = vec![Some(*left), right.cloned()];
    Ok(serde_ipld_dagcbor::to_vec(&merkle_node)?.into())
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

#[cfg(test)]
mod tests {
    use super::*;
    use expect_test::expect;

    #[tokio::test]
    async fn test_index_to_path() {
        let mmr = MerkleMountainRange::new();
        // '1/'  10 > 8, 14
        // '1/0/' 2 > 4, 6
        // '1/0/' 0b10
        // '1/0/1/0'
        expect![[r#"
            Ok(
                "1/0/1/0",
            )
        "#]]
        .assert_debug_eq(mmr.index_to_path(10, 14));

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
