use crate::DagCborIpfsBlock;
use anyhow::Result;
use cid::Cid;
use multihash_codetable::{Code, MultihashDigest};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ProofBlock {
    chain_id: String,
    root: Cid,
    tx_hash: Cid,
    tx_type: String,
}

// TODO: Replace with real implementation that anchors to an anchoring service.
// make_proof eventually needs to return the proof block, the path, and the intermediate Merkle tree nodes.
// Returns:
// - The proof block CID.
// - The path to the prev.
// - A vector containing the intermediate Merkle tree nodes.
pub async fn make_proof(root: Cid) -> Result<(Cid, String, Vec<DagCborIpfsBlock>)> {
    let mock_data = b"mock txHash";
    let mock_hash = MultihashDigest::digest(&Code::Sha2_256, mock_data);
    let mock_tx_hash = Cid::new_v1(0x00, mock_hash);
    let mock_proof_block = ProofBlock {
        chain_id: "mock chain id".to_string(),
        root,
        tx_hash: mock_tx_hash,
        tx_type: "mock tx type".to_string(),
    };
    let mock_proof: DagCborIpfsBlock = serde_ipld_dagcbor::to_vec(&mock_proof_block)?.into();
    let mock_path = "".to_owned();
    Ok((mock_proof.cid, mock_path, vec![mock_proof]))
}
