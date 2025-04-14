use std::str::FromStr as _;

use alloy::primitives::TxHash;
use ceramic_core::Cid;

/// The ethereum RPC provider module
pub mod eth_rpc;

/// Get the expected transaction hash for a given root CID (this is v1 proof type)
pub(crate) fn tx_hash_try_from_cid(cid: Cid) -> anyhow::Result<TxHash> {
    Ok(TxHash::from_str(&hex::encode(cid.hash().digest()))?)
}
