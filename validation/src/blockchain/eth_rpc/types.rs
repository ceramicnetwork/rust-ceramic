use std::str::FromStr;

use alloy::transports::{RpcError, TransportErrorKind};
use anyhow::anyhow;
use ceramic_core::Cid;

pub use alloy::primitives::{BlockHash, TxHash};

use crate::blockchain::Error;

impl From<RpcError<TransportErrorKind>> for Error {
    fn from(value: RpcError<TransportErrorKind>) -> Self {
        match value {
            alloy::transports::RpcError::Transport(err) => {
                if err.is_retry_err() {
                    Self::Transient(anyhow!(err))
                } else {
                    Self::Application(anyhow!(err))
                }
            }
            err => Self::Application(anyhow!(err)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Input for an ethereum transaction proof for time events
pub struct EthTxProofInput {
    /// The transaction hash as a CID from the time event
    pub tx_hash: Cid,
    /// The time event proof type
    pub tx_type: EthProofType,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// The format of the ethereum time event proof
pub enum EthProofType {
    /// raw
    V0,
    /// f(bytes32)
    V1,
}

impl std::fmt::Display for EthProofType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EthProofType::V0 => write!(f, "{}", V0_PROOF_TYPE),
            EthProofType::V1 => write!(f, "{}", V1_PROOF_TYPE),
        }
    }
}

pub(crate) const V0_PROOF_TYPE: &str = "raw";
pub(crate) const V1_PROOF_TYPE: &str = "f(bytes32)"; // See: https://namespaces.chainagnostic.org/eip155/caip168

impl FromStr for EthProofType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            V0_PROOF_TYPE => Ok(Self::V0),
            V1_PROOF_TYPE => Ok(Self::V1),
            v => anyhow::bail!("Unknown proof type: {}", v),
        }
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use alloy::primitives::Bytes;

    use super::*;

    #[test]
    /// we currently use a string in the public interface of [`ChainTransaction`] for the `input` and expect that they are hex encoded 0x prefixed
    fn bytes_display_as_hex() {
        let hex_encoded_prefix =
            "0x97ad09eb1a315930c36a6252c157a565b7fca969230faa8cd695172538138ac579488f65";
        let hex_encoded =
            "97ad09eb1a315930c36a6252c157a565b7fca969230faa8cd695172538138ac579488f65";
        let bytes_prefix = hex_encoded_prefix.parse::<Bytes>().unwrap();
        let bytes = hex_encoded.parse::<Bytes>().unwrap();

        assert_eq!(hex_encoded_prefix, bytes_prefix.to_string());
        assert_eq!(hex_encoded_prefix, bytes.to_string());
    }

    #[test]
    // these tests are basically duplicates of the alloy Bytes tests, but if we swap out our type, we need to support both formats
    fn can_parse_hex_strings_as_tx_hash() {
        TxHash::from_str("0x1bfe594e9f2e7b32a39fe50d24c2fd3fb15255bde5bace0140c1c861c9cdb091")
            .expect("should parse with 0x prefix");

        TxHash::from_str("0cc4c353d087574ee4bf721928c5ebf13e680dc67f441d98cb0934d6eef50b12")
            .expect("should parse without 0x prefix");
    }

    #[test]
    fn can_parse_hex_strings_as_block_hash() {
        BlockHash::from_str("783cd5a6febe13d08ac0d59fa7e666483d5e476542b29688a6f0bec3d15febd4")
            .expect("should parse without 0x prefix");

        BlockHash::from_str("0x783cd5a6febe13d08ac0d59fa7e666483d5e476542b29688a6f0bec3d15febd4")
            .expect("should parse with 0x prefix");
    }
}
