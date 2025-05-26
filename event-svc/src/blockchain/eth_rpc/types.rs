use std::str::FromStr;

use alloy::transports::{RpcError, TransportErrorKind};
use anyhow::anyhow;
use ceramic_core::Cid;
use ssi::caip2;

pub use alloy::primitives::{BlockHash, TxHash};
use ceramic_event::unvalidated::AnchorProof;

#[derive(Debug)]
/// The error variants expected from an Ethereum RPC request
pub enum Error {
    /// Invalid input with reason for rejection
    InvalidArgument(String),
    /// Transaction hash not found
    TxNotFound {
        /// The chain ID
        chain_id: caip2::ChainId,
        /// The transaction hash we tried to find
        tx_hash: String,
    },
    /// The transaction exists but has not been mined yet
    TxNotMined {
        /// The chain ID
        chain_id: caip2::ChainId,
        /// The transaction hash we found but didn't find a corresponding block
        tx_hash: String,
    },
    /// The block was included on the transaction but could not be found
    BlockNotFound {
        /// The chain ID
        chain_id: caip2::ChainId,
        /// The block hash we tried to find
        block_hash: String,
    },
    /// The proof was invalid for the given reason
    InvalidProof(String),
    /// No chain provider configured for the event, whether that's an error is up to the caller
    NoChainProvider(caip2::ChainId),
    /// This is a transient error related to the transport and may be retried
    Transient(anyhow::Error),
    /// This is a standard application error (e.g. server 500) and should not be retried.
    Application(anyhow::Error),
}

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// A timestamp that is able to provide seconds since the unix epoch
pub struct Timestamp(u64);

impl Timestamp {
    /// Create a timestamp from a unix epoch timestamp
    pub const fn from_unix_ts(ts: u64) -> Self {
        Self(ts)
    }

    /// A unix epoch timestamp
    pub fn as_unix_ts(&self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// A proof of time derived from state on the blockchain
pub struct ChainInclusionProof {
    /// The timestamp the proof was recorded
    pub timestamp: Timestamp,
    /// The block hash in hex form with '0x' prefix
    pub block_hash: String,
    /// The root CID of the proof
    pub root_cid: Cid,
    /// The metadata about the proof and where it's stored
    pub metadata: ChainProofMetadata,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// Metadata about the proof and where it's stored
pub struct ChainProofMetadata {
    /// Chain ID of the proof
    pub chain_id: ssi::caip2::ChainId,
    /// The transaction hash in hex form with '0x' prefix
    pub tx_hash: String,
    /// The transaction input in hex form with '0x' prefix
    pub tx_input: String,
}

#[async_trait::async_trait]
/// Wrapper around blockchain RPC provider that can be used to query blockchain state for the
/// relevant information (such as the timestamp) for the given transaction described by an
/// `AnchorProof`. This is a higher level type than the actual RPC calls need and
/// may wrap a multiple calls into a logical behavior of getting necessary information.
pub trait ChainInclusion {
    /// Get the CAIP2 chain ID supported by this RPC provider
    fn chain_id(&self) -> &caip2::ChainId;

    /// Get the block chain transaction if it exists with the block timestamp information
    async fn get_chain_inclusion_proof(
        &self,
        input: &AnchorProof,
    ) -> Result<ChainInclusionProof, Error>;
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
