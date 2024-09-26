use alloy::transports::{RpcError, TransportErrorKind};
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use ssi::caip2;

pub use alloy::primitives::{BlockHash, TxHash};

#[derive(Debug)]
pub enum Error {
    /// Invalid input with reason for rejection
    InvalidArgument(String),
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
/// A blockchain transaction
pub struct ChainTransaction {
    /// Transaction hash. While a 32 byte hash is not universal, it is for bitcoin and the EVM,
    /// so we use that for now to make things easier. We could use a String encoded representation,
    /// but this covers our current state and lets the caller decide how to encode the bytes for
    ///  their needs (e.g. persistence), and avoids any changes to Display (e.g. 0x prefixed) breaking things.
    pub hash: TxHash,
    /// 0x prefixed hex encoded string representation of transaction contract input.
    pub input: String,
    /// Information about the block in which this transaction was mined.
    /// If None, the transaction exists but has not been mined yet.
    pub block: Option<ChainBlock>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
/// A blockchain block
pub struct ChainBlock {
    /// The 32 byte block hash
    pub hash: BlockHash,
    /// The block number
    pub number: u64,
    /// The unix epoch timestamp of the block
    pub timestamp: u64,
}

#[async_trait::async_trait]
/// Ethereum RPC provider methods. This is a higher level type than the actual RPC calls neeed and
/// may wrap a multiple calls into a logical behavior of getting necessary information.
pub trait EthRpc {
    /// Get the CAIP2 chain ID supported by this RPC provider
    fn chain_id(&self) -> &caip2::ChainId;
    
    /// The RPC url used by the provider
    fn url(&self) -> String;

    /// Get the block chain transaction if it exists with the block timestamp information
    async fn get_block_timestamp(&self, tx_hash: &str) -> Result<Option<ChainTransaction>, Error>;
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
