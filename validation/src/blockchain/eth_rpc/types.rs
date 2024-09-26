use serde::{Deserialize, Serialize};
use ssi::caip2;

pub use alloy::primitives::{BlockHash, TxHash};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
/// A blockchain transaction
pub struct ChainTransaction {
    /// Transaction hash. While a 32 byte hash is not universal, it is for bitcoin and the EVM,
    /// so we use that for now to make things easier. We could use a String encoded representation,
    /// but this covers our current state and lets the caller decide how to encode the bytes for
    ///  their needs (e.g. persistence), and avoids any changes to Display (e.g. 0x prefixed) breaking things.
    pub hash: TxHash,
    /// Transaction contract input
    pub input: String,
    /// Information about the block in which this transaction was mined.
    /// If None, the transaction exists but has not been mined yet.
    pub block: Option<ChainBlock>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
/// A blockchain block
pub struct ChainBlock {
    pub hash: BlockHash,
    /// the block number
    pub number: u64,
    /// the unix epoch timestamp
    pub timestamp: u64,
}

#[async_trait::async_trait]
pub trait EthRpc {
    fn chain_id(&self) -> &caip2::ChainId;
    fn url(&self) -> String;
    async fn get_block_timestamp(&self, tx_hash: &str) -> anyhow::Result<Option<ChainTransaction>>;
}
