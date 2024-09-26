use serde::{Deserialize, Serialize};
use ssi::caip2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
/// A blockchain transaction
pub struct ChainTransaction {
    /// Transaction hash
    pub hash: String,
    /// Transaction contract input
    pub input: String,
    /// Information about the block in which this transaction was mined.
    /// If None, the transaction exists but has not been mined yet.
    pub block: Option<ChainBlock>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
/// A blockchain block
pub struct ChainBlock {
    pub hash: String,
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
