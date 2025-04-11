use crate::eth_rpc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::FromRow)]
/// A chain inclusion proof stored in the database
/// Unique by chain_id and transaction_hash
pub struct ChainProof {
    /// The chain ID of the proof
    pub chain_id: String,
    /// The transaction hash of the proof
    pub transaction_hash: String,
    /// The transaction input of the proof
    pub transaction_input: String,
    /// The block hash of the proof
    pub block_hash: String,
    /// The timestamp of the proof
    pub timestamp: i64,
}

impl From<eth_rpc::ChainInclusionProof> for ChainProof {
    fn from(value: eth_rpc::ChainInclusionProof) -> Self {
        Self {
            chain_id: value.meta_data.chain_id.to_string(),
            block_hash: value.block_hash,
            transaction_hash: value.meta_data.tx_hash,
            transaction_input: value.meta_data.tx_input,
            timestamp: value
                .timestamp
                .as_unix_ts()
                .try_into()
                .expect("chain proof timestamp overflow"),
        }
    }
}
