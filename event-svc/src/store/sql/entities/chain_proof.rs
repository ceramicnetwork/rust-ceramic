use crate::eth_rpc;

#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::FromRow)]
/// A chain inclusion proof stored in the database
/// Unique by chain_id and transaction_hash
pub struct ChainProof {
    /// The chain ID of the proof
    pub chain_id: String,
    /// The transaction hash of the proof
    pub transaction_hash: String,
    /// The transaction input of the proof (i.e. the CID of the root of the Merkle tree being anchored)
    pub transaction_input: String,
    /// The block hash of the proof
    pub block_hash: String,
    /// The timestamp of the proof
    pub timestamp: i64,
}

impl From<eth_rpc::ChainInclusionProof> for ChainProof {
    fn from(value: eth_rpc::ChainInclusionProof) -> Self {
        Self {
            chain_id: value.metadata.chain_id.to_string(),
            block_hash: value.block_hash,
            transaction_hash: value.metadata.tx_hash,
            transaction_input: value.metadata.tx_input,
            timestamp: value
                .timestamp
                .as_unix_ts()
                .try_into()
                .expect("chain proof timestamp overflow"),
        }
    }
}

impl From<ceramic_anchor_service::ChainInclusionData> for ChainProof {
    fn from(value: ceramic_anchor_service::ChainInclusionData) -> Self {
        Self {
            chain_id: value.chain_id,
            transaction_hash: value.transaction_hash,
            transaction_input: value.transaction_input,
            block_hash: value.block_hash,
            timestamp: value
                .timestamp
                .try_into()
                .expect("chain proof timestamp overflow"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ceramic_anchor_service::ChainInclusionData;

    #[test]
    fn test_chain_proof_from_chain_inclusion_data() {
        let data = ChainInclusionData {
            chain_id: "eip155:100".to_string(),
            transaction_hash: "0xabc123def456abc123def456abc123def456abc123def456abc123def456abc1"
                .to_string(),
            transaction_input: "0x97ad09eb1234567890abcdef1234567890abcdef1234567890abcdef12345678"
                .to_string(),
            block_hash: "0xdef456abc123def456abc123def456abc123def456abc123def456abc123def4"
                .to_string(),
            timestamp: 1704067200,
        };

        let proof: ChainProof = data.into();

        assert_eq!(proof.chain_id, "eip155:100");
        assert_eq!(
            proof.transaction_hash,
            "0xabc123def456abc123def456abc123def456abc123def456abc123def456abc1"
        );
        assert_eq!(
            proof.transaction_input,
            "0x97ad09eb1234567890abcdef1234567890abcdef1234567890abcdef12345678"
        );
        assert_eq!(
            proof.block_hash,
            "0xdef456abc123def456abc123def456abc123def456abc123def456abc123def4"
        );
        assert_eq!(proof.timestamp, 1704067200);
    }

    #[test]
    fn test_chain_proof_from_chain_inclusion_data_preserves_chain_id_format() {
        for chain_id in ["eip155:1", "eip155:100", "eip155:137", "eip155:42161"] {
            let data = ChainInclusionData {
                chain_id: chain_id.to_string(),
                transaction_hash: "0x".to_string() + &"a".repeat(64),
                transaction_input: "0x".to_string() + &"b".repeat(72),
                block_hash: "0x".to_string() + &"c".repeat(64),
                timestamp: 1704067200,
            };

            let proof: ChainProof = data.into();
            assert_eq!(proof.chain_id, chain_id);
        }
    }

    #[test]
    fn test_chain_proof_timestamp_zero() {
        let data = ChainInclusionData {
            chain_id: "eip155:1".to_string(),
            transaction_hash: "0x".to_string() + &"a".repeat(64),
            transaction_input: "0x".to_string() + &"b".repeat(72),
            block_hash: "0x".to_string() + &"c".repeat(64),
            timestamp: 0,
        };

        let proof: ChainProof = data.into();
        assert_eq!(proof.timestamp, 0);
    }
}
