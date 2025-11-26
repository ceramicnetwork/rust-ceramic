use anyhow::{anyhow, Result};
use ceramic_core::Cid;
use ceramic_event::unvalidated::AnchorProof;
use multihash_codetable::{Code, MultihashDigest};

/// Ethereum transaction codec for IPLD (from multicodec table)
const ETH_TX_CODEC: u64 = 0x93;

/// Builds an AnchorProof from EVM transaction details
pub struct ProofBuilder;

impl ProofBuilder {
    /// Create an AnchorProof from EVM transaction receipt details
    ///
    /// # Arguments
    /// * `chain_id` - The EVM chain ID (e.g., 1 for Ethereum mainnet)
    /// * `tx_hash` - The transaction hash as a hex string (with or without 0x prefix)
    /// * `root_cid` - The root CID that was anchored
    ///
    /// # Returns
    /// An AnchorProof that can be used to create time events
    pub fn build_proof(chain_id: u64, tx_hash: String, root_cid: Cid) -> Result<AnchorProof> {
        let tx_hash_cid = Self::tx_hash_to_cid(&tx_hash)?;
        let chain_id_string = format!("eip155:{}", chain_id);
        let tx_type = "f(bytes32)".to_string();

        Ok(AnchorProof::new(
            chain_id_string,
            root_cid,
            tx_hash_cid,
            tx_type,
        ))
    }

    /// Convert a hex transaction hash to a CID
    ///
    /// Ethereum transaction hashes are already Keccak-256 hashes, so we wrap them
    /// directly using Code::Keccak256 with the eth-tx codec (0x93).
    fn tx_hash_to_cid(tx_hash: &str) -> Result<Cid> {
        let hex_str = tx_hash.strip_prefix("0x").unwrap_or(tx_hash);

        let tx_bytes = hex::decode(hex_str)
            .map_err(|e| anyhow!("Failed to decode transaction hash hex: {}", e))?;

        if tx_bytes.len() != 32 {
            return Err(anyhow!(
                "Invalid transaction hash length: expected 32 bytes, got {}",
                tx_bytes.len()
            ));
        }

        let multihash = Code::Keccak256
            .wrap(&tx_bytes)
            .map_err(|e| anyhow!("Failed to create multihash: {}", e))?;

        Ok(Cid::new_v1(ETH_TX_CODEC, multihash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ceramic_core::SerializeExt;
    use std::str::FromStr;

    #[test]
    fn test_tx_hash_to_cid_uses_keccak256_code() {
        let tx_hash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let cid = ProofBuilder::tx_hash_to_cid(tx_hash).unwrap();

        assert_eq!(cid.version(), cid::Version::V1);
        assert_eq!(cid.codec(), ETH_TX_CODEC);
        assert_eq!(cid.hash().code(), u64::from(Code::Keccak256));
    }

    #[test]
    fn test_tx_hash_to_cid_without_prefix() {
        let tx_hash = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let cid = ProofBuilder::tx_hash_to_cid(tx_hash).unwrap();

        assert_eq!(cid.version(), cid::Version::V1);
        assert_eq!(cid.codec(), ETH_TX_CODEC);
        assert_eq!(cid.hash().code(), u64::from(Code::Keccak256));
    }

    #[test]
    fn test_tx_hash_to_cid_preserves_original_hash() {
        let tx_hash = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let cid = ProofBuilder::tx_hash_to_cid(tx_hash).unwrap();

        let expected_bytes = hex::decode(tx_hash).unwrap();
        assert_eq!(cid.hash().digest(), expected_bytes.as_slice());
    }

    #[test]
    fn test_tx_hash_to_cid_rejects_invalid_length() {
        let short_hash = "0x1234";
        let result = ProofBuilder::tx_hash_to_cid(short_hash);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expected 32 bytes"));
    }

    #[test]
    fn test_build_proof_parameter_order() {
        let chain_id = 1;
        let tx_hash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let root_cid =
            Cid::from_str("bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54").unwrap();

        let proof = ProofBuilder::build_proof(chain_id, tx_hash.to_string(), root_cid).unwrap();

        assert_eq!(proof.chain_id(), "eip155:1");
        assert_eq!(proof.root(), root_cid);
        assert_eq!(proof.tx_hash().codec(), ETH_TX_CODEC);
        assert_eq!(proof.tx_type(), "f(bytes32)");

        let _proof_cid = proof.to_cid().unwrap();
    }

    #[test]
    fn test_build_proof_different_chains() {
        let root_cid =
            Cid::from_str("bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54").unwrap();
        let tx_hash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

        let proof_mainnet = ProofBuilder::build_proof(1, tx_hash.to_string(), root_cid).unwrap();
        assert_eq!(proof_mainnet.chain_id(), "eip155:1");

        let proof_gnosis = ProofBuilder::build_proof(100, tx_hash.to_string(), root_cid).unwrap();
        assert_eq!(proof_gnosis.chain_id(), "eip155:100");

        let proof_polygon = ProofBuilder::build_proof(137, tx_hash.to_string(), root_cid).unwrap();
        assert_eq!(proof_polygon.chain_id(), "eip155:137");

        let proof_arbitrum =
            ProofBuilder::build_proof(42161, tx_hash.to_string(), root_cid).unwrap();
        assert_eq!(proof_arbitrum.chain_id(), "eip155:42161");
    }

    /// Verifies that the Rust implementation produces the same CID format as the JavaScript
    /// ceramic-anchor-service convertEthHashToCid function:
    ///
    /// ```javascript
    /// function convertEthHashToCid(hash: string): CID {
    ///   const KECCAK_256_CODE = 0x1b
    ///   const ETH_TX_CODE = 0x93
    ///   const CID_VERSION = 1
    ///   const bytes = Buffer.from(hash, 'hex')
    ///   const multihash = createMultihash(KECCAK_256_CODE, bytes)
    ///   return CID.create(CID_VERSION, ETH_TX_CODE, multihash)
    /// }
    /// ```
    #[test]
    fn test_tx_hash_to_cid_matches_js_implementation() {
        let tx_hash = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        let cid = ProofBuilder::tx_hash_to_cid(tx_hash).unwrap();

        assert_eq!(cid.version(), cid::Version::V1);
        assert_eq!(cid.codec(), 0x93);
        assert_eq!(cid.hash().code(), 0x1b);

        let original_bytes = hex::decode(tx_hash).unwrap();
        assert_eq!(cid.hash().digest(), original_bytes.as_slice());
    }
}
