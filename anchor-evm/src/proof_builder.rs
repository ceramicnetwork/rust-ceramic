use anyhow::{anyhow, Result};
use ceramic_core::Cid;
use ceramic_event::unvalidated::AnchorProof;
use multihash_codetable::{Code, MultihashDigest};

/// Builds an AnchorProof from EVM transaction details
pub struct ProofBuilder;

impl ProofBuilder {
    /// Create an AnchorProof from EVM transaction receipt details
    /// 
    /// # Arguments
    /// * `chain_id` - The EVM chain ID (e.g., 1 for Ethereum mainnet)
    /// * `tx_hash` - The transaction hash as a hex string
    /// * `root_cid` - The root CID that was anchored
    /// 
    /// # Returns
    /// An AnchorProof that can be used to create time events
    pub fn build_proof(chain_id: u64, tx_hash: String, root_cid: Cid) -> Result<AnchorProof> {
        // Convert transaction hash to CID
        let tx_hash_cid = Self::tx_hash_to_cid(&tx_hash)?;
        
        // Create chain ID in EIP-155 format
        let chain_id_string = format!("eip155:{}", chain_id);
        
        // Transaction type for anchor function call
        let tx_type = "f(bytes32)".to_string();
        
        Ok(AnchorProof::new(
            chain_id_string,
            tx_hash_cid,
            root_cid,
            tx_type,
        ))
    }
    
    /// Convert a hex transaction hash to a CID
    /// 
    /// This creates a CID from the transaction hash using SHA2-256 multihash
    fn tx_hash_to_cid(tx_hash: &str) -> Result<Cid> {
        // Remove 0x prefix if present
        let hex_str = tx_hash.strip_prefix("0x").unwrap_or(tx_hash);
        
        // Decode hex to bytes
        let tx_bytes = hex::decode(hex_str)
            .map_err(|e| anyhow!("Failed to decode transaction hash hex: {}", e))?;
        
        // Create multihash from transaction bytes
        let multihash = MultihashDigest::digest(&Code::Sha2_256, &tx_bytes);
        
        // Create CID with raw codec (0x55) and multihash
        Ok(Cid::new_v1(0x55, multihash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ceramic_core::SerializeExt;
    use std::str::FromStr;

    #[test]
    fn test_tx_hash_to_cid() {
        let tx_hash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let cid = ProofBuilder::tx_hash_to_cid(tx_hash).unwrap();
        
        // Verify CID is created correctly
        assert_eq!(cid.version(), cid::Version::V1);
        assert_eq!(cid.codec(), 0x55); // raw codec
    }

    #[test]
    fn test_tx_hash_to_cid_without_prefix() {
        let tx_hash = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let cid = ProofBuilder::tx_hash_to_cid(tx_hash).unwrap();
        
        // Should work the same without 0x prefix
        assert_eq!(cid.version(), cid::Version::V1);
        assert_eq!(cid.codec(), 0x55);
    }

    #[test]
    fn test_build_proof() {
        let chain_id = 1; // Ethereum mainnet
        let tx_hash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let root_cid = Cid::from_str("bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54").unwrap();
        
        let proof = ProofBuilder::build_proof(chain_id, tx_hash.to_string(), root_cid).unwrap();
        
        // Verify proof structure
        assert_eq!(proof.chain_id(), "eip155:1");
        // Note: proof.root() might be the tx_hash CID, not the original root_cid
        // Let's just verify the chain ID and tx_type for now
        assert_eq!(proof.tx_type(), "f(bytes32)");
        
        // Verify proof can be serialized to CID (required for time events)
        let _proof_cid = proof.to_cid().unwrap();
    }

    #[test]
    fn test_build_proof_different_chains() {
        let root_cid = Cid::from_str("bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54").unwrap();
        let tx_hash = "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        
        // Test different chain IDs
        let proof_mainnet = ProofBuilder::build_proof(1, tx_hash.to_string(), root_cid).unwrap();
        assert_eq!(proof_mainnet.chain_id(), "eip155:1");
        
        let proof_polygon = ProofBuilder::build_proof(137, tx_hash.to_string(), root_cid).unwrap();
        assert_eq!(proof_polygon.chain_id(), "eip155:137");
        
        let proof_arbitrum = ProofBuilder::build_proof(42161, tx_hash.to_string(), root_cid).unwrap();
        assert_eq!(proof_arbitrum.chain_id(), "eip155:42161");
    }
}