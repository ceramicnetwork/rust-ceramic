use std::str::FromStr as _;

use alloy::primitives::TxHash;
use ceramic_core::Cid;

/// The ethereum RPC provider module
pub mod eth_rpc;

/// Get the expected transaction hash for a given root CID (this is v1 proof type)
pub(crate) fn tx_hash_try_from_cid(cid: Cid) -> anyhow::Result<TxHash> {
    Ok(TxHash::from_str(&hex::encode(cid.hash().digest()))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use multihash_codetable::{Code, MultihashDigest};

    /// Ethereum transaction codec for IPLD (from multicodec table)
    /// This matches the constant in anchor-evm/src/proof_builder.rs
    const ETH_TX_CODEC: u64 = 0x93;

    /// Simulate what ProofBuilder::tx_hash_to_cid does
    fn simulate_proof_builder_tx_hash_to_cid(tx_hash: &str) -> Cid {
        let hex_str = tx_hash.strip_prefix("0x").unwrap_or(tx_hash);
        let tx_bytes = hex::decode(hex_str).unwrap();
        let multihash = Code::Keccak256.wrap(&tx_bytes).unwrap();
        Cid::new_v1(ETH_TX_CODEC, multihash)
    }

    #[test]
    fn test_tx_hash_roundtrip_self_anchor_to_lookup() {
        // critical invariant: what self-anchoring stores must match what lookup queries
        let original_tx_hash = "0xa1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";

        // Step 1: ProofBuilder creates a CID from the tx_hash (simulated here)
        let tx_cid = simulate_proof_builder_tx_hash_to_cid(original_tx_hash);

        // Step 2: Validation converts CID back to tx_hash for lookup
        let lookup_tx_hash = tx_hash_try_from_cid(tx_cid).unwrap().to_string();

        // Step 3: Verify they match (both should be 0x-prefixed lowercase hex)
        assert_eq!(
            original_tx_hash.to_lowercase(),
            lookup_tx_hash.to_lowercase(),
            "Self-anchored tx_hash must match lookup tx_hash for chain proof discovery to work"
        );
    }

    #[test]
    fn test_tx_hash_roundtrip_without_0x_prefix() {
        // Test that round-trip works even without 0x prefix in original
        let original_tx_hash = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";

        let tx_cid = simulate_proof_builder_tx_hash_to_cid(original_tx_hash);
        let lookup_tx_hash = tx_hash_try_from_cid(tx_cid).unwrap().to_string();

        // TxHash::to_string() always adds 0x prefix
        assert_eq!(
            format!("0x{}", original_tx_hash.to_lowercase()),
            lookup_tx_hash.to_lowercase()
        );
    }

    #[test]
    fn test_tx_hash_format_matches_alloy_txhash() {
        // Verify that our round-trip produces the same format as alloy's TxHash::to_string()
        let tx_hash_hex = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        let tx_cid = simulate_proof_builder_tx_hash_to_cid(tx_hash_hex);

        let recovered = tx_hash_try_from_cid(tx_cid).unwrap();

        // Create TxHash directly from the same hex
        let direct = TxHash::from_str(tx_hash_hex).unwrap();

        assert_eq!(recovered, direct);
        assert_eq!(recovered.to_string(), direct.to_string());
    }

    #[test]
    fn test_tx_hash_roundtrip_mixed_case() {
        // Test that mixed-case hex input produces correct round-trip.
        // Ethereum tx hashes are case-insensitive, but our storage uses lowercase.
        // This verifies the normalization works correctly.
        let mixed_case_tx_hash =
            "0xAbCdEf1234567890AbCdEf1234567890AbCdEf1234567890AbCdEf1234567890";

        // Step 1: ProofBuilder creates a CID (hex decoding is case-insensitive)
        let tx_cid = simulate_proof_builder_tx_hash_to_cid(mixed_case_tx_hash);

        // Step 2: Validation converts CID back to tx_hash for lookup
        let lookup_tx_hash = tx_hash_try_from_cid(tx_cid).unwrap().to_string();

        // Step 3: The lookup should produce lowercase (TxHash::to_string() uses lowercase)
        assert_eq!(
            lookup_tx_hash, "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "Mixed-case input should normalize to lowercase for lookup"
        );

        // Step 4: Verify case-insensitive equality with original
        assert_eq!(
            mixed_case_tx_hash.to_lowercase(),
            lookup_tx_hash,
            "Normalized tx_hash must match original (case-insensitive)"
        );
    }
}
