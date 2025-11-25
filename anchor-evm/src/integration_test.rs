use std::time::Duration;
use std::str::FromStr;

use crate::{EvmConfig, EvmTransactionManager, GasConfig, RetryConfig};
use ceramic_anchor_service::TransactionManager;
use ceramic_core::Cid;
// use alloy::primitives::U256;
use anyhow::Result;

/// Integration test for the EVM transaction manager
/// 
/// This test can be run against a real blockchain (like Gnosis) with:
/// ```
/// RUST_LOG=debug TEST_RPC_URL="https://gnosis-rpc-url" \
/// TEST_PRIVATE_KEY="your_private_key_hex" \
/// TEST_CONTRACT_ADDRESS="0x..." \
/// cargo test test_gnosis_integration -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore] // Only run with explicit --ignored flag
async fn test_gnosis_integration() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init()
        .ok();

    // Get configuration from environment variables
    let rpc_url = std::env::var("TEST_RPC_URL")
        .expect("TEST_RPC_URL environment variable required for integration test");
    
    let private_key = std::env::var("TEST_PRIVATE_KEY")
        .expect("TEST_PRIVATE_KEY environment variable required for integration test");
    
    let contract_address = std::env::var("TEST_CONTRACT_ADDRESS")
        .expect("TEST_CONTRACT_ADDRESS environment variable required for integration test");
    
    // Configure for Gnosis Chain (Chain ID 100)
    let config = EvmConfig {
        rpc_url,
        private_key,
        chain_id: 100, // Gnosis Chain
        contract_address,
        gas_config: GasConfig {
            gas_limit: Some(200_000), // Higher limit for safety
            gas_increase_percent: 20, // 20% increase per retry
            ..GasConfig::default()
        },
        retry_config: RetryConfig {
            max_retries: 5,
            base_delay: Duration::from_secs(3),
            ..RetryConfig::default()
        },
        confirmations: 2, // Wait for 2 confirmations on Gnosis
        confirmation_timeout: Duration::from_secs(120), // 2 minutes timeout
        poll_interval: Duration::from_secs(5),
    };

    // Create the transaction manager
    let tx_manager = EvmTransactionManager::new(config).await?;
    
    // Create a test root CID
    let test_root = Cid::from_str("bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54")?;
    
    println!("Testing EVM transaction manager on Gnosis Chain");
    println!("Root CID to anchor: {}", test_root);
    
    // Test the anchoring process
    let root_time_event = tx_manager.anchor_root(test_root).await?;
    
    println!("✅ Anchoring successful!");
    println!("Chain ID: {}", root_time_event.proof.chain_id());
    println!("Transaction Type: {}", root_time_event.proof.tx_type());
    println!("Proof CID: {}", root_time_event.detached_time_event.proof);
    println!("Path: '{}'", root_time_event.detached_time_event.path);
    
    // Verify the proof structure
    assert_eq!(root_time_event.proof.chain_id(), "eip155:100");
    assert_eq!(root_time_event.proof.tx_type(), "f(bytes32)");
    assert!(root_time_event.detached_time_event.path.is_empty()); // Self-anchoring has empty path
    
    println!("✅ All verification checks passed!");
    
    Ok(())
}

/// Test the configuration validation
#[test]
fn test_config_validation() {
    // Valid configuration
    let valid_config = EvmConfig {
        rpc_url: "http://localhost:8545".to_string(),
        private_key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        chain_id: 100,
        contract_address: "0x1234567890123456789012345678901234567890".to_string(),
        ..EvmConfig::default()
    };
    assert!(EvmTransactionManager::validate_config(&valid_config).is_ok());
    
    // Invalid configurations
    let mut invalid_config = valid_config.clone();
    invalid_config.private_key = String::new();
    assert!(EvmTransactionManager::validate_config(&invalid_config).is_err());
    
    invalid_config = valid_config.clone();
    invalid_config.contract_address = String::new();
    assert!(EvmTransactionManager::validate_config(&invalid_config).is_err());
    
    invalid_config = valid_config.clone();
    invalid_config.confirmations = 0;
    assert!(EvmTransactionManager::validate_config(&invalid_config).is_err());
}

/// Test CID to bytes32 conversion matches existing service pattern
#[test]
fn test_cid_processing() -> Result<()> {
    let test_cid = Cid::from_str("bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54")?;
    let bytes32 = EvmTransactionManager::cid_to_bytes32(&test_cid)?;
    
    // Should be 32 bytes
    assert_eq!(bytes32.len(), 32);
    
    // The conversion should be deterministic
    let bytes32_again = EvmTransactionManager::cid_to_bytes32(&test_cid)?;
    assert_eq!(bytes32, bytes32_again);
    
    println!("CID: {}", test_cid);
    println!("Bytes32: 0x{}", hex::encode(bytes32.as_slice()));
    
    Ok(())
}

/// Test with multiple different CIDs to ensure robustness
#[test]
fn test_multiple_cids() -> Result<()> {
    let test_cids = [
        "bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54",
        "bafyreih4qf5knkxlrlxf7o7x3hgqe6h4r4u4r4u4r4u4r4u4r4u4r4u4r",
        "bafyreibla2vbn6edfbc6g2wc5j5f7mzr5wzxlmcf6frlhxdmk5wfv6q4e",
    ];
    
    for cid_str in test_cids {
        if let Ok(cid) = Cid::from_str(cid_str) {
            let bytes32 = EvmTransactionManager::cid_to_bytes32(&cid)?;
            assert_eq!(bytes32.len(), 32);
            println!("CID {} -> 0x{}", cid, hex::encode(bytes32.as_slice()));
        }
    }
    
    Ok(())
}

/// Performance test for CID processing
#[test]
fn test_cid_processing_performance() -> Result<()> {
    let test_cid = Cid::from_str("bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54")?;
    
    let start = std::time::Instant::now();
    for _ in 0..1000 {
        let _bytes32 = EvmTransactionManager::cid_to_bytes32(&test_cid)?;
    }
    let duration = start.elapsed();
    
    println!("Processed 1000 CIDs in {:?} ({:.2}μs per CID)", 
             duration, duration.as_micros() as f64 / 1000.0);
    
    // Should be very fast (less than 1ms for 1000 conversions)
    assert!(duration < Duration::from_millis(1));
    
    Ok(())
}

/// Test configuration for different networks
#[test]
fn test_network_configurations() {
    let networks = [
        ("Ethereum Mainnet", 1, "https://mainnet.infura.io/v3/KEY"),
        ("Gnosis Chain", 100, "https://rpc.gnosischain.com"),
        ("Polygon", 137, "https://polygon-rpc.com"),
        ("Arbitrum One", 42161, "https://arb1.arbitrum.io/rpc"),
        ("Base", 8453, "https://mainnet.base.org"),
    ];
    
    for (name, chain_id, rpc_url) in networks {
        let config = EvmConfig {
            rpc_url: rpc_url.to_string(),
            private_key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            chain_id,
            contract_address: "0x1234567890123456789012345678901234567890".to_string(),
            gas_config: GasConfig {
                gas_limit: Some(if chain_id == 1 { 150_000 } else { 100_000 }),
                ..GasConfig::default()
            },
            ..EvmConfig::default()
        };
        
        assert!(EvmTransactionManager::validate_config(&config).is_ok());
        println!("✅ {} (Chain ID {}) configuration valid", name, chain_id);
    }
}