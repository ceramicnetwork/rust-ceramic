use std::time::Duration;
use std::str::FromStr;

use crate::{EvmConfig, EvmTransactionManager, GasConfig, RetryConfig};
use ceramic_anchor_service::TransactionManager;
use ceramic_core::Cid;
use anyhow::Result;

/// Test anchoring on Gnosis Chain with the provided RPC
/// 
/// This test uses a real Gnosis RPC endpoint to test the anchoring functionality.
/// You'll need to:
/// 1. Deploy the test contract to Gnosis (or use an existing one)
/// 2. Have a test account with some xDAI for gas
/// 
/// Run with:
/// ```
/// TEST_PRIVATE_KEY="your_private_key_hex" \
/// TEST_CONTRACT_ADDRESS="0x..." \
/// GNOSIS_RPC_URL="https://your-rpc-endpoint" \
/// cargo test test_gnosis_anchoring -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore] // Only run with explicit --ignored flag
async fn test_gnosis_anchoring() -> Result<()> {
    // Initialize logging to see what's happening
    tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init()
        .ok();

    // Get configuration from environment variables
    let private_key = std::env::var("TEST_PRIVATE_KEY")
        .expect("TEST_PRIVATE_KEY environment variable required. Set it to a test account private key (hex format, no 0x prefix)");
    
    let contract_address = std::env::var("TEST_CONTRACT_ADDRESS")
        .unwrap_or_else(|_| {
            println!("TEST_CONTRACT_ADDRESS not set. Using deployed test contract on Gnosis.");
            "0x231055A0852D67C7107Ad0d0DFeab60278fE6AdC".to_string() // Existing deployed contract
        });
    
    // Configure for Gnosis Chain
    let rpc_url = std::env::var("GNOSIS_RPC_URL")
        .unwrap_or_else(|_| "https://gnosis-mainnet.public.blastapi.io".to_string());
    
    let config = EvmConfig {
        rpc_url,
        private_key,
        chain_id: 100, // Gnosis Chain
        contract_address,
        gas_config: GasConfig {
            gas_limit: Some(300_000), // Higher limit for safety on Gnosis
            gas_increase_percent: 25, // 25% increase per retry
            override_gas_estimation: false, // Let it estimate gas
            ..GasConfig::default()
        },
        retry_config: RetryConfig {
            max_retries: 5,
            base_delay: Duration::from_secs(3),
            backoff_multiplier: 1.5,
            ..RetryConfig::default()
        },
        confirmations: 2, // Wait for 2 confirmations on Gnosis
        confirmation_timeout: Duration::from_secs(180), // 3 minutes timeout
        poll_interval: Duration::from_secs(5),
    };

    println!("🧪 Testing EVM anchoring on Gnosis Chain");
    println!("📡 RPC: {}", config.rpc_url);
    println!("🔗 Chain ID: {}", config.chain_id);
    println!("📄 Contract: {}", config.contract_address);
    println!("⏰ Confirmations: {}", config.confirmations);

    // Validate configuration first
    println!("🔍 Validating configuration...");
    EvmTransactionManager::validate_config(&config)?;
    println!("✅ Configuration valid");

    // Create the transaction manager
    println!("🏗️ Creating EVM transaction manager...");
    let tx_manager = EvmTransactionManager::new(config).await?;
    println!("✅ Transaction manager created");
    
    // Create a test root CID (this would normally come from the Merkle tree of events)
    let test_root = Cid::from_str("bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54")?;
    println!("📝 Test root CID: {}", test_root);
    
    // Test CID processing (show how CID gets converted to bytes32)
    let cid_bytes32 = EvmTransactionManager::cid_to_bytes32(&test_root)?;
    println!("🔢 CID as bytes32: 0x{}", hex::encode(cid_bytes32.as_slice()));
    
    // Perform the anchoring
    println!("⚓ Starting anchoring process...");
    let start_time = std::time::Instant::now();
    
    match tx_manager.anchor_root(test_root).await {
        Ok(root_time_event) => {
            let duration = start_time.elapsed();
            
            println!("🎉 Anchoring successful in {:?}!", duration);
            println!("📊 Results:");
            println!("  ├─ Chain ID: {}", root_time_event.proof.chain_id());
            println!("  ├─ Transaction Type: {}", root_time_event.proof.tx_type());
            println!("  ├─ Transaction Hash: {}", root_time_event.proof.tx_hash());
            println!("  ├─ Proof CID: {}", root_time_event.detached_time_event.proof);
            println!("  └─ Path: '{}'", root_time_event.detached_time_event.path);
            
            // Verify the proof structure
            assert_eq!(root_time_event.proof.chain_id(), "eip155:100");
            assert_eq!(root_time_event.proof.tx_type(), "f(bytes32)");
            assert!(root_time_event.detached_time_event.path.is_empty()); // Self-anchoring has empty path
            assert!(root_time_event.remote_merkle_nodes.iter().count() == 0); // No remote nodes for self-anchoring
            
            println!("✅ All verification checks passed!");
            
            // Show the transaction hash for verification on block explorer
            println!("🔗 View transaction on Gnosis block explorer:");
            println!("   https://gnosisscan.io/tx/{}", root_time_event.proof.tx_hash());
            
            Ok(())
        }
        Err(e) => {
            println!("❌ Anchoring failed: {}", e);
            println!("💡 Common issues:");
            println!("   • Contract not deployed at the specified address");
            println!("   • Insufficient xDAI balance for gas fees");
            println!("   • Invalid private key format");
            println!("   • Network connectivity issues");
            Err(e)
        }
    }
}

/// Test multiple CID anchoring to see how different CIDs are processed
#[tokio::test]
#[ignore]
async fn test_multiple_cid_processing() -> Result<()> {
    println!("🧪 Testing CID processing for multiple test CIDs");
    
    let test_cids = [
        "bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54",
        "bafyreic5hs2qtbuakzqgj24p5gvs7rxvvfhp6blrq7pljemznaxqlwh6la",
        "bafyreigxfc2hqq5z7w3gpvbk5rjlpvj2nkyz5jk4x5p7l6m7mxpvqs4rm4",
    ];
    
    for (i, cid_str) in test_cids.iter().enumerate() {
        match Cid::from_str(cid_str) {
            Ok(cid) => {
                let bytes32 = EvmTransactionManager::cid_to_bytes32(&cid)?;
                println!("CID {}: {}", i + 1, cid);
                println!("  └─ bytes32: 0x{}", hex::encode(bytes32.as_slice()));
            }
            Err(e) => {
                println!("❌ Failed to parse CID {}: {}", cid_str, e);
            }
        }
    }
    
    Ok(())
}

/// Test configuration validation with various invalid inputs
#[test]
fn test_gnosis_config_validation() {
    // Test valid Gnosis configuration
    let valid_config = EvmConfig {
        rpc_url: "https://gnosis-mainnet.public.blastapi.io".to_string(),
        private_key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        chain_id: 100,
        contract_address: "0x1234567890123456789012345678901234567890".to_string(),
        gas_config: GasConfig {
            gas_limit: Some(300_000),
            gas_increase_percent: 25,
            ..GasConfig::default()
        },
        retry_config: RetryConfig {
            max_retries: 5,
            ..RetryConfig::default()
        },
        confirmations: 2,
        confirmation_timeout: Duration::from_secs(180),
        poll_interval: Duration::from_secs(5),
    };
    
    assert!(EvmTransactionManager::validate_config(&valid_config).is_ok());
    println!("✅ Valid Gnosis configuration passed validation");
    
    // Test various invalid configurations
    let test_cases = [
        ("Empty private key", {
            let mut config = valid_config.clone();
            config.private_key = String::new();
            config
        }),
        ("Empty contract address", {
            let mut config = valid_config.clone();
            config.contract_address = String::new();
            config
        }),
        ("Zero confirmations", {
            let mut config = valid_config.clone();
            config.confirmations = 0;
            config
        }),
        ("Zero max retries", {
            let mut config = valid_config.clone();
            config.retry_config.max_retries = 0;
            config
        }),
        ("Zero gas increase percent", {
            let mut config = valid_config.clone();
            config.gas_config.gas_increase_percent = 0;
            config
        }),
    ];
    
    for (test_name, invalid_config) in test_cases {
        assert!(EvmTransactionManager::validate_config(&invalid_config).is_err());
        println!("✅ {} correctly failed validation", test_name);
    }
}

/// Benchmark CID processing performance
#[test]
fn test_cid_processing_performance() -> Result<()> {
    let test_cid = Cid::from_str("bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54")?;
    
    // Warm up
    for _ in 0..100 {
        let _ = EvmTransactionManager::cid_to_bytes32(&test_cid)?;
    }
    
    // Benchmark
    let iterations = 10_000;
    let start = std::time::Instant::now();
    
    for _ in 0..iterations {
        let _ = EvmTransactionManager::cid_to_bytes32(&test_cid)?;
    }
    
    let duration = start.elapsed();
    let avg_time = duration.as_nanos() as f64 / iterations as f64;
    
    println!("🚀 CID Processing Performance:");
    println!("  ├─ Iterations: {}", iterations);
    println!("  ├─ Total time: {:?}", duration);
    println!("  ├─ Average per CID: {:.1} ns", avg_time);
    println!("  └─ Throughput: {:.0} CIDs/sec", 1_000_000_000.0 / avg_time);
    
    // Should be very fast (less than 1 microsecond per conversion)
    assert!(avg_time < 1_000.0, "CID processing should be sub-microsecond");
    
    Ok(())
}