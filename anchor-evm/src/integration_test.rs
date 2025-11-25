use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use ceramic_anchor_service::TransactionManager;
use ceramic_core::Cid;

use crate::{EvmConfig, EvmTransactionManager, RetryConfig};

/// Integration test for the EVM transaction manager
///
/// Run with:
/// ```
/// TEST_PRIVATE_KEY="your_private_key_hex" \
/// cargo test -p ceramic-anchor-evm test_evm_anchoring -- --ignored --nocapture
/// ```
///
/// Optional environment variables:
/// - TEST_RPC_URL: RPC endpoint (defaults to public Gnosis RPC)
/// - TEST_CONTRACT_ADDRESS: Contract address (defaults to deployed test contract)
/// - TEST_CHAIN_ID: Chain ID (defaults to 100 for Gnosis)
#[tokio::test]
#[ignore]
async fn test_evm_anchoring() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("debug")
        .try_init()
        .ok();

    let private_key =
        std::env::var("TEST_PRIVATE_KEY").expect("TEST_PRIVATE_KEY environment variable required");

    let rpc_url = std::env::var("TEST_RPC_URL")
        .unwrap_or_else(|_| "https://gnosis-mainnet.public.blastapi.io".to_string());

    let contract_address = std::env::var("TEST_CONTRACT_ADDRESS")
        .unwrap_or_else(|_| "0x231055A0852D67C7107Ad0d0DFeab60278fE6AdC".to_string());

    let chain_id: u64 = std::env::var("TEST_CHAIN_ID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    let config = EvmConfig {
        rpc_url: rpc_url.clone(),
        private_key,
        chain_id,
        contract_address: contract_address.clone(),
        retry_config: RetryConfig {
            max_retries: 5,
            base_delay: Duration::from_secs(3),
            ..RetryConfig::default()
        },
        confirmations: 2,
        confirmation_timeout: Duration::from_secs(180),
        poll_interval: Duration::from_secs(5),
    };

    println!("RPC: {}", rpc_url);
    println!("Chain ID: {}", chain_id);
    println!("Contract: {}", contract_address);

    let tx_manager = EvmTransactionManager::new(config).await?;

    let test_root = Cid::from_str("bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54")?;
    println!("Test root CID: {}", test_root);

    let start_time = std::time::Instant::now();

    match tx_manager.anchor_root(test_root).await {
        Ok(root_time_event) => {
            let duration = start_time.elapsed();

            println!("Anchoring successful in {:?}", duration);
            println!("Chain ID: {}", root_time_event.proof.chain_id());
            println!("Transaction Type: {}", root_time_event.proof.tx_type());
            println!("Transaction Hash: {}", root_time_event.proof.tx_hash());
            println!("Proof CID: {}", root_time_event.detached_time_event.proof);

            assert_eq!(
                root_time_event.proof.chain_id(),
                format!("eip155:{}", chain_id)
            );
            assert_eq!(root_time_event.proof.tx_type(), "f(bytes32)");
            assert!(root_time_event.detached_time_event.path.is_empty());
            assert_eq!(root_time_event.remote_merkle_nodes.iter().count(), 0);

            Ok(())
        }
        Err(e) => {
            println!("Anchoring failed: {}", e);
            Err(e)
        }
    }
}

#[test]
fn test_config_validation() {
    let valid_config = EvmConfig {
        rpc_url: "http://localhost:8545".to_string(),
        private_key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        chain_id: 100,
        contract_address: "0x1234567890123456789012345678901234567890".to_string(),
        ..EvmConfig::default()
    };
    assert!(EvmTransactionManager::validate_config(&valid_config).is_ok());

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
    ];

    for (test_name, invalid_config) in test_cases {
        assert!(
            EvmTransactionManager::validate_config(&invalid_config).is_err(),
            "{} should fail validation",
            test_name
        );
    }
}

#[test]
fn test_cid_to_bytes32() -> Result<()> {
    let test_cid = Cid::from_str("bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54")?;
    let bytes32 = EvmTransactionManager::cid_to_bytes32(&test_cid)?;

    assert_eq!(bytes32.len(), 32);

    // Conversion should be deterministic
    let bytes32_again = EvmTransactionManager::cid_to_bytes32(&test_cid)?;
    assert_eq!(bytes32, bytes32_again);

    Ok(())
}

#[test]
fn test_network_configurations() {
    let networks = [
        ("Ethereum Mainnet", 1),
        ("Gnosis Chain", 100),
        ("Polygon", 137),
        ("Arbitrum One", 42161),
        ("Base", 8453),
    ];

    for (name, chain_id) in networks {
        let config = EvmConfig {
            rpc_url: "http://localhost:8545".to_string(),
            private_key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_string(),
            chain_id,
            contract_address: "0x1234567890123456789012345678901234567890".to_string(),
            ..EvmConfig::default()
        };

        assert!(
            EvmTransactionManager::validate_config(&config).is_ok(),
            "{} config should be valid",
            name
        );
    }
}

/// Full integration test for the AnchorService with EVM transaction manager
///
/// This test verifies the complete anchor flow:
/// 1. AnchorService receives anchor requests from MockAnchorEventService
/// 2. Builds a Merkle tree from the requests
/// 3. Anchors the root on the EVM chain via EvmTransactionManager
/// 4. Creates time events with proper proofs and merkle paths
///
/// Run with:
/// ```
/// TEST_PRIVATE_KEY="your_private_key_hex" \
/// cargo test -p ceramic-anchor-evm test_anchor_service_with_evm -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore]
async fn test_anchor_service_with_evm() -> Result<()> {
    use ceramic_anchor_service::{AnchorService, MockAnchorEventService, Store};
    use ceramic_core::NodeKey;
    use ceramic_sql::sqlite::SqlitePool;
    use std::sync::Arc;

    tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init()
        .ok();

    let private_key =
        std::env::var("TEST_PRIVATE_KEY").expect("TEST_PRIVATE_KEY environment variable required");

    let rpc_url = std::env::var("TEST_RPC_URL")
        .unwrap_or_else(|_| "https://gnosis-mainnet.public.blastapi.io".to_string());

    let contract_address = std::env::var("TEST_CONTRACT_ADDRESS")
        .unwrap_or_else(|_| "0x231055A0852D67C7107Ad0d0DFeab60278fE6AdC".to_string());

    let chain_id: u64 = std::env::var("TEST_CHAIN_ID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    println!("=== Full AnchorService Integration Test ===");
    println!("RPC: {}", rpc_url);
    println!("Chain ID: {}", chain_id);
    println!("Contract: {}", contract_address);

    // Create EVM transaction manager
    let config = EvmConfig {
        rpc_url,
        private_key,
        chain_id,
        contract_address,
        retry_config: RetryConfig {
            max_retries: 5,
            base_delay: Duration::from_secs(3),
            ..RetryConfig::default()
        },
        confirmations: 2,
        confirmation_timeout: Duration::from_secs(180),
        poll_interval: Duration::from_secs(5),
    };
    let tx_manager = Arc::new(EvmTransactionManager::new(config).await?);

    // Create mock event service with 3 anchor requests
    let num_requests = 3u64;
    let mock_event_service = Arc::new(MockAnchorEventService::new(num_requests));

    // Create in-memory SQLite pool for high water mark storage
    let pool = SqlitePool::connect_in_memory().await?;

    // Create AnchorService
    let node_id = NodeKey::random().id();
    let anchor_service = AnchorService::new(
        tx_manager,
        mock_event_service.clone(),
        pool,
        node_id,
        Duration::from_secs(3600), // anchor_interval (not used in direct call)
        1000,                      // anchor_batch_size
    );

    // Get anchor requests
    let anchor_requests = mock_event_service
        .events_since_high_water_mark(node_id, 0, 1_000_000)
        .await?;

    println!("Anchor requests: {}", anchor_requests.len());
    for (i, req) in anchor_requests.iter().enumerate() {
        println!("  Request {}: id={}, prev={}", i, req.id, req.prev);
    }

    // Anchor the batch
    let start_time = std::time::Instant::now();
    println!("\nAnchoring batch...");

    let time_event_batch = anchor_service
        .anchor_batch(anchor_requests.as_slice())
        .await?;

    let duration = start_time.elapsed();
    println!("Anchoring completed in {:?}", duration);

    // Verify results
    println!("\n=== Results ===");
    println!(
        "Time events created: {}",
        time_event_batch.raw_time_events.events.len()
    );

    // Check proof
    let proof = &time_event_batch.proof;
    println!("Proof chain ID: {}", proof.chain_id());
    println!("Proof tx_type: {}", proof.tx_type());
    println!("Proof tx_hash: {}", proof.tx_hash());
    println!("Proof root: {}", proof.root());

    // Verify chain ID format
    assert_eq!(
        proof.chain_id(),
        format!("eip155:{}", chain_id),
        "Chain ID should be in EIP-155 format"
    );

    // Verify tx_type
    assert_eq!(
        proof.tx_type(),
        "f(bytes32)",
        "Transaction type should be f(bytes32)"
    );

    // Verify we got time events for all requests
    assert_eq!(
        time_event_batch.raw_time_events.events.len(),
        num_requests as usize,
        "Should have one time event per anchor request"
    );

    // Print time event details (events is Vec<(AnchorRequest, RawTimeEvent)>)
    for (i, (_anchor_req, time_event)) in time_event_batch.raw_time_events.events.iter().enumerate()
    {
        println!(
            "\nTime Event {}:\n  prev: {}\n  proof: {}\n  path: {}",
            i,
            time_event.prev(),
            time_event.proof(),
            time_event.path()
        );
    }

    // Verify merkle paths are correct for a 3-node tree
    // With 3 nodes, tree structure is:
    //       root
    //      /    \
    //    node    leaf2
    //   /    \
    // leaf0  leaf1
    //
    // Paths: leaf0="0/0", leaf1="0/1", leaf2="1"
    let expected_paths = ["0/0", "0/1", "1"];
    for (i, (_anchor_req, time_event)) in time_event_batch.raw_time_events.events.iter().enumerate()
    {
        assert_eq!(
            time_event.path(),
            expected_paths[i],
            "Time event {} should have path {}",
            i,
            expected_paths[i]
        );
    }

    println!("\n✅ All assertions passed!");
    println!("The full anchor flow with EVM anchoring works correctly.");

    Ok(())
}
