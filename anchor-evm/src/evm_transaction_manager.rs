use std::time::Duration;

use alloy::{
    network::EthereumWallet,
    primitives::{Address, FixedBytes, U256},
    providers::{Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
    transports::http::{Client, Http},
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use ceramic_anchor_service::{DetachedTimeEvent, MerkleNodes, RootTimeEvent, TransactionManager};
use ceramic_core::{Cid, SerializeExt};
use tokio::time::{interval, sleep};
use tracing::{debug, info, warn};
use url::Url;

use crate::{contract::AnchorContract, proof_builder::ProofBuilder};

/// Configuration for EVM transaction manager
#[derive(Clone)]
pub struct EvmConfig {
    /// RPC endpoint URL for the EVM chain
    pub rpc_url: String,
    /// Private key for signing transactions (hex string without 0x prefix)
    pub private_key: String,
    /// EVM chain ID (e.g., 1 for Ethereum mainnet)
    pub chain_id: u64,
    /// Address of the anchor contract
    pub contract_address: String,
    /// Retry configuration
    pub retry_config: RetryConfig,
    /// Number of confirmations to wait for
    pub confirmations: u64,
    /// Timeout for transaction confirmation
    pub confirmation_timeout: Duration,
    /// Interval between confirmation checks
    pub poll_interval: Duration,
}

/// Retry and recovery configuration
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Base delay between retries
    pub base_delay: Duration,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Whether to check for previous transaction success on nonce errors
    pub check_previous_success: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_secs(5),
            backoff_multiplier: 1.5,
            check_previous_success: true,
        }
    }
}

impl Default for EvmConfig {
    fn default() -> Self {
        Self {
            rpc_url: "http://localhost:8545".to_string(),
            private_key: String::new(),
            chain_id: 1,
            contract_address: String::new(),
            retry_config: RetryConfig::default(),
            confirmations: 4,
            confirmation_timeout: Duration::from_secs(300), // 5 minutes
            poll_interval: Duration::from_secs(5),
        }
    }
}

/// EVM-based transaction manager for self-anchoring
pub struct EvmTransactionManager {
    config: EvmConfig,
}

impl EvmTransactionManager {
    /// Create a new EVM transaction manager
    pub async fn new(config: EvmConfig) -> Result<Self> {
        // Validate configuration
        Self::validate_config(&config)?;

        Ok(Self { config })
    }

    /// Validate the configuration
    pub fn validate_config(config: &EvmConfig) -> Result<()> {
        if config.private_key.is_empty() {
            return Err(anyhow!("Private key cannot be empty"));
        }

        if config.contract_address.is_empty() {
            return Err(anyhow!("Contract address cannot be empty"));
        }

        if config.confirmations == 0 {
            return Err(anyhow!("Confirmations must be greater than 0"));
        }

        if config.retry_config.max_retries == 0 {
            return Err(anyhow!("Max retries must be greater than 0"));
        }

        Ok(())
    }

    /// Convert a Ceramic CID to a 32-byte array for the contract
    /// Following the existing anchor service pattern: removes 4-byte multicodec prefix
    pub fn cid_to_bytes32(cid: &Cid) -> Result<FixedBytes<32>> {
        let cid_bytes = cid.to_bytes();

        // Skip 4-byte multicodec prefix like the existing EthereumBlockchainService
        // This matches: uint8arrays.toString(rootCid.bytes.slice(4), 'base16')
        if cid_bytes.len() < 36 {
            // 4 prefix + 32 hash bytes
            return Err(anyhow!(
                "CID too short: need at least 36 bytes (4 prefix + 32 hash)"
            ));
        }

        let hash_bytes = &cid_bytes[4..]; // Skip multicodec prefix
        let mut bytes32 = [0u8; 32];
        bytes32.copy_from_slice(&hash_bytes[..32]);

        Ok(FixedBytes::from(bytes32))
    }

    /// Submit an anchor transaction and wait for confirmation with retry logic
    async fn submit_and_wait(&self, root_cid: Cid) -> Result<String> {
        info!(
            "Anchoring root CID: {} on chain {}",
            root_cid, self.config.chain_id
        );

        // Parse contract address
        let contract_address: Address = self
            .config
            .contract_address
            .parse()
            .map_err(|e| anyhow!("Invalid contract address: {}", e))?;

        // Create private key signer from hex string
        let private_key_bytes = hex::decode(&self.config.private_key)
            .map_err(|e| anyhow!("Invalid private key hex: {}", e))?;
        let signer = PrivateKeySigner::from_slice(&private_key_bytes)
            .map_err(|e| anyhow!("Invalid private key: {}", e))?;
        let wallet_address = signer.address();

        let wallet = EthereumWallet::from(signer);

        // Create provider
        let rpc_url =
            Url::parse(&self.config.rpc_url).map_err(|e| anyhow!("Invalid RPC URL: {}", e))?;

        let provider = ProviderBuilder::new()
            .with_recommended_fillers()
            .wallet(wallet)
            .on_http(rpc_url);

        let actual_chain_id = provider
            .get_chain_id()
            .await
            .map_err(|e| anyhow!("Failed to connect to EVM node: {}", e))?;

        if actual_chain_id != self.config.chain_id {
            return Err(anyhow!(
                "Chain ID mismatch: configured {} but connected to {}",
                self.config.chain_id,
                actual_chain_id
            ));
        }

        info!("Connected to EVM chain with ID: {}", actual_chain_id);

        // Log starting wallet balance
        let starting_balance = provider
            .get_balance(wallet_address)
            .await
            .map_err(|e| anyhow!("Failed to get wallet balance: {}", e))?;
        info!("Starting wallet balance: {} wei", starting_balance);

        // Create contract instance
        let contract = AnchorContract::new(contract_address, provider.clone());

        // Convert CID to bytes32 for contract call
        let root_bytes32 = Self::cid_to_bytes32(&root_cid)?;

        // Retry loop
        let max_retries = self.config.retry_config.max_retries;
        let mut last_error: Option<anyhow::Error> = None;
        let mut previous_tx_hashes: Vec<String> = Vec::new();

        for attempt in 0..max_retries {
            if attempt > 0 {
                let delay = self.config.retry_config.base_delay.mul_f64(
                    self.config
                        .retry_config
                        .backoff_multiplier
                        .powi(attempt as i32 - 1),
                );
                warn!(
                    "Retry attempt {} of {} after {:?} delay",
                    attempt + 1,
                    max_retries,
                    delay
                );
                sleep(delay).await;
            }

            debug!(
                "Submitting anchor transaction for root: {} (attempt {})",
                root_cid,
                attempt + 1
            );

            match contract.anchor_root(root_bytes32).await {
                Ok(receipt) => {
                    let tx_hash = format!("0x{:x}", receipt.transaction_hash);
                    info!("Anchor transaction submitted: {}", tx_hash);
                    previous_tx_hashes.push(tx_hash.clone());

                    // Check if transaction reverted
                    if !receipt.status() {
                        return Err(anyhow!(
                            "Transaction {} reverted - anchor contract rejected the call",
                            tx_hash
                        ));
                    }

                    // Get block number from receipt - a mined transaction should always have this
                    let block_number = receipt
                        .block_number
                        .ok_or_else(|| anyhow!("Transaction receipt missing block number"))?;

                    // Wait for required confirmations
                    match self
                        .wait_for_confirmations(&provider, &tx_hash, block_number)
                        .await
                    {
                        Ok(()) => {
                            // Log ending wallet balance
                            if let Ok(ending_balance) = provider.get_balance(wallet_address).await {
                                info!("Ending wallet balance: {} wei", ending_balance);
                                let gas_used = starting_balance.saturating_sub(ending_balance);
                                info!("Total gas cost: {} wei", gas_used);
                            }
                            return Ok(tx_hash);
                        }
                        Err(e) => {
                            warn!("Transaction confirmation failed: {}", e);
                            last_error = Some(e);
                        }
                    }
                }
                Err(e) => {
                    let error_str = e.to_string().to_lowercase();

                    // Check for specific error types
                    if error_str.contains("insufficient funds")
                        || error_str.contains("insufficient balance")
                    {
                        // Log detailed cost vs balance info and fail immediately
                        let current_balance = provider
                            .get_balance(wallet_address)
                            .await
                            .unwrap_or(U256::ZERO);
                        return Err(anyhow!(
                            "Insufficient funds for transaction. Current balance: {} wei. Error: {}",
                            current_balance, e
                        ));
                    }

                    if error_str.contains("nonce") && error_str.contains("expired")
                        || error_str.contains("nonce too low")
                        || error_str.contains("replacement transaction underpriced")
                    {
                        // Nonce error - check if a previous transaction was mined
                        if self.config.retry_config.check_previous_success
                            && !previous_tx_hashes.is_empty()
                        {
                            info!("Nonce error detected, checking if previous transaction was mined...");
                            for prev_tx in previous_tx_hashes.iter().rev() {
                                if let Ok(Some(_)) = provider
                                    .get_transaction_receipt(prev_tx.parse().unwrap_or_default())
                                    .await
                                {
                                    info!(
                                        "Previous transaction {} was mined successfully",
                                        prev_tx
                                    );
                                    // Log ending wallet balance
                                    if let Ok(ending_balance) =
                                        provider.get_balance(wallet_address).await
                                    {
                                        info!("Ending wallet balance: {} wei", ending_balance);
                                    }
                                    return Ok(prev_tx.clone());
                                }
                            }
                        }
                    }

                    warn!(
                        "Transaction attempt {} failed: {}. {} retries remain",
                        attempt + 1,
                        e,
                        max_retries - attempt - 1
                    );
                    last_error = Some(anyhow!("Failed to submit anchor transaction: {}", e));
                }
            }
        }

        // All retries exhausted
        Err(last_error.unwrap_or_else(|| {
            anyhow!("Failed to send transaction after {} attempts", max_retries)
        }))
    }

    /// Wait for the required number of confirmations
    async fn wait_for_confirmations<P: Provider<Http<Client>>>(
        &self,
        provider: &P,
        tx_hash: &str,
        tx_block: u64,
    ) -> Result<()> {
        let mut interval = interval(self.config.poll_interval);
        let start_time = std::time::Instant::now();

        loop {
            if start_time.elapsed() > self.config.confirmation_timeout {
                return Err(anyhow!(
                    "Timeout waiting for confirmations for tx: {}",
                    tx_hash
                ));
            }

            interval.tick().await;

            // Get current block number
            let current_block = provider
                .get_block_number()
                .await
                .map_err(|e| anyhow!("Failed to get current block number: {}", e))?;

            let confirmations = current_block.saturating_sub(tx_block);

            debug!(
                "Transaction {} has {} confirmations (need {})",
                tx_hash, confirmations, self.config.confirmations
            );

            if confirmations >= self.config.confirmations {
                info!(
                    "Transaction {} confirmed with {} confirmations",
                    tx_hash, confirmations
                );
                return Ok(());
            }
        }
    }
}

#[async_trait]
impl TransactionManager for EvmTransactionManager {
    async fn anchor_root(&self, root: Cid) -> Result<RootTimeEvent> {
        // Submit transaction and wait for confirmation
        let tx_hash = self.submit_and_wait(root).await?;

        // Build anchor proof from transaction details
        let proof = ProofBuilder::build_proof(self.config.chain_id, tx_hash, root)?;
        let proof_cid = proof.to_cid()?;

        // Create detached time event
        // Since we're self-anchoring, the path is empty (we own the entire tree)
        let detached_time_event = DetachedTimeEvent {
            path: String::new(),
            proof: proof_cid,
        };

        // Return root time event with no additional remote Merkle nodes
        // (all nodes are local since we built the entire tree)
        Ok(RootTimeEvent {
            proof,
            detached_time_event,
            remote_merkle_nodes: MerkleNodes::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_validate_config() {
        let mut config = EvmConfig::default();
        config.private_key = "0x1234".to_string();
        config.contract_address = "0x1234567890123456789012345678901234567890".to_string();

        assert!(EvmTransactionManager::validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_empty_private_key() {
        let config = EvmConfig::default();
        assert!(EvmTransactionManager::validate_config(&config).is_err());
    }

    #[test]
    fn test_cid_to_bytes32() {
        let cid =
            Cid::from_str("bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54").unwrap();
        let bytes32 = EvmTransactionManager::cid_to_bytes32(&cid).unwrap();

        // Should produce a valid 32-byte array
        assert_eq!(bytes32.len(), 32);
    }

    /// Verify Rust cid_to_bytes32 matches JS implementation exactly.
    /// JS test uses: bafyreic5p7grucmzx363ayxgoywb6d4qf5zjxgbqjixpkokbf5jtmdj5ni
    /// JS expects:   0x5d7fcd1a0999befdb062e6762c1f0f902f729b98304a2ef539412f53360d3d6a
    /// From: ceramic-anchor-service/src/services/blockchain/__tests__/eth-bc-service.test.ts
    #[test]
    fn test_cid_to_bytes32_matches_js() {
        let cid =
            Cid::from_str("bafyreic5p7grucmzx363ayxgoywb6d4qf5zjxgbqjixpkokbf5jtmdj5ni").unwrap();
        let bytes32 = EvmTransactionManager::cid_to_bytes32(&cid).unwrap();

        let expected = "5d7fcd1a0999befdb062e6762c1f0f902f729b98304a2ef539412f53360d3d6a";
        let actual = hex::encode(bytes32.as_slice());

        assert_eq!(
            actual, expected,
            "Rust cid_to_bytes32 must match JS implementation"
        );
    }

    #[test]
    fn test_default_config() {
        let config = EvmConfig::default();
        assert_eq!(config.chain_id, 1);
        assert_eq!(config.confirmations, 4);
        assert_eq!(config.retry_config.max_retries, 3);
    }
}
