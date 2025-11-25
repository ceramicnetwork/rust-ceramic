use alloy::{
    primitives::{Address, FixedBytes, U256},
    providers::Provider,
    sol,
    transports::Transport,
};
use anyhow::Result;

// Solidity contract interface for anchoring Ceramic roots
// Based on the existing CeramicAnchorServiceV2 pattern
sol! {
    #[derive(Debug)]
    #[sol(rpc)]
    interface IAnchorContract {
        /// Anchor a root CID on the blockchain (matching existing pattern)
        function anchorDagCbor(bytes32 root) external;
        
        /// Get the block number when a root was anchored
        function getRootBlock(bytes32 root) external view returns (uint256 blockNumber);
        
        /// Event emitted when a root is successfully anchored
        event RootAnchored(bytes32 indexed root, uint256 blockNumber, address indexed anchor);
    }
}

/// Wrapper for interacting with the Ceramic anchor contract
pub struct AnchorContract<T, P> {
    contract: IAnchorContract::IAnchorContractInstance<T, P>,
}

impl<T: Transport + Clone, P: Provider<T> + Clone> AnchorContract<T, P> {
    /// Create a new AnchorContract instance
    pub fn new(address: Address, provider: P) -> Self {
        let contract = IAnchorContract::new(address, provider);
        Self { contract }
    }

    /// Anchor a root CID on the blockchain
    pub async fn anchor_root(&self, root: FixedBytes<32>) -> Result<alloy::rpc::types::TransactionReceipt> {
        let call = self.contract.anchorDagCbor(root);
        let receipt = call.send().await?.get_receipt().await?;
        Ok(receipt)
    }

    /// Check if a root has been anchored and return the block number
    pub async fn get_root_block(&self, root: FixedBytes<32>) -> Result<U256> {
        let result = self.contract.getRootBlock(root).call().await?;
        Ok(result.blockNumber)
    }
}