use alloy::{
    primitives::{Address, FixedBytes},
    providers::Provider,
    sol,
    transports::Transport,
};
use anyhow::Result;

// Solidity contract interface for anchoring Ceramic roots
sol! {
    #[derive(Debug)]
    #[sol(rpc)]
    interface IAnchorContract {
        /// Anchor a root CID on the blockchain
        function anchorDagCbor(bytes32 root) external;
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
    pub async fn anchor_root(
        &self,
        root: FixedBytes<32>,
    ) -> Result<alloy::rpc::types::TransactionReceipt> {
        let call = self.contract.anchorDagCbor(root);
        let receipt = call.send().await?.get_receipt().await?;
        Ok(receipt)
    }
}
