//! EVM blockchain anchoring service for Ceramic
//! 
//! This crate provides a self-anchoring implementation that can submit Ceramic root CIDs
//! to any EVM-compatible blockchain using the alloy library.
#![warn(missing_docs)]

mod evm_transaction_manager;
mod proof_builder;
mod contract;

#[cfg(test)]
mod integration_test;

#[cfg(test)]
mod gnosis_test;

pub use evm_transaction_manager::{EvmTransactionManager, EvmConfig, GasConfig, RetryConfig};
pub use contract::AnchorContract;