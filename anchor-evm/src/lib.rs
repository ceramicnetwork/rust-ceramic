//! EVM blockchain anchoring service for Ceramic
//!
//! This crate provides a self-anchoring implementation that can submit Ceramic root CIDs
//! to any EVM-compatible blockchain using the alloy library.
#![warn(missing_docs)]

mod contract;
mod evm_transaction_manager;
mod proof_builder;

#[cfg(test)]
mod integration_test;

pub use contract::AnchorContract;
pub use evm_transaction_manager::{EvmConfig, EvmTransactionManager, RetryConfig};
