//! A unified implementation of both the [`recon::Store`] and [`iroh_bitswap::Store`] traits.
//! This unified implementation allows for exposing Recon values as IPFS blocks
#![warn(missing_docs)]

mod sqlite;
mod store_metrics;
#[cfg(test)]
mod tests;

pub use sqlite::{DbTx, InterestStore, ModelStore, SqlitePool};
pub use store_metrics::{Metrics, StoreMetricsMiddleware};
