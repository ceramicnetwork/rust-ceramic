//! A unified implementation of both the [`recon::Store`] and [`iroh_bitswap::Store`] traits.
//! This unified implementation allows for exposing Recon values as IPFS blocks
#![warn(missing_docs)]

mod metrics;
mod sql;
#[cfg(test)]
mod tests;

pub use metrics::{Metrics, StoreMetricsMiddleware};
pub use sql::{DbTx, Migrations, RootStore, SqliteEventStore, SqliteInterestStore, SqlitePool};
