//! A unified implementation of both the [`recon::Store`] and [`iroh_bitswap::Store`] traits.
//! This unified implementation allows for exposing Recon values as IPFS blocks
#![warn(missing_docs)]

mod error;
mod metrics;
mod red_tree;
mod sql;
#[cfg(test)]
mod tests;

pub use error::Error;
pub use metrics::{Metrics, StoreMetricsMiddleware};
pub use red_tree::RedTree;
pub use sql::{
    DbTxSqlite, Migrations, SqliteEventStore, SqliteInterestStore, SqlitePool, SqliteRootStore,
};

pub(crate) type Result<T> = std::result::Result<T, Error>;
