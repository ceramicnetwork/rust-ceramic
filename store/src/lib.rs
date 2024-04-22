//! A unified implementation of both the [`recon::Store`] and [`iroh_bitswap::Store`] traits.
//! This unified implementation allows for exposing Recon values as IPFS blocks
#![warn(missing_docs)]

mod error;
mod metrics;
mod sql;
#[cfg(test)]
mod tests;

pub use error::Error;
pub use metrics::{Metrics, StoreMetricsMiddleware};
pub use sql::{
    DbTxPg, DbTxSqlite, EventStorePostgres, EventStoreSqlite, InterestStorePostgres,
    InterestStoreSqlite, Migrations, PostgresPool, RootStorePostgres, RootStoreSqlite, SqlitePool,
};

pub(crate) type Result<T> = std::result::Result<T, Error>;
