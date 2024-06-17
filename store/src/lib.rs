//! A unified implementation of both the [`recon::Store`] and [`iroh_bitswap::Store`] traits.
//! This unified implementation allows for exposing Recon values as IPFS blocks
#![warn(missing_docs)]

mod error;
mod metrics;
mod migration;
mod sql;

pub use error::Error;
pub use metrics::{Metrics, StoreMetricsMiddleware};
pub use migration::DataMigrator;
pub use sql::{
    entities::EventInsertable, entities::EventInsertableBody, CeramicOneBlock, CeramicOneEvent,
    CeramicOneEventBlock, CeramicOneInterest, Migrations, SqlitePool, SqliteRootStore,
    SqliteTransaction,
};

pub(crate) type Result<T> = std::result::Result<T, Error>;
