//! An implementation of store for event.

mod error;
mod metrics;
mod sql;

pub use error::Error;
pub use metrics::{Metrics, StoreMetricsMiddleware};
pub use sql::{
    entities::{BlockHash, EventBlockRaw, EventInsertable},
    CeramicOneBlock, CeramicOneEvent, CeramicOneEventBlock, CeramicOneVersion, InsertResult,
    InsertedEvent, Migrations, SqlitePool, SqliteRootStore, SqliteTransaction,
};

pub(crate) type Result<T> = std::result::Result<T, Error>;
