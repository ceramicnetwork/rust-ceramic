//! An implementation of store for event.

mod error;
mod metrics;
mod sql;

pub use error::Error;
pub use metrics::{Metrics, StoreMetricsMiddleware};
pub use sql::{CeramicOneInterest, CeramicOneVersion, Migrations, SqlitePool, SqliteTransaction};

pub(crate) type Result<T> = std::result::Result<T, Error>;
