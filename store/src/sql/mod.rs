mod entities;
mod postgres;
mod root;
mod sqlite;

pub use entities::*;
pub use postgres::{DbTxPg, EventStorePostgres, InterestStorePostgres, PostgresPool};
pub use root::{RootStorePostgres, RootStoreSqlite};
pub use sqlite::{DbTxSqlite, EventStoreSqlite, InterestStoreSqlite, SqlitePool};

use std::sync::atomic::AtomicI64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Now to handle outstanding database migrations.
/// Intend to add a `Check` variant to verify the database is up to date and return an error if it is not.
pub enum Migrations {
    /// Apply migrations after opening connection
    Apply,
    /// Do nothing
    Skip,
}

pub(crate) static GLOBAL_COUNTER: AtomicI64 = AtomicI64::new(0);
