mod event;
mod interest;
mod postgres;
mod root;
mod sqlite;

pub use event::EventStore;
pub use interest::InterestStore;
pub use postgres::PostgresPool;
pub use root::RootStore;
pub use sqlite::SqlitePool;

use sqlx::{Postgres, Sqlite, Transaction};

/// A trivial wrapper around a sqlx Sqlite database transaction
// pub type DbTx<'a> = Transaction<'a, Sqlite>;
pub type DbTx<'a> = Transaction<'a, Postgres>;

pub type DbTxTemp<'a> = Transaction<'a, Sqlite>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Now to handle ounstanding database migrations.
/// Intend to add a `Check` variant to verify the database is up to date and return an error if it is not.
pub enum Migrations {
    /// Apply migrations after opening connection
    Apply,
    /// Do nothing
    Skip,
}
