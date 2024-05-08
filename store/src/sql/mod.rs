mod access;
pub mod entities;
mod event;
mod query;
mod root;
mod sqlite;

// temp to make diff cleaner
pub use event::CeramicOneEvent;

pub use access::{CeramicOneBlock, CeramicOneEventBlock, CeramicOneInterest};
pub use root::SqliteRootStore;
pub use sqlite::{DbTxSqlite, SqlitePool};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Now to handle outstanding database migrations.
/// Intend to add a `Check` variant to verify the database is up to date and return an error if it is not.
pub enum Migrations {
    /// Apply migrations after opening connection
    Apply,
    /// Do nothing
    Skip,
}
