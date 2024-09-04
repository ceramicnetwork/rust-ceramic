mod access;
pub mod entities;
mod query;
mod sqlite;

pub use access::{CeramicOneInterest, CeramicOneVersion};
pub use sqlite::{SqlitePool, SqliteTransaction};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Now to handle outstanding database migrations.
/// Intend to add a `Check` variant to verify the database is up to date and return an error if it is not.
pub enum Migrations {
    /// Apply migrations after opening connection
    Apply,
    /// Do nothing
    Skip,
}
