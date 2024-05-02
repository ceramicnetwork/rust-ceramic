mod access;
pub mod entities;
mod query;
mod root;
mod sqlite;
mod anchor_request;

#[cfg(test)]
mod test;


pub use access::{
    CeramicOneBlock, CeramicOneEvent, CeramicOneEventBlock, CeramicOneInterest, InsertResult,
    InsertedEvent,
};
pub use root::SqliteRootStore;
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
