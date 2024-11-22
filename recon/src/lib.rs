//! Recon is a network protocol for set reconciliation
#![warn(missing_docs, missing_debug_implementations, clippy::all)]

pub use crate::{
    error::Error,
    metrics::Metrics,
    recon::{
        btreestore::BTreeStore, AssociativeHash, FullInterests, HashCount, InsertResult,
        InterestProvider, InvalidItem, Key, RangeHash, Recon, ReconInterestProvider, ReconItem,
        Split, Store, SyncState,
    },
    sha256a::Sha256a,
};

mod error;
pub mod libp2p;
mod metrics;
pub mod protocol;
mod recon;
mod sha256a;

#[cfg(test)]
mod tests;

/// A result type that wraps a recon Error
pub type Result<T> = std::result::Result<T, Error>;
