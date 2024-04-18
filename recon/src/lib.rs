//! Recon is a network protocol for set reconciliation
#![warn(missing_docs, clippy::all)]

pub use crate::{
    client::{Client, Server},
    error::Error,
    metrics::Metrics,
    recon::{
        AssociativeHash, EventIdStore, FullInterests, HashCount, InsertResult, InterestProvider,
        InterestStore, Key, Range, Recon, ReconInterestProvider, ReconItem, Store, SyncState,
    },
    sha256a::Sha256a,
};

mod client;
mod error;
pub mod libp2p;
mod metrics;
pub mod protocol;
mod recon;
mod sha256a;

/// Testing utilities related to recon
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

/// A result type that wraps a recon Error
pub type Result<T> = std::result::Result<T, Error>;
