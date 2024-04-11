//! Recon is a network protocol for set reconciliation
#![warn(missing_docs, missing_debug_implementations, clippy::all)]

pub use crate::{
    client::{Client, Server},
    metrics::Metrics,
    recon::{
        btreestore::BTreeStore, AssociativeHash, EventIdStore, FullInterests, HashCount,
        InsertResult, InterestProvider, InterestStore, Key, Range, Recon, ReconInterestProvider,
        ReconItem, Store, SyncState,
    },
    sha256a::Sha256a,
};

mod client;
pub mod libp2p;
mod metrics;
pub mod protocol;
mod recon;
mod sha256a;

#[cfg(test)]
mod tests;
