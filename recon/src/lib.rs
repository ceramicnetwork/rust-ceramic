//! Recon is a network protocol for set reconciliation
#![warn(missing_docs, missing_debug_implementations, clippy::all)]

pub use crate::recon::{
    btreestore::BTreeStore, sqlitestore::SQLiteStore, AssociativeHash, FullInterests,
    InterestProvider, Key, Message, Recon, ReconInterestProvider, Response, Store,
};
pub use client::{Client, Server};

pub use sha256a::Sha256a;

mod client;
pub mod libp2p;
mod recon;
mod sha256a;
