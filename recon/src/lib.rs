//! Recon is a network protocol for set reconciliation
#![warn(missing_docs, missing_debug_implementations, clippy::all)]

pub use crate::recon::{btree::BTreeStore, AssociativeHash, Key, Message, Recon, Response, Store};
pub use sha256a::Sha256a;

pub mod libp2p;
mod recon;
mod sha256a;
