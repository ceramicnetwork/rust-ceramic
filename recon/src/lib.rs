//! Recon is a network protocol for set reconciliation
#![warn(missing_docs)]

pub use crate::recon::{Hash, Message, Recon};
pub use ahash::AHash;
#[cfg(test)]
pub use recon::tests;

mod ahash;
pub mod libp2p;
mod recon;

#[cfg(test)]
#[macro_use]
extern crate lalrpop_util;
