#![deny(warnings, missing_docs, missing_debug_implementations, clippy::all)]

//! Recon is a network protocol for set reconciliation

pub use crate::recon::{Hash, Message, Recon};
pub use ahash::AHash;
#[cfg(test)]
pub use recon::tests;

mod ahash;
mod recon;

#[cfg(test)]
#[macro_use]
extern crate lalrpop_util;
