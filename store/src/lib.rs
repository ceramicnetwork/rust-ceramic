//! A unified implementation of both the [`recon::Store`] and [`iroh_bitswap::Store`] traits.
//! This unified implementation allows for exposing Recon values as IPFS blocks
#![warn(missing_docs)]

mod sqlite;
#[cfg(test)]
mod tests;

pub use sqlite::{DbTx, InterestStore, ModelStore, SqlitePool};
