//! This crate include all the machinery needed for building Merkle Trees from Anchor Requests and then the Time Events
//! corresponding to those requests once the root of the tree has been anchored.
#![warn(missing_docs)]
mod anchor;
mod anchor_batch;
mod merkle_tree;
mod time_event;
mod transaction_manager;

pub use anchor_batch::{AnchorService, Store};
pub use transaction_manager::{DetachedTimeEvent, Receipt, TransactionManager};
