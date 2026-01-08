//! This crate include all the machinery needed for building Merkle Trees from Anchor Requests and then the Time Events
//! corresponding to those requests once the root of the tree has been anchored.
#![warn(missing_docs)]
mod anchor;
mod anchor_batch;
mod cas_mock;
mod high_water_mark_store;
mod merkle_tree;
mod time_event;
mod transaction_manager;

pub use anchor::{AnchorRequest, MerkleNode, MerkleNodes, TimeEventBatch, TimeEventInsertable};
pub use anchor_batch::{AnchorService, Store};
pub use cas_mock::{MockAnchorEventService, MockCas};
pub use transaction_manager::{ChainInclusionData, DetachedTimeEvent, RootTimeEvent, TransactionManager};
