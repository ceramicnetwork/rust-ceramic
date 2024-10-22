//! The Event Service provides an API for ingesting and querying Ceramic Events.
#![warn(missing_docs)]

mod error;
mod event;
pub mod store;
#[cfg(test)]
mod tests;

pub use ceramic_validation::eth_rpc;
pub use error::Error;
pub use event::ChainInclusionProvider;
pub use event::{BlockStore, EventService};

pub(crate) type Result<T> = std::result::Result<T, Error>;
