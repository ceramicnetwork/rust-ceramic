//! The Event Service provides an API for ingesting and querying Ceramic Events.
#![warn(missing_docs)]

mod blockchain;
mod error;
mod event;
pub mod store;
#[cfg(test)]
mod tests;

pub use blockchain::eth_rpc;
pub use error::Error;
pub use event::ChainInclusionProvider;
pub use event::{BlockStore, EventService, UndeliveredEventReview};

pub(crate) type Result<T> = std::result::Result<T, Error>;
