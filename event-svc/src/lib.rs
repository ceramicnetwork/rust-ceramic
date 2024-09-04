//! The Event Service provides an API for ingesting and querying Ceramic Events.
#![warn(missing_docs)]

mod error;
mod event;
pub mod store;
#[cfg(test)]
mod tests;

pub use error::Error;
pub use event::{BlockStore, CeramicEventService};

pub(crate) type Result<T> = std::result::Result<T, Error>;
