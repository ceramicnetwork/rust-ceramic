//! The Event Service provides an API for ingesting and querying Ceramic Events.
#![warn(missing_docs)]

mod error;
mod service;
pub mod store;
#[cfg(test)]
mod tests;

pub use error::Error;
pub use service::PeerService;
