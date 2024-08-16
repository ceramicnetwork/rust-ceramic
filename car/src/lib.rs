//! Implementation of the [car](https://ipld.io/specs/transport/car/) format.

mod error;
mod header;
mod reader;
/// Synchronous version of the same API.
/// Useful if all data already exists in memory.
pub mod sync;
mod util;
mod writer;

pub use crate::error::Error;
pub use crate::header::{CarHeader, CarHeaderV1};
pub use crate::reader::CarReader;
pub use crate::writer::CarWriter;
