//! Implementation of the [car](https://ipld.io/specs/transport/car/) format.

mod reader;
mod util;
mod writer;

pub use crate::error::Error;
pub use crate::header::{CarHeader, CarHeaderV1};
pub use reader::CarReader;
pub use writer::CarWriter;
