mod error;
pub mod sqlite;

pub use error::Error;

pub type Result<T> = std::result::Result<T, Error>;
