mod error;
mod event;
mod interest;
#[cfg(test)]
mod tests;

pub use error::Error;
pub use event::CeramicEventService;
pub use interest::CeramicInterestService;

pub(crate) type Result<T> = std::result::Result<T, Error>;
