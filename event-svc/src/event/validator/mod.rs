// TODO: remove once this is actually used
#[allow(dead_code)]
mod event;
// this is not used yet
// we should export the following and consume in the event validator
// pub use time::{BlockchainVerifier, EthRpcProvider, EventTimestamper, Timestamp};
#[allow(dead_code)]
mod time;

pub use event::{EventValidator, UnvalidatedEvent, ValidatedEvent, ValidatedEvents};
