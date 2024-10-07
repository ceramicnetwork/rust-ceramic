//! This crate supports verifying CACAOs with different signing methods, event signatures,
//! as well as interacting with blockchains for time event validation.
#![warn(missing_docs)]

/// Types and traits to interact with blockchains used for time event validation
pub mod blockchain;
mod signature;
mod siwx_message;
#[cfg(test)]
mod test;
mod verifier;

pub use blockchain::{eth_rpc, hoku};
pub use ceramic_event::unvalidated::signed::cacao;
pub use verifier::{cacao_verifier, event_verifier, AtTime, VerifyCacaoOpts, VerifyJwsOpts};
