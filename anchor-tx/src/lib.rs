//! This crate provides a interface for interacting with a remote anchoring system (whether legacy CAS or a blockchain)
#![warn(missing_docs)]
mod cas_mock;
mod cas_remote;

pub use cas_remote::RemoteCas;
