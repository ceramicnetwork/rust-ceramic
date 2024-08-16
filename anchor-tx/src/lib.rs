//! A crate for delegating anchoring to a remote CAS.
#![warn(missing_docs)]
mod cas_mock;
mod cas_remote;

pub use cas_remote::RemoteCas;
