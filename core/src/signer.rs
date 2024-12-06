//! Define a trait [`Signer`] that provides a synchronous API for signing data.
use ssi::jwk::Algorithm;

use crate::DidDocument;

/// Sign bytes for an id and algorithm
pub trait Signer {
    /// Algorithm used by signer
    fn algorithm(&self) -> Algorithm;
    /// Id of signer
    fn id(&self) -> &DidDocument;
    /// Sign bytes
    fn sign_bytes(&self, bytes: &[u8]) -> anyhow::Result<Vec<u8>>;
    /// Sign payload returning compact JWS string
    fn sign_jws(&self, payload: &str) -> anyhow::Result<String>;
}

impl<S: Signer + Sync> Signer for &'_ S {
    fn algorithm(&self) -> Algorithm {
        (*self).algorithm()
    }
    fn id(&self) -> &DidDocument {
        (*self).id()
    }
    fn sign_bytes(&self, bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
        (*self).sign_bytes(bytes)
    }
    fn sign_jws(&self, payload: &str) -> anyhow::Result<String> {
        (*self).sign_jws(payload)
    }
}
impl<S: Signer + ?Sized> Signer for Box<S> {
    fn algorithm(&self) -> Algorithm {
        self.as_ref().algorithm()
    }
    fn id(&self) -> &DidDocument {
        self.as_ref().id()
    }
    fn sign_bytes(&self, bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
        self.as_ref().sign_bytes(bytes)
    }
    fn sign_jws(&self, payload: &str) -> anyhow::Result<String> {
        self.as_ref().sign_jws(payload)
    }
}
