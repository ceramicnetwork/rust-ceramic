use crate::{Base64UrlString, DidDocument, Jwk};
use ssi::jwk::Algorithm;

/// Sign bytes for an id and algorithm
#[async_trait::async_trait]
pub trait Signer {
    /// Algorithm used by signer
    fn algorithm(&self) -> Algorithm;
    /// Id of signer
    fn id(&self) -> &DidDocument;
    /// Sign bytes
    async fn sign(&self, bytes: &[u8]) -> anyhow::Result<Base64UrlString>;
}

#[async_trait::async_trait]
impl<'a, S: Signer + Sync> Signer for &'a S {
    fn algorithm(&self) -> Algorithm {
        (*self).algorithm()
    }

    fn id(&self) -> &DidDocument {
        (*self).id()
    }

    async fn sign(&self, bytes: &[u8]) -> anyhow::Result<Base64UrlString> {
        (*self).sign(bytes).await
    }
}

/// Did and jwk based signer
#[derive(Clone, Debug)]
pub struct JwkSigner {
    did: DidDocument,
    jwk: Jwk,
}

impl JwkSigner {
    /// Create a new signer from a did and private key
    pub async fn new(did: DidDocument, pk: &str) -> anyhow::Result<Self> {
        let jwk = Jwk::new(&did).await?;
        Ok(Self {
            did,
            jwk: jwk.with_private_key(pk)?,
        })
    }
}

#[async_trait::async_trait]
impl Signer for JwkSigner {
    fn algorithm(&self) -> Algorithm {
        Algorithm::EdDSA
    }

    fn id(&self) -> &DidDocument {
        &self.did
    }

    async fn sign(&self, bytes: &[u8]) -> anyhow::Result<Base64UrlString> {
        let signed = ssi::jws::sign_bytes_b64(self.algorithm(), bytes, &self.jwk)?;
        Ok(Base64UrlString::from(signed))
    }
}
