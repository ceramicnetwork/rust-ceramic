use crate::DidDocument;
use anyhow::bail;
use cid::multibase;
use once_cell::sync::Lazy;
use ssi::did::{Resource, VerificationMethod};
use ssi::did_resolve::{
    dereference, Content, DIDResolver, DereferencingInputMetadata, ResolutionInputMetadata,
};
use ssi::jwk::{Base64urlUInt, OctetParams, Params, JWK};
use std::ops::Deref;

static DID_TYPE_REGEX: Lazy<regex::Regex> =
    Lazy::new(|| regex::Regex::new(r#"did:(?P<T>[^:]+):(?P<K>[A-Za-z0-9:]+)"#).unwrap());

/// Newtype around JWK to make it easier to construct from a DID and provide a private key
#[derive(Clone, Debug)]
pub struct Jwk(JWK);

const DID_KEY: &str = "key";
const DID_PKH: &str = "pkh";

impl Jwk {
    /// Create a new JWK from a DID
    pub async fn new(did: &DidDocument) -> anyhow::Result<Self> {
        let cap = DID_TYPE_REGEX.captures(&did.id);
        let mut jwk = if let Some(cap) = cap {
            match &cap["T"] {
                DID_KEY => {
                    if let Some(jwk) = did_as_jwk(&did_method_key::DIDKey, &did.id).await? {
                        jwk
                    } else {
                        pk_as_jwk(&cap["K"])?
                    }
                }
                DID_PKH => {
                    if let Some(jwk) = did_as_jwk(&did_pkh::DIDPKH, &did.id).await? {
                        jwk
                    } else {
                        anyhow::bail!("Failed to get jwk for {}", did.id)
                    }
                }
                _ => {
                    unimplemented!()
                }
            }
        } else {
            anyhow::bail!("Invalid DID")
        };
        jwk.key_id = Some(did.id.clone());
        Ok(Self(jwk))
    }

    /// Add a private key to the JWK
    pub fn with_private_key(self, pk: &str) -> anyhow::Result<Self> {
        let pk = hex::decode(pk)?;
        let params = match self.0.params.clone() {
            Params::OKP(mut op) => {
                op.private_key = Some(Base64urlUInt(pk));
                Params::OKP(op)
            }
            Params::EC(mut ec) => {
                ec.ecc_private_key = Some(Base64urlUInt(pk));
                Params::EC(ec)
            }
            _ => anyhow::bail!("Unsupported JWK Params"),
        };
        let mut jwk = JWK::from(params);
        jwk.key_id.clone_from(&self.key_id);
        Ok(Self(jwk))
    }

    /// Resolve a DID and return the DIDDocument
    ///
    /// The DID Document contains the public key. For more info, see:
    /// https://www.w3.org/TR/did-core/#dfn-did-documents
    pub async fn resolve_did(
        did: &str,
        metadata: &ResolutionInputMetadata,
    ) -> anyhow::Result<DidDocument> {
        let base_did = did.split_once('#').map_or(did, |(b, _)| b);

        let resolver: &dyn DIDResolver = if base_did.starts_with("did:key:") {
            &did_method_key::DIDKey
        } else if base_did.starts_with("did:pkh:") {
            &did_pkh::DIDPKH
        } else {
            bail!("unknown DID method not 'key' or 'pkh'");
        };

        let (res, did_document, _metadata) = resolver.resolve(base_did, metadata).await;
        if let Some(err) = res.error {
            bail!("error resolving did: {}", err)
        }
        did_document.ok_or_else(|| anyhow::anyhow!("no did document found"))
    }
}

impl From<JWK> for Jwk {
    fn from(value: JWK) -> Self {
        Self(value)
    }
}

impl Deref for Jwk {
    type Target = JWK;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

async fn did_as_jwk(resolver: &dyn DIDResolver, id: &str) -> anyhow::Result<Option<JWK>> {
    let (res, object, _) = dereference(resolver, id, &DereferencingInputMetadata::default()).await;
    if res.error.is_none() {
        match object {
            Content::Object(Resource::VerificationMethod(vm)) => {
                let jwk = vm.get_jwk()?;
                return Ok(Some(jwk));
            }
            Content::DIDDocument(doc) => {
                if let Some(vms) = &doc.verification_method {
                    for vm in vms {
                        if let VerificationMethod::Map(vm) = vm {
                            if let Ok(jwk) = vm.get_jwk() {
                                return Ok(Some(jwk));
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(None)
}

fn pk_as_jwk(public_key: &str) -> anyhow::Result<JWK> {
    let (_, data) = multibase::decode(public_key)?;
    let curve = match data[0..2] {
        [0xed, 0x01] => "Ed25519",
        [0xe7, 0x01] => "SECP256K1",
        _ => anyhow::bail!("Unknown encoding prefix"),
    };
    let mut jwk = JWK::from(Params::OKP(OctetParams {
        curve: curve.to_string(),
        public_key: Base64urlUInt(data[2..].to_vec()),
        private_key: None,
    }));
    jwk.key_id = Some(public_key.to_string());
    Ok(jwk)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ssi::did::DIDMethod;
    use ssi::did::VerificationMethod;
    use test_pretty_log::test;

    #[test(tokio::test)]
    async fn should_convert_did_key_generated_without_vm() {
        let jwk = JWK::generate_ed25519().unwrap();
        let did = did_method_key::DIDKey
            .generate(&ssi::did::Source::Key(&jwk))
            .unwrap();
        let did = DidDocument::new(&did);
        let other_jwk = Jwk::new(&did).await.unwrap();
        if let (Params::OKP(did), Params::OKP(orig)) = (&other_jwk.params, jwk.params) {
            assert_eq!(did.public_key, orig.public_key)
        }
    }

    #[test(tokio::test)]
    async fn should_convert_did_key_generated_with_vm() {
        let jwk = ssi::jwk::JWK::generate_ed25519().unwrap();
        let did = did_method_key::DIDKey
            .generate(&ssi::did::Source::Key(&jwk))
            .unwrap();
        let pko = ssi::did::VerificationMethodMap {
            public_key_jwk: Some(jwk.clone()),
            ..Default::default()
        };
        let did = ssi::did::DocumentBuilder::default()
            .id(did)
            .verification_method(vec![VerificationMethod::Map(pko)])
            .build()
            .unwrap();
        let other_jwk = Jwk::new(&did).await.unwrap();
        if let (Params::OKP(did), Params::OKP(orig)) = (&other_jwk.params, jwk.params) {
            assert_eq!(did.public_key, orig.public_key)
        } else {
            panic!("Was not OKP");
        }
    }

    #[test(tokio::test)]
    async fn should_convert_did_key_with_vm() {
        let did = DidDocument::new("did:key:zQ3shokFTS3brHcDQrn82RUDfCZESWL1ZdCEJwekUDPQiYBme#zQ3shokFTS3brHcDQrn82RUDfCZESWL1ZdCEJwekUDPQiYBme");
        let jwk = Jwk::new(&did).await.unwrap();
        tracing::debug!("JWK={:?}", jwk);
        if let Params::EC(params) = &jwk.params {
            let crv = params.curve.clone().unwrap_or_default();
            tracing::debug!("EC={}", crv);
        } else {
            panic!("Was not EC");
        }
    }

    #[test(tokio::test)]
    async fn should_fail_to_convert_did_pkh_with_vm() {
        let did = DidDocument::new("did:pkh:eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a");
        if Jwk::new(&did).await.is_ok() {
            panic!("Should not get JWK from document");
        }
    }
}
