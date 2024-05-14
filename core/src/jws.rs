use crate::{Base64String, Base64UrlString, MultiBase32String, Signer};
use cid::Cid;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str::FromStr;

/// The fields associated with the signature used to sign a JWS
#[derive(Debug, Serialize, Deserialize)]
pub struct JwsSignature {
    /// Protected header
    pub protected: Option<Base64String>,
    /// Signature
    pub signature: Base64UrlString,
}

/// Builder used to create JWS
pub struct JwsBuilder<S> {
    signer: S,
    additional: BTreeMap<String, serde_json::Value>,
}

impl<S: Signer> JwsBuilder<S> {
    pub fn new(signer: S) -> Self {
        Self {
            signer,
            additional: BTreeMap::new(),
        }
    }

    pub fn with_additional(mut self, key: String, value: serde_json::Value) -> Self {
        self.additional.insert(key, value);
        self
    }

    pub fn replace_additional(mut self, additional: BTreeMap<String, serde_json::Value>) -> Self {
        self.additional = additional;
        self
    }

    pub async fn build_for_cid(self, cid: &Cid) -> anyhow::Result<Jws> {
        let cid_str = Base64UrlString::from_cid(cid);
        let link = MultiBase32String::try_from(cid)?;
        Jws::new(&self.signer, cid_str, Some(link), self.additional).await
    }

    pub async fn build_for_data<T: Serialize>(self, input: &T) -> anyhow::Result<Jws> {
        let input = serde_json::to_vec(input)?;
        let input = Base64UrlString::from(input);
        Jws::new(&self.signer, input, None, self.additional).await
    }
}

/// A JWS object
#[derive(Debug, Serialize, Deserialize)]
pub struct Jws {
    /// Link to CID that contains encoded data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link: Option<MultiBase32String>,
    /// Encoded data
    pub payload: Base64UrlString,
    /// The signatures of the JWS
    pub signatures: Vec<JwsSignature>,
}

impl Jws {
    /// Create a builder for Jws objects
    pub fn builder<S: Signer>(signer: S) -> JwsBuilder<S> {
        JwsBuilder::new(signer)
    }

    /// Creates a new JWS from a payload that has already been serialized to Base64UrlString
    pub async fn new(
        signer: &impl Signer,
        input: Base64UrlString,
        link: Option<MultiBase32String>,
        additional_parameters: BTreeMap<String, serde_json::Value>,
    ) -> anyhow::Result<Self> {
        let alg = signer.algorithm();
        let header = ssi::jws::Header {
            algorithm: alg,
            type_: Some("JWT".to_string()),
            key_id: Some(signer.id().id.clone()),
            additional_parameters,
            ..Default::default()
        };
        // creates compact signature of protected.signature
        let header_str = Base64String::from(serde_json::to_vec(&header)?);
        let signing_input = format!("{}.{}", header_str.as_ref(), input.as_ref());
        let signed = signer.sign(signing_input.as_bytes()).await?;
        Ok(Self {
            link,
            payload: input,
            signatures: vec![JwsSignature {
                protected: Some(header_str),
                signature: signed,
            }],
        })
    }

    /// Get the payload of this jws
    pub fn payload(&self) -> &Base64UrlString {
        &self.payload
    }

    /// Get the additional parameters of the jws signature
    pub fn additional(&self) -> anyhow::Result<BTreeMap<String, serde_json::Value>> {
        let first = self
            .signatures
            .first()
            .ok_or_else(|| anyhow::anyhow!("No signatures"))?;
        let protected = first
            .protected
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No protected header"))?;
        let protected = serde_json::from_slice::<ssi::jws::Header>(&protected.to_vec()?)?;
        Ok(protected.additional_parameters)
    }

    /// Get the capability field for this jws
    pub fn capability(&self) -> anyhow::Result<Cid> {
        let additional = self.additional()?;
        let cap = additional
            .get("cap")
            .ok_or_else(|| anyhow::anyhow!("No cap"))?
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("cap is not a string"))?;
        let cid = Cid::from_str(cap)?;
        Ok(cid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DagCborEncoded, DidDocument, Jwk, JwkSigner};
    use cid::Cid;
    use multihash_codetable::Code;
    use multihash_codetable::MultihashDigest;
    use ssi::did::DIDMethod;
    use ssi::did::Source;
    use ssi::jwk::Params;

    #[tokio::test]
    async fn should_produce_verifiable_signatures() {
        let key = ssi::jwk::JWK::generate_ed25519().unwrap();
        let private_key = if let Params::OKP(params) = &key.params {
            let pk = params.private_key.as_ref().unwrap();
            hex::encode(pk.0.as_slice())
        } else {
            panic!("Invalid key generated");
        };
        let did = did_method_key::DIDKey.generate(&Source::Key(&key)).unwrap();
        let did = DidDocument::new(&did);
        let signer = JwkSigner::new(did.clone(), &private_key).await.unwrap();
        let data = "some data";
        let linked_block = DagCborEncoded::new(&data).unwrap();
        let cid = Cid::new_v1(0x71, Code::Sha2_256.digest(linked_block.as_ref()));
        let jws = Jws::builder(&signer).build_for_cid(&cid).await.unwrap();
        let compact = format!(
            "{}.{}.{}",
            jws.signatures[0].protected.as_ref().unwrap().as_ref(),
            jws.payload.as_ref(),
            jws.signatures[0].signature.as_ref(),
        );
        let jwk = Jwk::new(&did).await.unwrap();
        let (_, payload) = ssi::jws::decode_verify(&compact, &jwk).unwrap();
        assert_eq!(
            Base64UrlString::from(payload).as_ref(),
            Base64UrlString::from_cid(&cid).as_ref()
        );
    }
}
