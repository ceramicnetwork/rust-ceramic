use crate::{Base64String, Base64UrlString, MultiBase32String, Signer};
use cid::Cid;
use serde::{Deserialize, Serialize};

/// The fields associated with the signature used to sign a JWS
#[derive(Debug, Serialize, Deserialize)]
pub struct JwsSignature {
    /// Protected header
    pub protected: Option<Base64String>,
    /// Signature
    pub signature: Base64UrlString,
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
    /// Creates a new JWS object
    pub async fn for_data<T: Serialize>(signer: &impl Signer, input: &T) -> anyhow::Result<Self> {
        let input = serde_json::to_vec(input)?;
        let input = Base64UrlString::from(input);
        Jws::new(signer, input, None).await
    }

    /// Creates a new JWS object for a cid
    pub async fn for_cid(signer: &impl Signer, cid: &Cid) -> anyhow::Result<Self> {
        let cid_str = Base64UrlString::from_cid(cid);
        let link = MultiBase32String::try_from(cid)?;
        Jws::new(signer, cid_str, Some(link)).await
    }

    /// Creates a new JWS from a payload that has already been serialized to Base64UrlString
    pub async fn new(
        signer: &impl Signer,
        input: Base64UrlString,
        link: Option<MultiBase32String>,
    ) -> anyhow::Result<Self> {
        let alg = signer.algorithm();
        let header = ssi::jws::Header {
            algorithm: alg,
            type_: Some("JWT".to_string()),
            key_id: Some(signer.id().id.clone()),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DagCborEncoded, DidDocument, Jwk, JwkSigner};
    use cid::multihash::{Code, MultihashDigest};
    use cid::Cid;
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
        let jws = Jws::for_cid(&signer, &cid).await.unwrap();
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
