use crate::{Base64String, Base64UrlString, DidDocument, Jwk, MultiBase32String};
use cid::Cid;
use serde::{Deserialize, Serialize};
use ssi::jwk::Algorithm;

/// The fields associated with the signature used to sign a JWS
#[derive(Debug, Serialize, Deserialize)]
pub struct JwsSignature {
    pub protected: Option<Base64String>,
    pub signature: Base64UrlString,
}

/// A JWS object
#[derive(Debug, Serialize, Deserialize)]
pub struct Jws {
    /// Link to CID that contains encoded data
    pub link: MultiBase32String,
    /// CID of the encoded data
    pub payload: Base64UrlString,
    /// The signatures of the JWS
    pub signatures: Vec<JwsSignature>,
}

impl Jws {
    /// Creates a new JWS object
    pub fn new(jwk: &Jwk, signer: &DidDocument, cid: &Cid) -> anyhow::Result<Self> {
        let alg = Algorithm::EdDSA;
        let header = ssi::jws::Header {
            algorithm: alg,
            type_: Some("JWT".to_string()),
            key_id: Some(signer.id.clone()),
            ..Default::default()
        };
        let cid_str = Base64UrlString::from_cid(cid);
        // creates compact signature of protected.signature
        let header_str = Base64String::from(serde_json::to_vec(&header)?);
        let signing_input = format!("{}.{}", header_str.as_ref(), cid_str.as_ref());
        let signed = ssi::jws::sign_bytes_b64(header.algorithm, signing_input.as_bytes(), jwk)?;
        let link = MultiBase32String::try_from(cid)?;
        Ok(Self {
            payload: cid_str,
            link,
            signatures: vec![JwsSignature {
                protected: Some(header_str),
                signature: Base64UrlString::from(signed),
            }],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DagCborEncoded, DidDocument};
    use cid::multihash::{Code, MultihashDigest};
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
        let jwk = Jwk::new(&did).await.unwrap();
        let jwk = jwk.with_private_key(&private_key).unwrap();
        let data = "some data";
        let linked_block = DagCborEncoded::new(&data).unwrap();
        let cid = Cid::new_v1(0x71, Code::Sha2_256.digest(linked_block.as_ref()));
        let jws = Jws::new(&jwk, &did, &cid).unwrap();
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
