//! Unvalidated signed events.
use crate::bytes::Bytes;
use crate::unvalidated::Payload;
use base64::Engine;
use ceramic_core::{DidDocument, Jwk};
use cid::Cid;
use ipld_core::codec::Codec;
use ipld_core::ipld::Ipld;
use iroh_car::{CarHeader, CarWriter};
use multihash_codetable::{Code, MultihashDigest};
use serde::{Deserialize, Serialize};
use serde_ipld_dagcbor::codec::DagCborCodec;
use ssi::jwk::Algorithm;
use std::collections::BTreeMap;

/// Materialized signed Event.
pub struct Event<D> {
    envelope: Envelope,
    payload: Payload<D>,
    envelope_cid: Cid,
    payload_cid: Cid,
}

impl<D: serde::Serialize> Event<D> {
    fn cid_from_dag_cbor(data: &[u8]) -> Cid {
        Cid::new_v1(
            <DagCborCodec as Codec<Ipld>>::CODE,
            Code::Sha2_256.digest(data),
        )
    }

    fn cid_from_dag_jose(data: &[u8]) -> Cid {
        Cid::new_v1(
            0x85, // TODO use constant for DagJose codec
            Code::Sha2_256.digest(data),
        )
    }

    /// Constructs a signed event by signing a given event payload.
    pub fn from_payload(payload: Payload<D>, signer: impl Signer) -> anyhow::Result<Self> {
        let payload_cid = Self::cid_from_dag_cbor(&serde_ipld_dagcbor::to_vec(&payload)?);
        let payload_cid_str =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&payload_cid.to_bytes());

        let alg = signer.algorithm();
        let header = ssi::jws::Header {
            algorithm: alg,
            key_id: Some(signer.id().id.clone()),
            ..Default::default()
        };
        // creates compact signature of protected.signature
        let header_bytes = serde_json::to_vec(&header)?;
        let header_str = base64::engine::general_purpose::STANDARD_NO_PAD.encode(&header_bytes);
        let signing_input = format!("{}.{}", header_str, payload_cid_str);
        let signed = signer.sign(signing_input.as_bytes())?;

        let envelope = Envelope {
            payload: payload_cid.to_bytes().into(),
            signatures: vec![Signature {
                header: None,
                protected: Some(header_bytes.into()),
                signature: signed.into(),
            }],
        };

        let envelope_cid = Self::cid_from_dag_jose(&serde_ipld_dagcbor::to_vec(&envelope)?);

        Ok(Self {
            payload,
            envelope,
            envelope_cid,
            payload_cid,
        })
    }

    /// Encodes the signature envelope as IPLD
    pub fn encode_envelope(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_ipld_dagcbor::to_vec(&self.envelope)?)
    }

    /// Encodes the payload as IPLD
    pub fn encode_payload(&self) -> anyhow::Result<Vec<u8>> {
        Ok(serde_ipld_dagcbor::to_vec(&self.payload)?)
    }

    /// Get the CID of the signature envelope
    pub fn envelope_cid(&self) -> Cid {
        self.envelope_cid
    }

    /// Get the CID of the payload
    pub fn payload_cid(&self) -> Cid {
        self.payload_cid
    }

    /// Encodes the full signed event into a CAR file.
    pub async fn encode_car(&self) -> anyhow::Result<Vec<u8>> {
        let envelope_bytes = self.encode_envelope()?;
        let payload_bytes = self.encode_payload()?;

        let mut car = Vec::new();
        let roots: Vec<Cid> = vec![self.envelope_cid];
        let mut writer = CarWriter::new(CarHeader::V1(roots.into()), &mut car);
        writer.write(self.payload_cid, payload_bytes).await?;
        writer.write(self.envelope_cid, envelope_bytes).await?;
        writer.finish().await?;

        Ok(car)
    }

    /// Accessor for the envelope and payload.
    pub fn into_parts(self) -> (Envelope, Payload<D>) {
        (self.envelope, self.payload)
    }
}

/// A signed event envelope.
#[derive(Debug, Serialize, Deserialize)]
pub struct Envelope {
    payload: Bytes,
    signatures: Vec<Signature>,
}

impl Envelope {
    /// Report the link of this signed event
    pub fn link(&self) -> Option<Cid> {
        // Parse payload as cid bytes
        Cid::read_bytes(self.payload.as_slice()).ok()
    }

    /// Report the cap field of the protected header if present
    pub fn cap(&self) -> Option<Cid> {
        // Parse signatures[0].protected for cad
        self.signatures[0].protected.as_ref().and_then(|protected| {
            serde_json::from_slice::<Protected>(protected.as_slice())
                .ok()
                .and_then(|protected| protected.cap)
        })
    }
}

/// A signature part of a JSON Web Signature.
#[derive(Debug, Serialize, Deserialize)]
pub struct Signature {
    /// The optional unprotected header.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header: Option<BTreeMap<String, Ipld>>,
    /// The protected header as a JSON object
    pub protected: Option<Bytes>,
    /// The web signature
    pub signature: Bytes,
}

#[derive(Debug, Serialize, Deserialize)]
struct Protected {
    // There are more field in this struct be we only care about cap so far.
    cap: Option<Cid>,
}

/// Sign bytes for an id and algorithm
pub trait Signer {
    /// Algorithm used by signer
    fn algorithm(&self) -> Algorithm;
    /// Id of signer
    fn id(&self) -> &DidDocument;
    /// Sign bytes
    fn sign(&self, bytes: &[u8]) -> anyhow::Result<Vec<u8>>;
}

impl<'a, S: Signer + Sync> Signer for &'a S {
    fn algorithm(&self) -> Algorithm {
        (*self).algorithm()
    }
    fn id(&self) -> &DidDocument {
        (*self).id()
    }
    fn sign(&self, bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
        (*self).sign(bytes)
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
    /// TODO: DidDocument should be generated from private key.
    pub async fn new(did: DidDocument, pk: &str) -> anyhow::Result<Self> {
        let jwk = Jwk::new(&did).await?;
        Ok(Self {
            did,
            jwk: jwk.with_private_key(pk)?,
        })
    }
}

impl Signer for JwkSigner {
    fn algorithm(&self) -> Algorithm {
        Algorithm::EdDSA
    }

    fn id(&self) -> &DidDocument {
        &self.did
    }

    fn sign(&self, bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
        Ok(ssi::jws::sign_bytes(self.algorithm(), bytes, &self.jwk)?)
    }
}
