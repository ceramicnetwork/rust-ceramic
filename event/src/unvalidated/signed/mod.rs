//! Unvalidated signed events.
pub mod cacao;

use std::fmt::Debug;

use crate::bytes::Bytes;
use crate::unvalidated::Payload;
use base64::Engine;
use ceramic_car::sync::{CarHeader, CarWriter};
use ceramic_core::{DidDocument, Jwk};
use cid::Cid;
use ipld_core::ipld::Ipld;
use serde::{Deserialize, Serialize};
use ssi::jwk::Algorithm;
use std::{collections::BTreeMap, str::FromStr as _};

use self::cacao::Capability;

use super::{cid_from_dag_cbor, cid_from_dag_jose};

/// Materialized signed Event.
pub struct Event<D> {
    envelope: Envelope,
    envelope_cid: Cid,
    payload: Payload<D>,
    payload_cid: Cid,
    capability: Option<(Cid, Capability)>,
}

impl<D: Debug> Debug for Event<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Event")
            .field("envelope", &self.envelope)
            .field("envelope_cid", &self.envelope_cid.to_string())
            .field("payload", &self.payload)
            .field("payload_cid", &self.payload_cid.to_string())
            .field(
                "capability",
                &self
                    .capability
                    .as_ref()
                    .map(|(cid, cap)| (cid.to_string(), cap)),
            )
            .finish()
    }
}

impl<D: serde::Serialize> Event<D> {
    /// Factory for building an Event.
    pub fn new(
        envelope_cid: Cid,
        envelope: Envelope,
        payload_cid: Cid,
        payload: Payload<D>,
        capability: Option<(Cid, Capability)>,
    ) -> Self {
        Self {
            envelope_cid,
            envelope,
            payload_cid,
            payload,
            capability,
        }
    }

    /// Get the Payload
    pub fn payload(&self) -> &Payload<D> {
        &self.payload
    }

    /// Constructs a signed event by signing a given event payload.
    pub fn from_payload(payload: Payload<D>, signer: impl Signer) -> anyhow::Result<Self> {
        let payload_cid = cid_from_dag_cbor(&serde_ipld_dagcbor::to_vec(&payload)?);
        let payload_cid_str =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload_cid.to_bytes());

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

        let envelope_cid = cid_from_dag_jose(&serde_ipld_dagcbor::to_vec(&envelope)?);

        Ok(Self {
            payload,
            envelope,
            envelope_cid,
            payload_cid,
            //TODO: Implement CACAO signing
            capability: None,
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
    /// Encodes the capability as IPLD if present
    pub fn encode_capability(&self) -> anyhow::Result<Option<(Cid, Vec<u8>)>> {
        self.capability
            .as_ref()
            .map(|(cid, cacao)| Ok((*cid, serde_ipld_dagcbor::to_vec(cacao)?)))
            .transpose()
    }

    /// Get the CID of the signature envelope
    pub fn envelope_cid(&self) -> Cid {
        self.envelope_cid
    }

    /// Get the CID of the payload
    pub fn payload_cid(&self) -> Cid {
        self.payload_cid
    }

    /// Get the CID of the capability
    pub fn capability_cid(&self) -> Option<Cid> {
        self.capability.as_ref().map(|c| c.0)
    }

    /// Encodes the full signed event into a CAR file.
    pub fn encode_car(&self) -> anyhow::Result<Vec<u8>> {
        let envelope_bytes = self.encode_envelope()?;
        let payload_bytes = self.encode_payload()?;
        let capability_bytes = self.encode_capability()?;

        let mut car = Vec::new();
        let roots: Vec<Cid> = vec![self.envelope_cid];
        let mut writer = CarWriter::new(CarHeader::V1(roots.into()), &mut car);
        if let Some((cid, bytes)) = capability_bytes {
            writer.write(cid, &bytes)?;
        }
        writer.write(self.payload_cid, payload_bytes)?;
        writer.write(self.envelope_cid, envelope_bytes)?;
        writer.finish()?;

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

    /// Report the capability from the protected header if present
    pub fn capability(&self) -> Option<Cid> {
        // Parse signatures[0].protected for cad
        self.signatures[0].protected.as_ref().and_then(|protected| {
            serde_json::from_slice::<Protected>(protected.as_slice())
                .ok()
                .and_then(|protected| {
                    protected.cap.and_then(|cid| {
                        cid.strip_prefix("ipfs://")
                            .and_then(|cid| Cid::from_str(cid).ok())
                    })
                })
        })
    }
}

/// A signature part of a JSON Web Signature.
#[derive(Debug, Serialize, Deserialize)]
pub struct Signature {
    /// The optional unprotected header.
    #[serde(skip_serializing_if = "Option::is_none")]
    header: Option<BTreeMap<String, Ipld>>,
    /// The protected header as a JSON object
    protected: Option<Bytes>,
    /// The web signature
    signature: Bytes,
}

#[derive(Debug, Serialize, Deserialize)]
struct Protected {
    // There are more field in this struct be we only care about cap so far.
    cap: Option<String>,
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
