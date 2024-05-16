use crate::bytes::Bytes;
use cid::Cid;
use ipld_core::ipld::Ipld;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A signed event payload.
/// TODO: What is the relationship between this and SignedEvent?
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
