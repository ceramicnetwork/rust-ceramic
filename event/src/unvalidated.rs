//! Types of raw unvalidated Ceramic Events

mod bytes;

use bytes::Bytes;

use std::collections::BTreeMap;

use cid::Cid;
use libipld::Ipld;
use serde::{Deserialize, Serialize};

/// Ceramic Event
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Event {
    /// Signed event in a stream
    Signed(JWS),
    /// Time event in a stream
    Time(TimeEvent),
}

/// Signed event
#[derive(Debug, Serialize, Deserialize)]
pub struct JWS {
    payload: Bytes,
    signatures: Vec<Signature>,
}

impl JWS {
    /// Report the link of this signed event
    pub fn link(&self) -> Option<Cid> {
        // Parse payload as cid bytes
        Cid::read_bytes(self.payload.as_slice()).ok()
    }
    /// Report the cap field of the protected header if present
    pub fn cap(&self) -> Option<Cid> {
        // Parse signatures[0].protected for cad
        self.signatures[0]
            .protected
            .as_ref()
            .map(|protected| {
                serde_json::from_slice::<Protected>(protected.as_slice())
                    .ok()
                    .map(|protected| protected.cap)
                    .flatten()
            })
            .flatten()
    }
}

/// Payload of a signed event
#[derive(Debug, Serialize, Deserialize)]
// Note untagged variants a deserialized in order and the first one that succeeds is returned.
// Therefore the order of the variants is important to be most specific to least specific
#[serde(untagged)]
pub enum Payload {
    /// Data event
    Data(DataPayload),
    /// Init event
    Init(InitPayload),
}

/// Payload of an init event
#[derive(Debug, Serialize, Deserialize)]
pub struct InitPayload {
    header: InitHeader,
    data: Option<Ipld>,
}

impl InitPayload {
    /// Get the header
    pub fn header(&self) -> &InitHeader {
        &self.header
    }

    /// Get the data
    pub fn data(&self) -> Option<&Ipld> {
        self.data.as_ref()
    }
}

/// Headers for an init event
#[derive(Debug, Serialize, Deserialize)]
pub struct InitHeader {
    controllers: Vec<String>,
    sep: String,
    model: Bytes,
}

impl InitHeader {
    /// Get the controllers
    pub fn controllers(&self) -> &[String] {
        self.controllers.as_ref()
    }

    /// Get the sep
    pub fn sep(&self) -> &str {
        self.sep.as_ref()
    }

    /// Get the model
    pub fn model(&self) -> &Bytes {
        &self.model
    }
}

/// Payload of a data event
#[derive(Debug, Serialize, Deserialize)]
pub struct DataPayload {
    id: Cid,
    prev: Cid,
    header: Option<DataHeader>,
    data: Ipld,
}

impl DataPayload {
    /// Get the id
    pub fn id(&self) -> &Cid {
        &self.id
    }
    /// Get the prev
    pub fn prev(&self) -> &Cid {
        &self.prev
    }
    /// Get the header
    pub fn header(&self) -> Option<&DataHeader> {
        self.header.as_ref()
    }
    /// Get the data
    pub fn data(&self) -> &Ipld {
        &self.data
    }
}

/// Headers for a data event
#[derive(Debug, Serialize, Deserialize)]
pub struct DataHeader {
    controllers: Option<Vec<String>>,
    sep: String,
    model: Bytes,
}

impl DataHeader {
    /// Get the controllers
    pub fn controllers(&self) -> Option<&Vec<String>> {
        self.controllers.as_ref()
    }

    /// Get the sep
    pub fn sep(&self) -> &str {
        self.sep.as_ref()
    }

    /// Get the model
    pub fn model(&self) -> &Bytes {
        &self.model
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

/// Time event
#[derive(Debug, Serialize, Deserialize)]
pub struct TimeEvent {
    //id: Cid,
    //prev: Cid,
    //proof: Cid,
    path: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Protected {
    // There are more field in this struct be we only care about cap so far.
    cap: Option<Cid>,
}
