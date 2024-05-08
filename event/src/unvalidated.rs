//! Types of raw unvalidated Ceramic Events

use std::{collections::BTreeMap, marker::PhantomData};

use cid::Cid;
use ipld_core::ipld::Ipld;
use serde::{Deserialize, Serialize};

use crate::bytes::Bytes;

/// Ceramic Event
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Event<D> {
    /// Time event in a stream
    // NOTE: TimeEvent has several CIDs so its a relatively large struct (~312 bytes according to
    // the compiler). Therefore we box it here to keep the Event enum small.
    Time(Box<TimeEvent>),
    /// Signed event in a stream
    Signed(JWS),
    /// Unsigned event in a stream
    Unsigned(InitPayload<D>),
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
        self.signatures[0].protected.as_ref().and_then(|protected| {
            serde_json::from_slice::<Protected>(protected.as_slice())
                .ok()
                .and_then(|protected| protected.cap)
        })
    }
}

/// Payload of a signed event
#[derive(Debug, Serialize, Deserialize)]
// Note untagged variants a deserialized in order and the first one that succeeds is returned.
// Therefore the order of the variants is important to be most specific to least specific
#[serde(untagged)]
pub enum Payload<D> {
    /// Data event
    Data(DataPayload<D>),
    /// Init event
    Init(InitPayload<D>),
}

/// Payload of an init event
#[derive(Debug, Serialize, Deserialize)]
pub struct InitPayload<D> {
    header: InitHeader,
    data: Option<D>,
}

impl<D> InitPayload<D> {
    /// Construct a new payload for an init event
    pub fn new(header: InitHeader, data: Option<D>) -> Self {
        Self { header, data }
    }

    /// Get the header
    pub fn header(&self) -> &InitHeader {
        &self.header
    }

    /// Get the data
    pub fn data(&self) -> Option<&D> {
        self.data.as_ref()
    }
}

/// Headers for an init event
#[derive(Debug, Serialize, Deserialize)]
pub struct InitHeader {
    controllers: Vec<String>,
    sep: String,
    model: Bytes,
    unique: Option<Bytes>,
}

impl InitHeader {
    /// Construct a header for an init event payload
    pub fn new(controllers: Vec<String>, sep: String, model: Bytes, unique: Option<Bytes>) -> Self {
        Self {
            controllers,
            sep,
            model,
            unique,
        }
    }

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
pub struct DataPayload<D> {
    id: Cid,
    prev: Cid,
    header: Option<DataHeader>,
    data: D,
}

impl<D> DataPayload<D> {
    /// Construct a new payload for a data event
    pub fn new(id: Cid, prev: Cid, header: Option<DataHeader>, data: D) -> Self {
        Self {
            id,
            prev,
            header,
            data,
        }
    }

    /// Get the id
    pub fn id(&self) -> Cid {
        self.id
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
    pub fn data(&self) -> &D {
        &self.data
    }
}

/// Headers for a data event
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataHeader {
    should_index: bool,
}

impl DataHeader {
    /// Report the should_index property of the header.
    pub fn should_index(&self) -> bool {
        self.should_index
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
    id: TypedCid<Event>,
    prev: TypedCid<Event>,
    proof: TypedCid<Proof>,
    path: String,
}

impl TimeEvent {
    ///  Get the id
    pub fn id(&self) -> Cid {
        self.id
    }

    ///  Get the prev
    pub fn prev(&self) -> Cid {
        self.prev
    }

    ///  Get the proof
    pub fn proof(&self) -> Cid {
        self.proof
    }

    ///  Get the path
    pub fn path(&self) -> &str {
        self.path.as_ref()
    }
}

/// Proof data
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Proof {
    chain_id: String,
    root: TypedCid<ProofEdge>,
    tx_hash: Cid,
    tx_type: String,
}

impl Proof {
    /// Get chain ID
    pub fn chain_id(&self) -> &str {
        self.chain_id.as_ref()
    }

    /// Get root
    pub fn root(&self) -> TypedCid<ProofEdge> {
        self.root
    }

    /// Get tx hash
    pub fn tx_hash(&self) -> Cid {
        self.tx_hash
    }

    /// Get tx type
    pub fn tx_type(&self) -> &str {
        self.tx_type.as_ref()
    }
}

/// Proof edge
pub type ProofEdge = Vec<Cid>;

#[derive(Debug, Serialize, Deserialize)]
struct Protected {
    // There are more field in this struct be we only care about cap so far.
    cap: Option<Cid>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
struct TypedCid<T>(pub Cid, PhantomData<T>);

impl<T> TypedCid<T>
where
    T: for<'a> Deserialize<'a>,
{
    fn from_slice(
        &self,
        data: &[u8],
    ) -> Result<T, serde_ipld_dagcbor::DecodeError<std::convert::Infallible>> {
        serde_ipld_dagcbor::from_slice(data)
    }
}
