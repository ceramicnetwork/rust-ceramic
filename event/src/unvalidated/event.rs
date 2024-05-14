//! Types of raw unvalidated Ceramic Events
#![allow(dead_code)]
use crate::unvalidated::{init, signed};
use cid::Cid;
use serde::{Deserialize, Serialize};

/// Ceramic Event
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum Event<D> {
    /// Time event in a stream
    // NOTE: TimeEvent has several CIDs so its a relatively large struct (~312 bytes according to
    // the compiler). Therefore we box it here to keep the Event enum small.
    Time(Box<TimeEvent>),
    /// Signed event in a stream
    Signed(signed::Payload),
    /// Unsigned event in a stream
    Unsigned(init::Payload<D>),
}

impl<D> From<Box<TimeEvent>> for Event<D> {
    fn from(value: Box<TimeEvent>) -> Self {
        Self::Time(value)
    }
}

impl<D> From<init::Payload<D>> for Event<D> {
    fn from(value: init::Payload<D>) -> Self {
        Self::Unsigned(value)
    }
}

impl<D> From<signed::Payload> for Event<D> {
    fn from(value: signed::Payload) -> Self {
        Self::Signed(value)
    }
}

/// Time event
#[derive(Debug, Serialize, Deserialize)]
pub struct TimeEvent {
    id: Cid,
    prev: Cid,
    proof: Cid,
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
    root: Cid,
    tx_hash: Cid,
    tx_type: String,
}

impl Proof {
    /// Get chain ID
    pub fn chain_id(&self) -> &str {
        self.chain_id.as_ref()
    }

    /// Get root
    pub fn root(&self) -> Cid {
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