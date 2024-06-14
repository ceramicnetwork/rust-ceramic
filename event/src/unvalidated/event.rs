//! Types of raw unvalidated Ceramic Events

use crate::unvalidated::{init, signed};

use cid::Cid;
use ipld_core::ipld::Ipld;
use iroh_car::{CarHeader, CarWriter};
use serde::{Deserialize, Serialize};

use super::cid_from_dag_cbor;

/// Materialized Ceramic Event where internal structure is accessible.
pub enum Event<D> {
    /// Time event in a stream
    // NOTE: TimeEvent has several CIDs so it's a relatively large struct (~312 bytes according to
    // the compiler). Therefore, we box it here to keep the Event enum small.
    Time(Box<TimeEvent>),
    /// Signed event in a stream
    Signed(signed::Event<D>),
    /// Unsigned event in a stream
    Unsigned(init::Payload<D>),
}

/// Ceramic Event as it is encoded in the protocol.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum RawEvent<D> {
    /// Time event in a stream
    // NOTE: TimeEvent has several CIDs so it's a relatively large struct (~312 bytes according to
    // the compiler). Therefore, we box it here to keep the Event enum small.
    Time(Box<RawTimeEvent>),
    /// Signed event in a stream
    Signed(signed::Envelope),
    /// Unsigned event in a stream
    Unsigned(init::Payload<D>),
}

impl<D> From<Box<RawTimeEvent>> for RawEvent<D> {
    fn from(value: Box<RawTimeEvent>) -> Self {
        Self::Time(value)
    }
}

impl<D> From<init::Payload<D>> for RawEvent<D> {
    fn from(value: init::Payload<D>) -> Self {
        Self::Unsigned(value)
    }
}

impl<D> From<signed::Envelope> for RawEvent<D> {
    fn from(value: signed::Envelope) -> Self {
        Self::Signed(value)
    }
}

/// Materialized Time Event where all parts of the proof are accessible.
#[derive(Debug)]
pub struct TimeEvent {
    event: RawTimeEvent,
    proof: Proof,
    blocks_in_path: Vec<Ipld>,
}

impl TimeEvent {
    /// Create a new time event from its parts
    pub fn new(event: RawTimeEvent, proof: Proof, blocks_in_path: Vec<Ipld>) -> Self {
        Self {
            event,
            proof,
            blocks_in_path,
        }
    }

    ///  Get the id
    pub fn id(&self) -> Cid {
        self.event.id
    }

    ///  Get the prev
    pub fn prev(&self) -> Cid {
        self.event.prev
    }

    ///  Get the proof
    pub fn proof(&self) -> Cid {
        self.event.proof
    }

    ///  Get the path
    pub fn path(&self) -> &str {
        self.event.path.as_ref()
    }
    /// Encode the event into CAR bytes including all relevant blocks.
    pub async fn encode_car(&self) -> anyhow::Result<Vec<u8>> {
        let event = serde_ipld_dagcbor::to_vec(&self.event)?;
        let cid = cid_from_dag_cbor(&event);

        let proof = serde_ipld_dagcbor::to_vec(&self.proof)?;

        let mut car = Vec::new();
        let roots: Vec<Cid> = vec![cid];
        let mut writer = CarWriter::new(CarHeader::V1(roots.into()), &mut car);
        writer.write(cid, event).await?;
        writer.write(self.event.proof, proof).await?;
        for block in &self.blocks_in_path {
            let block_bytes = serde_ipld_dagcbor::to_vec(&block)?;
            let block_cid = cid_from_dag_cbor(&block_bytes);
            writer.write(block_cid, block_bytes).await?;
        }
        writer.finish().await?;
        Ok(car)
    }
}
/// Raw Time Event as it is encoded in the protocol.
#[derive(Debug, Serialize, Deserialize)]
pub struct RawTimeEvent {
    id: Cid,
    prev: Cid,
    proof: Cid,
    path: String,
}

impl RawTimeEvent {
    /// Create a raw events from its parts. Prefer using the builder API instead.
    pub fn new(id: Cid, prev: Cid, proof: Cid, path: String) -> Self {
        Self {
            id,
            prev,
            proof,
            path,
        }
    }

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
    /// Create a proof from its parts.
    pub fn new(chain_id: String, root: Cid, tx_hash: Cid, tx_type: String) -> Self {
        Self {
            chain_id,
            root,
            tx_hash,
            tx_type,
        }
    }

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
