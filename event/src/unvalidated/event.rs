//! Types of raw unvalidated Ceramic Events

use anyhow::{anyhow, bail, Context};
use cid::Cid;
use ipld_core::ipld::Ipld;
use iroh_car::{CarHeader, CarReader, CarWriter};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::io::AsyncRead;
use tracing::debug;

use super::{cid_from_dag_cbor, init, signed};

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

impl<D> Event<D>
where
    D: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    /// Encode the event into a CAR bytes containing all blocks of the event.
    pub async fn encode_car(&self) -> anyhow::Result<Vec<u8>> {
        match self {
            Event::Time(event) => event.encode_car().await,
            Event::Signed(event) => event.encode_car().await,
            Event::Unsigned(event) => event.encode_car().await,
        }
    }

    /// Decode bytes into a materialized event.
    pub async fn decode_car<R>(
        reader: R,
        deny_unexpected_fields: bool,
    ) -> anyhow::Result<(Cid, Self)>
    where
        R: AsyncRead + Send + Unpin,
    {
        let mut car = CarReader::new(reader).await?;
        let event_cid = *car
            .header()
            .roots()
            .first()
            .ok_or_else(|| anyhow!("CAR data should have at least one root"))?;

        debug!(%event_cid, "first root cid");

        let mut car_blocks = HashMap::new();
        while let Some((cid, bytes)) = car.next_block().await? {
            car_blocks.insert(cid, bytes);
        }
        let event_bytes = car_blocks
            .get(&event_cid)
            .ok_or_else(|| anyhow!("Event CAR data missing block for root CID"))?;
        let raw_event: RawEvent<D> =
            serde_ipld_dagcbor::from_slice(event_bytes).context("decoding event")?;

        if deny_unexpected_fields {
            // Re-serialize the event and compare the bytes. This indirectly checks that there were no
            // unexpected fields in the event sent by the client.
            let event_bytes_reserialized = serde_ipld_dagcbor::to_vec(&raw_event)?;
            if !event_bytes.eq(&event_bytes_reserialized) {
                bail!(
                "Event bytes do not round-trip. This most likely means the event contains unexpected fields."
            );
            }
        }

        match raw_event {
            RawEvent::Time(event) => {
                let proof_bytes = car_blocks
                    .get(&event.proof())
                    .ok_or_else(|| anyhow!("Time Event CAR data missing block for proof"))?;
                let proof: Proof =
                    serde_ipld_dagcbor::from_slice(proof_bytes).context("decoding proof")?;
                let mut blocks_in_path = Vec::new();
                let block_bytes = car_blocks
                    .get(&proof.root())
                    .ok_or_else(|| anyhow!("Time Event CAR data missing block for root",))?;
                let mut block: Ipld = serde_ipld_dagcbor::from_slice(block_bytes)?;
                blocks_in_path.push(block.clone());
                let parts: Vec<_> = event.path().split('/').collect();
                // Add blocks for all parts but the last as it is the prev.
                for index in parts.iter().take(parts.len() - 1) {
                    let cid = block
                        .get(*index)?
                        .ok_or_else(|| anyhow!("Time Event path indexes missing data"))?;
                    let cid = match cid {
                        Ipld::Link(cid) => cid,
                        _ => bail!("Time Event path does not index to a CID"),
                    };
                    let block_bytes = car_blocks.get(cid).ok_or_else(|| {
                        anyhow!("Time Event CAR data missing block for path index")
                    })?;
                    blocks_in_path.push(block);
                    block = serde_ipld_dagcbor::from_slice(block_bytes)?;
                }

                Ok((
                    event_cid,
                    Event::Time(Box::new(TimeEvent::new(*event, proof, blocks_in_path))),
                ))
            }
            RawEvent::Signed(envelope) => {
                let payload_cid = envelope
                    .link()
                    .ok_or_else(|| anyhow!("event should have a link"))?;

                let payload_bytes = car_blocks
                    .get(&payload_cid)
                    .ok_or_else(|| anyhow!("Signed Event CAR data missing block for payload"))?;
                let payload =
                    serde_ipld_dagcbor::from_slice(payload_bytes).context("decoding payload")?;

                if deny_unexpected_fields {
                    // Re-serialize the payload and compare the bytes. This indirectly checks that there
                    // were no unexpected fields in the event sent by the client.
                    let payload_bytes_reserialized = serde_ipld_dagcbor::to_vec(&payload)?;
                    if !payload_bytes.eq(&payload_bytes_reserialized) {
                        bail!("Signed event payload bytes do not round-trip. This most likely means the event contains unexpected fields.");
                    }
                }

                Ok((
                    event_cid,
                    Event::Signed(signed::Event::new(
                        event_cid,
                        envelope,
                        payload_cid,
                        payload,
                    )),
                ))
            }
            RawEvent::Unsigned(event) => Ok((event_cid, Event::Unsigned(event))),
        }
    }
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

impl<D> From<signed::Event<D>> for Event<D> {
    fn from(value: signed::Event<D>) -> Self {
        Self::Signed(value)
    }
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

#[cfg(test)]
mod tests {

    use ipld_core::ipld::Ipld;
    use test_log::test;

    use crate::unvalidated::{
        tests::{
            DATA_EVENT_CAR, DATA_EVENT_CAR_UNSIGNED_INIT, SIGNED_INIT_EVENT_CAR, TIME_EVENT_CAR,
            UNSIGNED_INIT_EVENT_CAR,
        },
        Event,
    };

    async fn round_trip(car: &str) {
        let (base, data) = multibase::decode(car).unwrap();
        let (_cid, event) = Event::<Ipld>::decode_car(data.as_slice(), true)
            .await
            .unwrap();
        assert_eq!(
            car,
            multibase::encode(base, event.encode_car().await.unwrap())
        );
    }
    #[test(tokio::test)]
    async fn round_trip_signed_init_event() {
        round_trip(SIGNED_INIT_EVENT_CAR).await;
    }
    #[test(tokio::test)]
    async fn round_trip_unsigned_init_event() {
        round_trip(UNSIGNED_INIT_EVENT_CAR).await;
    }
    #[test(tokio::test)]
    async fn round_trip_data_event() {
        round_trip(DATA_EVENT_CAR).await;
    }
    #[test(tokio::test)]
    async fn round_trip_data_event_unsigned_init() {
        round_trip(DATA_EVENT_CAR_UNSIGNED_INIT).await;
    }
    #[test(tokio::test)]
    async fn round_trip_time_event() {
        round_trip(TIME_EVENT_CAR).await;
    }
}
