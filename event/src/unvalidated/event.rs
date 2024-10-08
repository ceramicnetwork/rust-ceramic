//! Types of raw unvalidated Ceramic Events
use std::{collections::HashMap, fmt::Debug, io::Read};

use anyhow::{anyhow, bail, Context};
use cid::Cid;
use ipld_core::ipld::Ipld;
use serde::{Deserialize, Serialize};
use tracing::debug;

use ceramic_car::sync::{CarHeader, CarReader, CarWriter};
use ceramic_core::SerializeExt;

use super::{init, signed, Payload};

/// Helper function for Event::decode_car for gathering all the Ipld blocks used by a time event
/// witness proof.
fn get_time_event_witness_blocks(
    event: &RawTimeEvent,
    proof: &Proof,
    car_blocks: HashMap<Cid, Vec<u8>>,
) -> anyhow::Result<Vec<Ipld>> {
    let mut blocks_in_path = Vec::new();
    if event.prev == proof.root && event.path.is_empty() {
        return Ok(blocks_in_path);
    }

    let block_bytes = car_blocks
        .get(&proof.root())
        .ok_or_else(|| anyhow!("Time Event CAR data missing block for root",))?;
    blocks_in_path.push(serde_ipld_dagcbor::from_slice(block_bytes)?);
    let parts: Vec<_> = event.path().split('/').collect();
    // Add blocks for all parts but the last as it is the prev.
    for index in parts.iter().take(parts.len() - 1) {
        // unwrap() is safe because .last() only returns None if the Vec is empty, and we know
        // it has at least one element because we pushed a block before entering the loop.
        let cid = blocks_in_path
            .last()
            .unwrap()
            .get(*index)?
            .ok_or_else(|| anyhow!("Time Event path indexes missing data"))?;
        let cid = match cid {
            Ipld::Link(cid) => cid,
            _ => bail!("Time Event path does not index to a CID"),
        };
        let block_bytes = car_blocks
            .get(cid)
            .ok_or_else(|| anyhow!("Time Event CAR data missing block for path index"))?;
        blocks_in_path.push(serde_ipld_dagcbor::from_slice(block_bytes)?);
    }

    // make sure the last item truly does point to prev
    match blocks_in_path
        .last()
        .expect("blocks_in_path is not empty")
        .get(
            *parts
                .last()
                .ok_or_else(|| anyhow!("Time Event path must have at least one item"))?,
        )?
        .ok_or_else(|| anyhow!("Time Event path failed to resolve last element"))?
    {
        Ipld::Link(cid) => {
            if *cid != event.prev() {
                bail!(
                    "the anchor commit proof {} with path {} points to invalid 'prev' commit. Expected prev='{}' but found '{}'",
                    event.proof,
                    event.path,
                    event.prev(),
                    cid,
                )
            }
        }
        _ => bail!("Time Event path does not index to a CID"),
    }

    Ok(blocks_in_path)
}

/// Materialized Ceramic Event where internal structure is accessible.
#[derive(Debug)]
pub enum Event<D> {
    /// Time event in a stream
    // NOTE: TimeEvent has several CIDs so it's a relatively large struct (~312 bytes according to
    // the compiler). Therefore, we box it here to keep the Event enum small.
    Time(Box<TimeEvent>),
    /// Signed event in a stream
    Signed(signed::Event<D>),
    /// Unsigned event in a stream
    Unsigned(Box<init::Event<D>>),
}

impl<D> Event<D>
where
    D: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    /// Returns true if this Event is an init event, and false otherwise
    pub fn is_init(&self) -> bool {
        match self {
            Event::Time(_) => false,
            Event::Signed(event) => match event.payload() {
                Payload::Data(_) => false,
                Payload::Init(_) => true,
            },
            Event::Unsigned(_) => true,
        }
    }

    /// Returns the init payload if the event is an init event, otherwise returns None.
    pub fn init_payload(&self) -> Option<&init::Payload<D>> {
        match self {
            Event::Unsigned(event) => Some(event.payload()),
            Event::Signed(signed_event) => {
                if let Payload::Init(init_payload) = signed_event.payload() {
                    Some(init_payload)
                } else {
                    None
                }
            }
            Event::Time(_) => None,
        }
    }

    /// Returns the prev CID (or None if the event is an init event)
    pub fn prev(&self) -> Option<&Cid> {
        match self {
            Event::Time(t) => Some(t.prev()),
            Event::Signed(event) => match event.payload() {
                Payload::Data(d) => Some(d.prev()),
                Payload::Init(_) => None,
            },
            Event::Unsigned(_) => None,
        }
    }

    /// Encode the event into a CAR bytes containing all blocks of the event.
    pub fn encode_car(&self) -> anyhow::Result<Vec<u8>> {
        match self {
            Event::Time(event) => event.encode_car(),
            Event::Signed(event) => event.encode_car(),
            Event::Unsigned(event) => event.encode_car(),
        }
    }

    /// Decode bytes into a materialized event.
    pub fn decode_car<R>(reader: R, deny_unexpected_fields: bool) -> anyhow::Result<(Cid, Self)>
    where
        R: Read + Send + Unpin,
    {
        let car = CarReader::new(reader)?;
        let event_cid = *car
            .header()
            .roots()
            .first()
            .ok_or_else(|| anyhow!("CAR data should have at least one root"))?;

        debug!(%event_cid, "first root cid");

        let mut car_blocks = HashMap::new();
        for block in car {
            let (cid, bytes) = block?;
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
            let event_bytes_reserialized = raw_event.to_cbor()?;
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
                let blocks_in_path = get_time_event_witness_blocks(&event, &proof, car_blocks)?;
                let blocks_in_path = blocks_in_path
                    .into_iter()
                    .map(|block| match block {
                        Ipld::List(l) => Ok(l),
                        ipld => {
                            tracing::info!(?ipld, "Time Event witness node is not a list");
                            Err(anyhow!("Time Event witness node must be a list"))
                        }
                    })
                    .collect::<anyhow::Result<Vec<ProofEdge>>>()?;

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
                let payload: Payload<_> =
                    serde_ipld_dagcbor::from_slice(payload_bytes).context("decoding payload")?;
                let capability = envelope
                    .capability()
                    .map(|capability_cid| -> anyhow::Result<_> {
                        let capability_bytes =
                            car_blocks.get(&capability_cid).ok_or_else(|| {
                                anyhow!("Signed Event CAR data missing block for capability")
                            })?;
                        let capability: signed::cacao::Capability =
                            serde_ipld_dagcbor::from_slice(capability_bytes)
                                .context("decoding capability")?;
                        Ok((capability_cid, capability))
                    })
                    .transpose()?;

                if deny_unexpected_fields {
                    // Re-serialize the payload and compare the bytes. This indirectly checks that there
                    // were no unexpected fields in the event sent by the client.
                    let payload_bytes_reserialized = payload.to_cbor()?;
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
                        capability,
                    )),
                ))
            }
            RawEvent::Unsigned(payload) => Ok((
                event_cid,
                Event::Unsigned(Box::new(init::Event::new_with_cid(event_cid, payload))),
            )),
        }
    }

    /// Get the CID of the init event of the stream for this event
    pub fn stream_cid(&self) -> &Cid {
        match self {
            Event::Time(event) => event.id(),
            Event::Signed(event) => match event.payload() {
                Payload::Data(d) => d.id(),
                Payload::Init(_) => event.envelope_cid(),
            },
            Event::Unsigned(event) => event.cid(),
        }
    }

    /// Returns true if this Event is a time event, and false otherwise
    pub fn is_time_event(&self) -> bool {
        matches!(self, Event::Time(_))
    }
}

impl<D> From<Box<TimeEvent>> for Event<D> {
    fn from(value: Box<TimeEvent>) -> Self {
        Self::Time(value)
    }
}

impl<D> From<Box<init::Event<D>>> for Event<D> {
    fn from(value: Box<init::Event<D>>) -> Self {
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
    blocks_in_path: Vec<ProofEdge>,
}

impl TimeEvent {
    /// Create a new time event from its parts
    pub fn new(event: RawTimeEvent, proof: Proof, blocks_in_path: Vec<ProofEdge>) -> Self {
        Self {
            event,
            proof,
            blocks_in_path,
        }
    }

    ///  Get the id
    pub fn id(&self) -> &Cid {
        &self.event.id
    }

    ///  Get the prev
    pub fn prev(&self) -> &Cid {
        &self.event.prev
    }

    ///  Get the proof
    pub fn proof(&self) -> &Proof {
        &self.proof
    }

    ///  Get the path
    pub fn path(&self) -> &str {
        self.event.path.as_ref()
    }
    /// Encode the event into CAR bytes including all relevant blocks.
    pub fn encode_car(&self) -> anyhow::Result<Vec<u8>> {
        let (cid, event) = self.event.to_dag_cbor_block()?;

        let proof = self.proof.to_cbor()?;

        let mut car = Vec::new();
        let roots: Vec<Cid> = vec![cid];
        let mut writer = CarWriter::new(CarHeader::V1(roots.into()), &mut car);
        writer.write(cid, event)?;
        writer.write(self.event.proof, proof)?;
        for block in &self.blocks_in_path {
            let (block_cid, block_bytes) = block.to_dag_cbor_block()?;
            writer.write(block_cid, block_bytes)?;
        }
        writer.finish()?;
        Ok(car)
    }
}

/// Raw Time Event as it is encoded in the protocol.
#[derive(Serialize, Deserialize, Clone)]
pub struct RawTimeEvent {
    /// The CID of the init event of the stream
    id: Cid,
    /// The CID of the Data event that is being anchored in the chain
    prev: Cid,
    /// The CID of the proof block that tells us how to query the chain
    proof: Cid,
    /// path from the root in the proof block to the prev in the merkle tree
    path: String,
}

impl Debug for RawTimeEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawTimeEvent")
            .field("id", &format!("{:?}", &self.id))
            .field("prev", &format!("{:?}", &self.prev))
            .field("proof", &format!("{:?}", &self.proof))
            .field("path", &self.path)
            .finish()
    }
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
#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Proof {
    /// eip-155 CHAIN_ID see https://chainid.network https://github.com/ethereum/EIPs/blob/master/EIPS/eip-155.md
    chain_id: String,
    /// the root node of the merkle tree
    root: Cid,
    /// the transaction hash for looking up the transaction in the chain
    tx_hash: Cid,
    /// the type of the transaction so we know how to extract the root from the transaction
    tx_type: String,
}

impl Debug for Proof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Proof")
            .field("chain_id", &self.chain_id)
            .field("root", &format!("{:?}", &self.root))
            .field("tx_hash", &format!("{:?}", &self.tx_hash))
            .field("tx_type", &self.tx_type)
            .finish()
    }
}

impl Proof {
    /// Create a proof from its parts.
    /// hoku(adress:index) is the format of the tx_type
    /// transaction hash is the value in the format of the tx_type
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

/// Proof edge TODO: rename witness node
pub type ProofEdge = Vec<Ipld>;

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use cid::Cid;
    use ipld_core::ipld::Ipld;
    use test_log::test;

    use crate::unvalidated::tests::{
        SIGNED_INIT_EVENT_CID, TIME_EVENT_CAR_MULTI_EVENT_BATCH, UNSIGNED_INIT_NO_SEP_CAR,
    };
    use crate::unvalidated::{
        payload,
        tests::{
            CACAO_SIGNED_DATA_EVENT_CAR, DATA_EVENT_CAR_UNSIGNED_INIT, SIGNED_DATA_EVENT_CAR,
            SIGNED_INIT_EVENT_CAR, TIME_EVENT_CAR_SINGLE_EVENT_BATCH, UNSIGNED_INIT_EVENT_CAR,
        },
        Builder, Event,
    };

    fn round_trip(car: &str) {
        let (base, data) = multibase::decode(car).unwrap();
        let (_cid, event) = Event::<Ipld>::decode_car(data.as_slice(), true).unwrap();
        assert_eq!(car, multibase::encode(base, event.encode_car().unwrap()));
    }
    #[test]
    fn round_trip_signed_init_event() {
        round_trip(SIGNED_INIT_EVENT_CAR);
    }
    #[test]
    fn round_trip_unsigned_init_event() {
        round_trip(UNSIGNED_INIT_EVENT_CAR);
    }
    #[test]
    fn round_trip_signed_data_event() {
        round_trip(SIGNED_DATA_EVENT_CAR);
    }
    #[test]
    fn round_trip_cacao_signed_data_event() {
        round_trip(CACAO_SIGNED_DATA_EVENT_CAR);
    }
    #[test]
    fn round_trip_data_event_unsigned_init() {
        round_trip(DATA_EVENT_CAR_UNSIGNED_INIT);
    }
    #[test(tokio::test)]
    async fn round_trip_time_event_single_event_batch() {
        round_trip(TIME_EVENT_CAR_SINGLE_EVENT_BATCH);
    }
    #[test(tokio::test)]
    async fn round_trip_time_event_multi_event_batch() {
        round_trip(TIME_EVENT_CAR_MULTI_EVENT_BATCH);
    }
    #[test]
    fn round_trip_init_payload_with_no_sep() {
        round_trip(UNSIGNED_INIT_NO_SEP_CAR);
    }

    #[test]
    fn decode_time_event_with_no_tree() {
        let id = Cid::from_str(SIGNED_INIT_EVENT_CID).unwrap();
        let prev =
            Cid::from_str("bagcqcerae5oqoglzjjgz53enwsttl7mqglp5eoh2llzbbvfktmzxleeiffbq").unwrap();
        let tx_hash =
            Cid::from_str("bagjqcgzadp7fstu7fz5tfi474ugsjqx5h6yvevn54w5m4akayhegdsonwciq").unwrap();

        let event = Builder::time()
            .with_id(id)
            .with_tx(
                "eip155:11155111".to_string(),
                tx_hash,
                "f(bytes32)".to_string(),
            )
            .with_prev(prev)
            .build()
            .unwrap();

        let event_car = event.encode_car().unwrap();
        let (_cid, parsed_event) = Event::<Ipld>::decode_car(event_car.as_slice(), true).unwrap();

        let Event::Time(parsed_event) = parsed_event else {
            panic!("Event must be a time event")
        };

        assert_eq!(prev, parsed_event.event.prev);
        assert_eq!(prev, parsed_event.proof.root);
        assert_eq!("", parsed_event.event.path);
    }
    #[test]
    fn decode_event_with_no_sep() {
        // Tests that decoding an init payload that does not have the `sep` field defaults to
        // `model`.
        const INIT_PAYLOAD_NO_SEP:&str="uomRkYXRhpmRkYXRho2N1cmxgZWxhYmVsZ0Zhc3RpbmduY2hpbGRyZW5IaWRkZW70ZHR5cGVsUXVlc3Rpb25Ob2RlZ2NyZWF0ZWR4GDIwMjMtMDItMjBUMTU6MTk6MzYuMjc5Wmhwb3NpdGlvbqJhePtApYOSIAAAAGF5-0C2p4AAAAAAaWxhdGVyYWxJRHgkNjlhYWYzN2QtNTU5Yi00Yjk1LWExMDAtNWVlOTgxOGZjNWVkaXByb2plY3RJRHg_a2p6bDZrY3ltN3c4eTVkNjVmOW9rbjRyaXlkYXQ5MmgzczZ2dnpwd3d1NzU0NGk5MmZqeWdjNTY3bHpocnZjZmhlYWRlcqNlbW9kZWxYKM4BAgGFARIgluyz1feTN9qD54Xo4XHQoMg5Xo_kPE6L5xYBadM3kWNmdW5pcXVlTCrb84nFhVpKhrCYm2tjb250cm9sbGVyc4F4O2RpZDpwa2g6ZWlwMTU1OjE6MHhjYTVmZjRiMzQ0MmZjYWMyN2UxYWY0NDU3ZTAyZWI2MjljNzEyOTgz";
        let init_payload: payload::init::Payload<Ipld> =
            serde_ipld_dagcbor::from_slice(&multibase::decode(INIT_PAYLOAD_NO_SEP).unwrap().1)
                .unwrap();
        assert_eq!("model", init_payload.header().sep());
    }
}
