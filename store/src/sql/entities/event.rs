use anyhow::anyhow;
use ceramic_core::EventId;
use ceramic_event::unvalidated;
use cid::Cid;
use ipld_core::ipld::Ipld;
use iroh_car::{CarHeader, CarReader, CarWriter};

use std::collections::BTreeSet;

use crate::{
    sql::entities::{BlockRow, EventBlockRaw},
    Error, Result,
};

use super::{EventHeader, EventType};

pub async fn rebuild_car(blocks: Vec<BlockRow>) -> Result<Option<Vec<u8>>> {
    if blocks.is_empty() {
        return Ok(None);
    }

    let size = blocks.iter().fold(0, |sum, row| sum + row.bytes.len());
    let roots: Vec<Cid> = blocks
        .iter()
        .filter(|row| row.root)
        .map(|row| row.cid)
        .collect();
    // Reconstruct the car file
    // TODO figure out a better capacity calculation
    let mut car = Vec::with_capacity(size + 100 * blocks.len());
    let mut writer = CarWriter::new(CarHeader::V1(roots.into()), &mut car);
    for BlockRow {
        cid,
        bytes,
        root: _,
    } in blocks
    {
        writer
            .write(cid, bytes)
            .await
            .map_err(Error::new_transient)?;
    }
    writer.finish().await.map_err(Error::new_transient)?;
    Ok(Some(car))
}

#[derive(Debug, Clone)]
/// The type we use to insert events into the database
pub struct EventInsertable {
    /// The event order key (e.g. EventID)
    pub order_key: EventId,
    /// The data that makes up the event
    pub body: EventInsertableBody,
}

impl EventInsertable {
    /// Try to build the EventInsertable struct from a carfile.
    pub async fn try_new(order_key: EventId, body: &[u8]) -> Result<Self> {
        let cid = order_key.cid().ok_or_else(|| {
            Error::new_invalid_arg(anyhow::anyhow!("EventID is missing a CID: {}", order_key))
        })?;
        let body = EventInsertableBody::try_from_carfile(cid, body).await?;
        Ok(Self { order_key, body })
    }

    /// Get the CID of the event
    pub fn cid(&self) -> Cid {
        self.body.cid
    }

    /// Get the stream CID
    pub fn stream_cid(&self) -> Cid {
        self.body.header.stream_cid()
    }

    /// Get the previous event CID if any
    pub fn prev(&self) -> Option<Cid> {
        match &self.body.header {
            EventHeader::Data { prev, .. } | EventHeader::Time { prev, .. } => Some(*prev),
            EventHeader::Init { .. } => None,
        }
    }

    /// Whether this event is deliverable currently
    pub fn deliverable(&self) -> bool {
        self.body.deliverable
    }

    /// Mark the event as deliverable.
    /// This will be used when inserting the event to make sure the field is updated accordingly.
    pub fn set_deliverable(&mut self, deliverable: bool) {
        self.body.deliverable = deliverable;
    }
}

#[derive(Debug, Clone)]
/// The type we use to insert events into the database
pub struct EventInsertableBody {
    /// The event CID i.e. the root CID from the car file
    pub(crate) cid: Cid,
    /// The event header data about the event type and stream
    pub(crate) header: EventHeader,
    /// Whether the event is deliverable i.e. it's prev has been delivered and the chain is continuous to an init event
    pub(crate) deliverable: bool,
    /// The blocks of the event
    // could use a map but there aren't that many blocks per event (right?)
    pub(crate) blocks: Vec<EventBlockRaw>,
}

impl EventInsertableBody {
    /// Create a new EventInsertRaw struct. Deliverable is set to false by default.
    pub fn new(
        cid: Cid,
        header: EventHeader,
        blocks: Vec<EventBlockRaw>,
        deliverable: bool,
    ) -> Self {
        Self {
            cid,
            header,
            blocks,
            deliverable,
        }
    }

    /// Get the CID of the event
    pub fn cid(&self) -> Cid {
        self.cid
    }

    /// Whether this event is immediately deliverable to clients or the history chain needs to be reviewed
    /// false indicates it can be stored and delivered immediately
    pub fn event_type(&self) -> EventType {
        self.header.event_type()
    }

    /// Get the blocks of the event
    pub fn blocks(&self) -> &Vec<EventBlockRaw> {
        &self.blocks
    }

    /// Find a block from the carfile for a given CID if it's included
    pub fn block_for_cid_opt(&self, cid: &Cid) -> Option<&EventBlockRaw> {
        self.blocks
            .iter()
            .find(|b| Cid::new_v1(b.codec.try_into().unwrap(), *b.multihash.inner()) == *cid)
    }

    /// Find a block from the carfile for a given CID if it's included
    pub fn block_for_cid(&self, cid: &Cid) -> Result<&EventBlockRaw> {
        self.block_for_cid_opt(cid)
            .ok_or_else(|| Error::new_app(anyhow!("Event data is missing data for CID {}", cid)))
    }

    /// Builds a new EventInsertRaw from a CAR file. Will error if the CID in the EventID doesn't match the
    /// first root of the carfile.
    pub async fn try_from_carfile(event_cid: Cid, val: &[u8]) -> Result<Self> {
        if val.is_empty() {
            return Err(Error::new_app(anyhow!(
                "CAR file is empty: cid={}",
                event_cid
            )))?;
        }

        let mut reader = CarReader::new(val)
            .await
            .map_err(|e| Error::new_app(anyhow!(e)))?;
        let root_cid = reader
            .header()
            .roots()
            .first()
            .ok_or_else(|| Error::new_app(anyhow!("car data should have at least one root")))?;

        if event_cid != *root_cid {
            return Err(Error::new_app(anyhow!(
                "Event ID does not match the root CID of the CAR file"
            )));
        }
        let roots: BTreeSet<Cid> = reader.header().roots().iter().cloned().collect();
        let mut idx = 0;
        let mut blocks = vec![];
        while let Some((cid, data)) = reader.next_block().await.map_err(Error::new_app)? {
            let ebr = EventBlockRaw::try_new(&event_cid, idx, roots.contains(&cid), cid, data)
                .map_err(Error::from)?;
            blocks.push(ebr);
            idx += 1;
        }

        let ev_block = blocks
            .iter()
            .find(|b| b.cid() == event_cid)
            .ok_or_else(|| {
                Error::new_app(anyhow!(
                    "event block not found in car file: cid={}",
                    event_cid
                ))
            })?;
        let event_ipld: unvalidated::RawEvent<Ipld> =
            serde_ipld_dagcbor::from_slice(&ev_block.bytes).map_err(|e| {
                Error::new_invalid_arg(
                    anyhow::anyhow!(e).context("event block is not valid event format"),
                )
            })?;

        let cid = event_cid;

        let (deliverable, header) = match event_ipld {
            unvalidated::RawEvent::Time(t) => (
                false,
                EventHeader::Time {
                    cid,
                    stream_cid: t.id(),
                    prev: t.prev(),
                },
            ),
            unvalidated::RawEvent::Signed(signed) => {
                let link = signed.link().ok_or_else(|| {
                    Error::new_invalid_arg(anyhow::anyhow!("event should have a link"))
                })?;
                let link = blocks.iter().find(|b| b.cid() == link).ok_or_else(|| {
                    Error::new_invalid_arg(anyhow::anyhow!("prev CID missing from carfile"))
                })?;
                let payload: unvalidated::Payload<Ipld> =
                    serde_ipld_dagcbor::from_slice(&link.bytes).map_err(|e| {
                        Error::new_invalid_arg(
                            anyhow::anyhow!(e).context("Failed to follow event link"),
                        )
                    })?;

                match payload {
                    unvalidated::Payload::Data(d) => (
                        false,
                        EventHeader::Data {
                            cid,
                            stream_cid: *d.id(),
                            prev: *d.prev(),
                        },
                    ),
                    unvalidated::Payload::Init(init) => {
                        let header = init.header().to_owned();

                        (true, EventHeader::Init { cid, header })
                    }
                }
            }
            unvalidated::RawEvent::Unsigned(init) => (
                true,
                EventHeader::Init {
                    cid,
                    header: init.header().to_owned(),
                },
            ),
        };

        Ok(Self::new(event_cid, header, blocks, deliverable))
    }
}
