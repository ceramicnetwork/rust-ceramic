use std::{collections::BTreeSet, sync::Arc};

use anyhow::anyhow;
use ceramic_car::{CarHeader, CarReader, CarWriter};
use ceramic_core::{EventId, NodeId};
use ceramic_event::unvalidated;
use cid::Cid;
use ipld_core::ipld::Ipld;

use super::{BlockRow, EventBlockRaw};
use crate::store::{Error, Result};

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

#[derive(Debug)]
/// The type we use to insert events into the database
pub struct EventInsertable {
    /// The event order key (e.g. EventID)
    order_key: EventId,
    /// The event CID i.e. the root CID from the car file
    cid: Cid,
    /// Whether the event is deliverable i.e. it's prev has been delivered and the chain is continuous to an init event
    deliverable: bool,
    /// The parsed structure containing the actual Event data.
    event: Arc<unvalidated::Event<Ipld>>,
    /// The ID of the Node that informed us about this event
    informant: Option<NodeId>,
}

impl EventInsertable {
    /// EventInsertable constructor
    pub fn new(
        order_key: EventId,
        event_cid: Cid,
        event: unvalidated::Event<Ipld>,
        informant: Option<NodeId>,
        deliverable: bool,
    ) -> Result<Self> {
        let cid = order_key.cid().ok_or_else(|| {
            Error::new_app(anyhow::anyhow!(
                "EventId missing CID. EventID={}",
                order_key
            ))
        })?;

        if event_cid != cid {
            return Err(Error::new_app(anyhow::anyhow!(
                "EventId CID ({}) does not match the root CID of the CAR file ({})",
                cid,
                event_cid
            )));
        }

        Ok(Self {
            order_key,
            cid,
            deliverable,
            event: Arc::new(event),
            informant,
        })
    }
    /// Get the Recon order key (EventId) of the event.
    pub fn order_key(&self) -> &EventId {
        &self.order_key
    }

    /// Get the CID of the init event of the stream
    pub fn stream_cid(&self) -> &Cid {
        self.event.id()
    }

    /// Get the CID of the event
    pub fn cid(&self) -> &Cid {
        &self.cid
    }

    /// Get the parsed Event structure.
    pub fn event(&self) -> &Arc<unvalidated::Event<Ipld>> {
        &self.event
    }

    /// Get the Event source.
    pub fn informant(&self) -> &Option<NodeId> {
        &self.informant
    }

    /// Whether this event is deliverable currently
    pub fn deliverable(&self) -> bool {
        self.deliverable
    }

    /// Mark the event as deliverable.
    /// This will be used when inserting the event to make sure the field is updated accordingly.
    pub fn set_deliverable(&mut self, deliverable: bool) {
        self.deliverable = deliverable;
    }

    /// Underlying bytes that make up the event
    pub async fn get_raw_blocks(&self) -> Result<Vec<EventBlockRaw>> {
        // TODO(AES-311): Turn the Event into its raw blocks in a single pass, instead of first
        // turning it into a CAR file and then turning the CAR file into the raw blocks.
        let car = self.event.encode_car().map_err(Error::new_app)?;

        let mut reader = CarReader::new(car.as_slice())
            .await
            .map_err(|e| Error::new_app(anyhow!(e)))?;
        let root_cid = reader
            .header()
            .roots()
            .first()
            .ok_or_else(|| Error::new_app(anyhow!("car data should have at least one root")))?;

        if self.cid != *root_cid {
            return Err(Error::new_app(anyhow!(
                "Event ID cid {}, does not match the root CID of the CAR file {}",
                self.cid,
                root_cid
            )));
        }
        let roots: BTreeSet<Cid> = reader.header().roots().iter().cloned().collect();
        let mut idx = 0;
        let mut blocks = vec![];
        while let Some((cid, data)) = reader.next_block().await.map_err(Error::new_app)? {
            let ebr = EventBlockRaw::try_new(&self.cid, idx, roots.contains(&cid), cid, data)
                .map_err(Error::from)?;
            blocks.push(ebr);
            idx += 1;
        }

        Ok(blocks)
    }
}
