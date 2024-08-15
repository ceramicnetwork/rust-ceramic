use anyhow::anyhow;
use ceramic_core::EventId;
use cid::Cid;
use iroh_car::{CarHeader, CarReader, CarWriter};

use std::collections::BTreeSet;

pub use crate::sql::entities::EventBlockRaw;

use crate::{sql::entities::BlockRow, Error, Result};

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
    order_key: EventId,
    /// The event CID i.e. the root CID from the car file
    cid: Cid,
    /// Whether the event is deliverable i.e. it's prev has been delivered and the chain is continuous to an init event
    deliverable: bool,
    /// The blocks of the event
    // could use a map but there aren't that many blocks per event (right?)
    blocks: Vec<EventBlockRaw>,
}

impl EventInsertable {
    /// EventInsertable constructor
    pub fn new(order_key: EventId, blocks: Vec<EventBlockRaw>, deliverable: bool) -> Self {
        let cid = order_key.cid().unwrap();

        Self {
            order_key,
            cid,
            deliverable,
            blocks,
        }
    }

    /// Get the Recon order key (EventId) of the event.
    pub fn order_key(&self) -> &EventId {
        &self.order_key
    }

    /// Get the CID of the event
    pub fn cid(&self) -> Cid {
        self.cid
    }

    /// Underlying bytes that make up the event
    pub fn blocks(&self) -> &Vec<EventBlockRaw> {
        &self.blocks
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

    /// Try to build the EventInsertable struct from a carfile.
    pub async fn try_from_carfile(order_key: EventId, car_bytes: &[u8]) -> Result<Self> {
        let event_cid = order_key.cid().ok_or_else(|| {
            Error::new_invalid_arg(anyhow::anyhow!("EventID is missing a CID: {}", order_key))
        })?;

        if car_bytes.is_empty() {
            return Err(Error::new_app(anyhow!(
                "CAR file is empty: cid={}",
                event_cid
            )))?;
        }

        let mut reader = CarReader::new(car_bytes)
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
        Ok(Self {
            order_key,
            cid: event_cid,
            blocks,
            deliverable: false,
        })
    }
}
