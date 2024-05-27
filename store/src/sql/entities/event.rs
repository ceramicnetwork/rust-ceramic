use anyhow::anyhow;
use ceramic_core::EventId;
use cid::Cid;
use iroh_car::{CarHeader, CarReader, CarWriter};

use std::collections::BTreeSet;

use crate::{
    sql::entities::{BlockRow, EventBlockRaw},
    Error, Result,
};

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
    /// Try to build the EventInsertable struct. Will error if the key and body don't match.
    pub fn try_new(order_key: EventId, body: EventInsertableBody) -> Result<Self> {
        if order_key.cid().as_ref() != Some(&body.cid) {
            return Err(Error::new_app(anyhow!(
                "Event ID and body CID do not match: {:?} != {:?}",
                order_key.cid(),
                body.cid
            )))?;
        }
        Ok(Self { order_key, body })
    }

    /// change the deliverable status of the event
    pub fn set_deliverable(&mut self, deliverable: bool) {
        self.body.deliverable = deliverable;
    }

    /// Whether or not the event is deliverable
    pub fn deliverable(&self) -> bool {
        self.body.deliverable
    }

    /// Get the CID of the event
    pub fn cid(&self) -> Cid {
        self.body.cid
    }
}

#[derive(Debug, Clone)]
/// The type we use to insert events into the database
pub struct EventInsertableBody {
    /// The event CID i.e. the root CID from the car file
    pub(crate) cid: Cid,
    /// Whether this event is deliverable to clients or is waiting for more data
    pub(crate) deliverable: bool,
    /// The blocks of the event
    // could use a map but there aren't that many blocks per event (right?)
    pub(crate) blocks: Vec<EventBlockRaw>,
}

impl EventInsertableBody {
    /// Create a new EventInsertRaw struct. Deliverable is set to false by default.
    pub fn new(cid: Cid, blocks: Vec<EventBlockRaw>) -> Self {
        Self {
            cid,
            deliverable: false,
            blocks,
        }
    }

    /// Get the CID of the event
    pub fn cid(&self) -> Cid {
        self.cid
    }

    /// Whether this event is deliverable to clients or is waiting for more data
    pub fn deliverable(&self) -> bool {
        self.deliverable
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
        Ok(Self::new(event_cid, blocks))
    }
}
