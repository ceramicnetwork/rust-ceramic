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
pub struct EventRaw {
    pub order_key: EventId,
    pub blocks: Vec<EventBlockRaw>,
}

impl EventRaw {
    pub fn new(key: EventId, blocks: Vec<EventBlockRaw>) -> Self {
        Self {
            order_key: key,
            blocks,
        }
    }

    pub async fn try_build(key: EventId, val: &[u8]) -> Result<Self> {
        // TODO: remove this when value is always present
        if val.is_empty() {
            return Ok(Self::new(key, vec![]));
        }
        let mut reader = CarReader::new(val)
            .await
            .map_err(|e| Error::new_app(anyhow!(e)))?;
        let roots: BTreeSet<Cid> = reader.header().roots().iter().cloned().collect();
        let mut idx = 0;
        let mut blocks = vec![];
        while let Some((cid, data)) = reader.next_block().await.map_err(Error::new_app)? {
            let ebr = EventBlockRaw::try_new(&key, idx, roots.contains(&cid), cid, data)
                .map_err(Error::from)?;
            blocks.push(ebr);
            idx += 1;
        }
        Ok(Self::new(key, blocks))
    }
}
