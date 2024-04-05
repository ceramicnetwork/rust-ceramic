use anyhow::{Context, Result};
use ceramic_core::EventId;
use cid::Cid;
use iroh_car::{CarHeader, CarWriter};
use itertools::{process_results, Itertools};
use multihash::Multihash;
use sqlx::{postgres::PgRow, sqlite::SqliteRow, Row as _};

use super::BlockRow;

pub type EventIdError = <EventId as TryFrom<Vec<u8>>>::Error;

pub async fn rebuild_car(blocks: Vec<BlockRow>) -> anyhow::Result<Option<Vec<u8>>> {
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
        writer.write(cid, bytes).await?;
    }
    writer.finish().await?;
    Ok(Some(car))
}

#[derive(Debug, Clone)]
pub struct ReconHash {
    pub count: i64,
    pub ahash_0: u32,
    pub ahash_1: u32,
    pub ahash_2: u32,
    pub ahash_3: u32,
    pub ahash_4: u32,
    pub ahash_5: u32,
    pub ahash_6: u32,
    pub ahash_7: u32,
}

fn into_u32s(val: Option<i64>, col: &str) -> Result<u32, sqlx::Error> {
    let ahash_0 = val
        .map(u32::try_from)
        .transpose()
        .map_err(|e| sqlx::Error::Decode(Box::new(e)))?
        .ok_or_else(|| sqlx::Error::ColumnNotFound(format!("{} was not found", col)))?;
    Ok(ahash_0)
}

impl sqlx::FromRow<'_, PgRow> for ReconHash {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        let ahash_0 = into_u32s(row.try_get("ahash_0")?, "ahash_0")?;
        let ahash_1 = into_u32s(row.try_get("ahash_1")?, "ahash_1")?;
        let ahash_2 = into_u32s(row.try_get("ahash_2")?, "ahash_2")?;
        let ahash_3 = into_u32s(row.try_get("ahash_3")?, "ahash_3")?;
        let ahash_4 = into_u32s(row.try_get("ahash_4")?, "ahash_4")?;
        let ahash_5 = into_u32s(row.try_get("ahash_5")?, "ahash_5")?;
        let ahash_6 = into_u32s(row.try_get("ahash_6")?, "ahash_6")?;
        let ahash_7 = into_u32s(row.try_get("ahash_7")?, "ahash_7")?;
        Ok(Self {
            count: row.try_get("count")?,
            ahash_0,
            ahash_1,
            ahash_2,
            ahash_3,
            ahash_4,
            ahash_5,
            ahash_6,
            ahash_7,
        })
    }
}

impl sqlx::FromRow<'_, SqliteRow> for ReconHash {
    fn from_row(row: &SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            count: row.try_get("count")?,
            ahash_0: row.try_get("ahash_0")?,
            ahash_1: row.try_get("ahash_1")?,
            ahash_2: row.try_get("ahash_2")?,
            ahash_3: row.try_get("ahash_3")?,
            ahash_4: row.try_get("ahash_4")?,
            ahash_5: row.try_get("ahash_5")?,
            ahash_6: row.try_get("ahash_6")?,
            ahash_7: row.try_get("ahash_7")?,
        })
    }
}

impl ReconHash {
    pub fn count(&self) -> u64 {
        self.count as u64
    }
    pub fn hash(&self) -> [u32; 8] {
        [
            self.ahash_0,
            self.ahash_1,
            self.ahash_2,
            self.ahash_3,
            self.ahash_4,
            self.ahash_5,
            self.ahash_6,
            self.ahash_7,
        ]
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct EventValueRaw {
    pub order_key: Vec<u8>,
    pub codec: i64,
    pub root: bool,
    pub idx: i32,
    pub multihash: Vec<u8>,
    pub bytes: Vec<u8>,
}

impl EventValueRaw {
    pub fn into_block_row(self) -> Result<(EventId, BlockRow)> {
        let id = EventId::try_from(self.order_key)?;
        let hash = Multihash::from_bytes(&self.multihash[..])?;
        let code = self
            .codec
            .try_into()
            .context(format!("Invalid codec: {}", self.codec))?;
        let cid = Cid::new_v1(code, hash);

        Ok((
            id,
            BlockRow {
                cid,
                root: self.root,
                bytes: self.bytes,
            },
        ))
    }

    pub async fn into_carfiles(all_blocks: Vec<Self>) -> Result<Vec<(EventId, Vec<u8>)>> {
        // Consume all block into groups of blocks by their key.
        let all_blocks: Vec<(EventId, Vec<BlockRow>)> = process_results(
            all_blocks
                .into_iter()
                .map(|row| -> Result<(EventId, BlockRow), anyhow::Error> {
                    let (order_key, block) = row.into_block_row()?;

                    Ok((order_key, block))
                }),
            |blocks| {
                blocks
                    .group_by(|(key, _)| key.clone())
                    .into_iter()
                    .map(|(key, group)| {
                        (
                            key,
                            group.map(|(_key, block)| block).collect::<Vec<BlockRow>>(),
                        )
                    })
                    .collect()
            },
        )?;

        let mut values: Vec<(EventId, Vec<u8>)> = Vec::new();
        for (key, blocks) in all_blocks {
            if let Some(value) = rebuild_car(blocks).await? {
                values.push((key.clone(), value));
            }
        }

        Ok(values)
    }
}

#[derive(Debug, sqlx::FromRow)]
/// The query returns a column 'res' that is an int8
/// Be careful on postgres as some operations return int4 e.g. length()
pub struct CountRow {
    pub res: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OrderKey {
    pub order_key: Vec<u8>,
}

impl TryFrom<OrderKey> for EventId {
    type Error = ceramic_core::event_id::InvalidEventId;

    fn try_from(value: OrderKey) -> std::prelude::v1::Result<Self, Self::Error> {
        EventId::try_from(value.order_key)
    }
}

#[derive(sqlx::FromRow)]
pub struct DeliveredEvent {
    pub order_key: Vec<u8>,
    pub new_highwater_mark: i64,
}

impl DeliveredEvent {
    /// assumes rows are sorted by `delivered` ascending
    pub fn parse_query_results(current: i64, rows: Vec<Self>) -> Result<(i64, Vec<EventId>)> {
        let max: i64 = rows.last().map_or(current, |r| r.new_highwater_mark + 1);
        let rows = rows
            .into_iter()
            .map(|row| EventId::try_from(row.order_key))
            .collect::<Result<Vec<EventId>, EventIdError>>()?;

        Ok((max, rows))
    }
}
