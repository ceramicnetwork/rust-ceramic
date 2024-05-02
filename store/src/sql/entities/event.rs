use std::num::TryFromIntError;

use ceramic_core::EventId;
use cid::Cid;
use iroh_car::{CarHeader, CarReader, CarWriter};
use itertools::{process_results, Itertools};
use multihash::Multihash;

use sqlx::{sqlite::SqliteRow, Row as _};
use std::collections::BTreeSet;

use crate::{sql::BlockRow, Error, Result};

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

impl sqlx::FromRow<'_, SqliteRow> for ReconHash {
    fn from_row(row: &SqliteRow) -> std::result::Result<Self, sqlx::Error> {
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

use anyhow::anyhow;
use multihash_codetable::{Code, MultihashDigest};

#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::Type)]
pub struct BlockHash(Multihash<64>);

impl BlockHash {
    pub fn try_from_vec(data: &[u8]) -> Result<Self> {
        Ok(Self(Multihash::from_bytes(data).map_err(Error::new_app)?))
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    pub fn inner(&self) -> &Multihash<64> {
        &self.0
    }
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

#[derive(Debug, Clone)]
pub struct EventBlockRaw {
    pub order_key: Vec<u8>,
    pub codec: i64,
    pub root: bool,
    pub idx: i32,
    pub multihash: BlockHash,
    pub bytes: Vec<u8>,
}

impl sqlx::FromRow<'_, SqliteRow> for EventBlockRaw {
    fn from_row(row: &SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        let multihash: Vec<u8> = row.try_get("multihash")?;
        let multihash =
            BlockHash::try_from_vec(&multihash).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        Ok(Self {
            order_key: row.try_get("order_key")?,
            codec: row.try_get("codec")?,
            root: row.try_get("root")?,
            idx: row.try_get("idx")?,
            multihash,
            bytes: row.try_get("bytes")?,
        })
    }
}

impl EventBlockRaw {
    pub fn try_new(key: &EventId, idx: i32, root: bool, cid: Cid, bytes: Vec<u8>) -> Result<Self> {
        let multihash = match cid.hash().code() {
            0x12 => Code::Sha2_256.digest(&bytes),
            0x1b => Code::Keccak256.digest(&bytes),
            0x11 => return Err(Error::new_app(anyhow!("Sha1 not supported"))),
            code => {
                return Err(Error::new_app(anyhow!(
                    "multihash type {:#x} not Sha2_256, Keccak256",
                    code,
                )))
            }
        };

        if cid.hash().to_bytes() != multihash.to_bytes() {
            return Err(Error::new_app(anyhow!(
                "cid did not match blob {} != {}",
                hex::encode(cid.hash().to_bytes()),
                hex::encode(multihash.to_bytes())
            )));
        }

        let codec: i64 = cid.codec().try_into().map_err(|e: TryFromIntError| {
            Error::new_app(anyhow!(e).context(format!(
                "Invalid codec could not fit into an i64: {}",
                cid.codec()
            )))
        })?;
        let order_key = key
            .cid()
            .ok_or_else(|| Error::new_app(anyhow!("Event CID is required")))?
            .to_bytes();

        Ok(Self {
            order_key,
            codec,
            root,
            idx,
            multihash: BlockHash(multihash),
            bytes,
        })
    }

    pub fn into_block_row(self) -> Result<(EventId, BlockRow)> {
        let id = EventId::try_from(self.order_key).map_err(Error::new_app)?;
        let hash = self.multihash.inner().to_owned();
        let code = self.codec.try_into().map_err(|e: TryFromIntError| {
            let er = anyhow::anyhow!(e).context(format!("Invalid codec: {}", self.codec));
            Error::new_app(er)
        })?;

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
                .map(|row| -> Result<(EventId, BlockRow)> {
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
    pub cid: Vec<u8>,
    pub new_highwater_mark: i64,
}

impl DeliveredEvent {
    /// assumes rows are sorted by `delivered` ascending
    pub fn parse_query_results(current: i64, rows: Vec<Self>) -> Result<(i64, Vec<Cid>)> {
        let max: i64 = rows.last().map_or(current, |r| r.new_highwater_mark + 1);
        let rows = rows
            .into_iter()
            .map(|row| Cid::try_from(row.cid).map_err(Error::new_app))
            .collect::<Result<Vec<Cid>>>()?;

        Ok((max, rows))
    }
}
