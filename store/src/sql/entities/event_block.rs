use std::num::TryFromIntError;

use anyhow::anyhow;
use ceramic_core::EventId;
use ceramic_event::unvalidated;
use cid::Cid;
use ipld_core::ipld::Ipld;
use itertools::{process_results, Itertools};
use multihash_codetable::{Code, MultihashDigest};
use sqlx::{sqlite::SqliteRow, Row as _};

use crate::{
    sql::entities::{rebuild_car, BlockHash, BlockRow},
    Error, Result,
};

// TODO: make type private
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub struct EventBlockRaw {
    pub event_cid: Vec<u8>,
    pub codec: i64,
    pub root: bool,
    pub idx: i32,
    pub multihash: BlockHash,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ReconEventBlockRaw {
    pub order_key: EventId,
    pub block: EventBlockRaw,
}

impl ReconEventBlockRaw {
    pub async fn into_carfiles(all_blocks: Vec<Self>) -> Result<Vec<(EventId, Vec<u8>)>> {
        // Consume all block into groups of blocks by their key. This assumes the blocks are ordered by idx for an event
        let all_blocks: Vec<(EventId, Vec<BlockRow>)> = process_results(
            all_blocks
                .into_iter()
                .map(|row| -> Result<(EventId, BlockRow)> {
                    let block = row.block.try_into()?;

                    Ok((row.order_key, block))
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

    pub async fn into_events(
        all_blocks: Vec<Self>,
    ) -> Result<Vec<(cid::Cid, unvalidated::Event<Ipld>)>> {
        // TODO(AES-311): Turn the blocks into Events in a single pass, instead of first
        // turning them into a CAR file and then turning the CAR file into the Event.
        let parsed = Self::into_carfiles(all_blocks).await?;
        let mut res = Vec::with_capacity(parsed.len());
        for (_, carfile) in parsed {
            let (cid, parsed_event) =
                unvalidated::Event::<Ipld>::decode_car(carfile.as_slice(), false)
                    .map_err(Error::new_app)?;
            res.push((cid, parsed_event));
        }
        Ok(res)
    }
}

impl sqlx::FromRow<'_, SqliteRow> for ReconEventBlockRaw {
    fn from_row(row: &SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        let block = EventBlockRaw::from_row(row)?;
        let order_key: Vec<u8> = row.try_get("order_key")?;
        let order_key =
            EventId::try_from(order_key).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        Ok(Self { order_key, block })
    }
}

impl sqlx::FromRow<'_, SqliteRow> for EventBlockRaw {
    fn from_row(row: &SqliteRow) -> std::result::Result<Self, sqlx::Error> {
        let multihash: Vec<u8> = row.try_get("multihash")?;
        let multihash =
            BlockHash::try_from_vec(&multihash).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        Ok(Self {
            event_cid: row.try_get("event_cid")?,
            codec: row.try_get("codec")?,
            root: row.try_get("root")?,
            idx: row.try_get("idx")?,
            multihash,
            bytes: row.try_get("bytes")?,
        })
    }
}

impl EventBlockRaw {
    #[allow(missing_docs)]
    pub fn try_new(
        event_cid: &Cid,
        idx: i32,
        root: bool,
        cid: Cid,
        bytes: Vec<u8>,
    ) -> Result<Self> {
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

        Ok(Self {
            event_cid: event_cid.to_bytes(),
            codec,
            root,
            idx,
            multihash: BlockHash::new(multihash),
            bytes,
        })
    }

    /// CID of the block
    pub fn cid(&self) -> Cid {
        Cid::new_v1(self.codec as u64, self.multihash.clone().into_inner())
    }
}
