use std::num::TryFromIntError;

use anyhow::anyhow;
use ceramic_core::EventId;
use cid::Cid;
use itertools::{process_results, Itertools};
use multihash_codetable::{Code, MultihashDigest};
use sqlx::{sqlite::SqliteRow, Row as _};

use crate::{
    sql::{rebuild_car, BlockHash, BlockRow},
    Error, Result,
};

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
            multihash: BlockHash::new(multihash),
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
