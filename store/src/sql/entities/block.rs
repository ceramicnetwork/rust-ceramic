use std::num::TryFromIntError;

use anyhow::anyhow;
use cid::Cid;
use multihash_codetable::Multihash;
use sqlx::{sqlite::SqliteRow, FromRow, Row};

use crate::Error;

use super::EventBlockRaw;

#[derive(Debug)]
/// A CID identified block of (ipfs) data
pub struct BlockRow {
    pub cid: Cid,
    pub root: bool,
    pub bytes: Vec<u8>,
}

// unfortunately, query! macros require the exact field names, not the FromRow implementation so we need to impl
// the decode/type stuff for CID/EventId to get that to work as expected
impl FromRow<'_, SqliteRow> for BlockRow {
    fn from_row(row: &SqliteRow) -> sqlx::Result<Self> {
        let codec: i64 = row.try_get("codec")?;
        let multihash: &[u8] = row.try_get("multihash")?;
        let root: bool = row.try_get("root")?;
        let bytes = row.try_get("bytes")?;
        let hash =
            Multihash::from_bytes(multihash).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        let cid = Cid::new_v1(
            codec
                .try_into()
                .map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
            hash,
        );
        Ok(Self { cid, root, bytes })
    }
}

#[derive(Debug, FromRow)]
pub struct BlockBytes {
    pub bytes: Vec<u8>,
}

impl TryFrom<EventBlockRaw> for BlockRow {
    type Error = Error;

    fn try_from(block: EventBlockRaw) -> Result<Self, Self::Error> {
        let hash = block.multihash.into_inner();
        let codec = block.codec.try_into().map_err(|e: TryFromIntError| {
            Error::new_app(anyhow!("Failed to parse block codec: {}", e))
        })?;

        let cid = Cid::new_v1(codec, hash);

        Ok(Self {
            cid,
            root: block.root,
            bytes: block.bytes,
        })
    }
}
