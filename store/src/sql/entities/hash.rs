use multihash::Multihash;

use sqlx::{sqlite::SqliteRow, Row as _};

use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Hash, sqlx::Type)]
pub struct BlockHash(Multihash<64>);

impl BlockHash {
    pub fn new(hash: Multihash<64>) -> Self {
        Self(hash)
    }

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
