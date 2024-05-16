use cid::Cid;
use multihash_codetable::Multihash;

use crate::{
    sql::{
        entities::{BlockBytes, CountRow},
        query::BlockQuery,
        sqlite::SqliteTransaction,
    },
    Result, SqlitePool,
};

/// Access to the block table and related logic
pub struct CeramicOneBlock {}

impl CeramicOneBlock {
    /// Insert a block in a transaction (i.e. when creating an event)
    pub(crate) async fn insert(
        conn: &mut SqliteTransaction<'_>,
        hash: &Multihash,
        blob: &[u8],
    ) -> Result<bool> {
        let resp = sqlx::query(BlockQuery::put())
            .bind(hash.to_bytes())
            .bind(blob)
            .execute(&mut **conn.inner())
            .await;

        match resp {
            std::result::Result::Ok(_rows) => Ok(true),
            Err(sqlx::Error::Database(err)) => {
                if err.is_unique_violation() {
                    Ok(false)
                } else {
                    Err(sqlx::Error::Database(err).into())
                }
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Get the number of bytes for a block
    pub async fn get_size(pool: &SqlitePool, cid: &Cid) -> Result<usize> {
        let len: CountRow = sqlx::query_as(BlockQuery::length())
            .bind(cid.hash().to_bytes())
            .fetch_one(pool.reader())
            .await?;
        Ok(len.res as usize)
    }

    /// Retrieve a block from the database
    pub async fn get(pool: &SqlitePool, cid: &Cid) -> Result<Option<iroh_bitswap::Block>> {
        let block: Option<BlockBytes> = sqlx::query_as(BlockQuery::get())
            .bind(cid.hash().to_bytes())
            .fetch_optional(pool.reader())
            .await?;
        Ok(block.map(|b| iroh_bitswap::Block::new(b.bytes.into(), cid.to_owned())))
    }

    /// Check if a block exists in the database
    pub async fn has(pool: &SqlitePool, cid: &Cid) -> Result<bool> {
        let len: CountRow = sqlx::query_as(BlockQuery::has())
            .bind(cid.hash().to_bytes())
            .fetch_one(pool.reader())
            .await?;
        Ok(len.res > 0)
    }

    /// Add a block to the database
    pub async fn put(pool: &SqlitePool, block: &iroh_bitswap::Block) -> Result<bool> {
        let mut tx = pool.begin_tx().await?;
        let new = CeramicOneBlock::insert(&mut tx, block.cid().hash(), block.data()).await?;
        tx.commit().await?;
        Ok(new)
    }
}
