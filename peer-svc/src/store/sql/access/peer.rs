#![warn(missing_docs, missing_debug_implementations, clippy::all)]

use std::ops::Range;

use anyhow::anyhow;

use ceramic_core::PeerKey;
use recon::{AssociativeHash, HashCount, InsertResult, Key, Sha256a};
use sqlx::Row;

use crate::store::{
    sql::{
        entities::ReconHash,
        query::{ReconQuery, SqlBackend},
        SqliteTransaction,
    },
    Error, Result, SqlitePool,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Entity for storing PeerKey in the database.
pub struct PeerDB {}

type PeerKeyError = <PeerKey as TryFrom<Vec<u8>>>::Error;

impl PeerDB {
    async fn insert_tx<'a>(conn: &mut SqliteTransaction<'a>, key: &PeerKey) -> Result<bool> {
        let key_insert = sqlx::query(ReconQuery::insert_peer());

        let hash = Sha256a::digest(key);
        let resp = key_insert
            .bind(key.as_bytes())
            .bind(hash.as_u32s()[0])
            .bind(hash.as_u32s()[1])
            .bind(hash.as_u32s()[2])
            .bind(hash.as_u32s()[3])
            .bind(hash.as_u32s()[4])
            .bind(hash.as_u32s()[5])
            .bind(hash.as_u32s()[6])
            .bind(hash.as_u32s()[7])
            .execute(&mut **conn.inner())
            .await;
        match resp {
            std::result::Result::Ok(_rows) => Ok(true),
            Err(sqlx::Error::Database(err)) => {
                if err.is_unique_violation() {
                    Ok(false)
                } else {
                    Err(Error::from(sqlx::Error::Database(err)))
                }
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Insert a single peer into the database.
    pub async fn insert(pool: &SqlitePool, key: &PeerKey) -> Result<bool> {
        let mut tx = pool.begin_tx().await.map_err(Error::from)?;
        let new_key = PeerDB::insert_tx(&mut tx, key).await?;
        tx.commit().await.map_err(Error::from)?;
        Ok(new_key)
    }

    /// Insert a multiple peers into the database.
    pub async fn insert_many(
        pool: &SqlitePool,
        items: &[&PeerKey],
    ) -> Result<InsertResult<PeerKey>> {
        match items.len() {
            0 => Ok(InsertResult::new(0)),
            _ => {
                let mut results = 0;
                let mut tx = pool.begin_tx().await.map_err(Error::from)?;

                for item in items.iter() {
                    PeerDB::insert_tx(&mut tx, item)
                        .await?
                        .then(|| results += 1);
                }
                tx.commit().await.map_err(Error::from)?;
                Ok(InsertResult::new(results))
            }
        }
    }

    /// Calculate the Sha256a hash of all keys in the range.
    pub async fn hash_range(
        pool: &SqlitePool,
        range: Range<&PeerKey>,
    ) -> Result<HashCount<Sha256a>> {
        if range.start >= range.end {
            return Ok(HashCount::new(Sha256a::identity(), 0));
        }

        let res: ReconHash = sqlx::query_as(ReconQuery::hash_range(SqlBackend::Sqlite))
            .bind(range.start.as_bytes())
            .bind(range.end.as_bytes())
            .fetch_one(pool.reader())
            .await
            .map_err(Error::from)?;
        let bytes = res.hash();
        Ok(HashCount::new(Sha256a::from(bytes), res.count()))
    }

    /// Find the keys in the range
    pub async fn range(pool: &SqlitePool, range: Range<&PeerKey>) -> Result<Vec<PeerKey>> {
        let query = sqlx::query(ReconQuery::range());
        let rows = query
            .bind(range.start.as_bytes())
            .bind(range.end.as_bytes())
            .fetch_all(pool.reader())
            .await?;
        let rows = rows
            .into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row.get(0);
                PeerKey::try_from(bytes)
            })
            .collect::<std::result::Result<Vec<PeerKey>, PeerKeyError>>()
            .map_err(|e| Error::new_app(anyhow!(e)))?;
        Ok(rows)
    }

    /// Find the first key in the range
    pub async fn first(pool: &SqlitePool, range: Range<&PeerKey>) -> Result<Option<PeerKey>> {
        sqlx::query(ReconQuery::first())
            .bind(range.start.as_bytes())
            .bind(range.end.as_bytes())
            .fetch_optional(pool.reader())
            .await?
            .map(|row| {
                let bytes: Vec<u8> = row.get(0);
                PeerKey::try_from(bytes)
            })
            .transpose()
            .map_err(|e| Error::new_app(anyhow!(e)))
    }

    /// Find the approximate middle key in the range
    pub async fn middle(pool: &SqlitePool, range: Range<&PeerKey>) -> Result<Option<PeerKey>> {
        let count = Self::count(pool, range.clone()).await?;
        // (usize::MAX / 2) == i64::MAX, meaning it should always fit inside an i64.
        // However to be safe we default to i64::MAX.
        let half: i64 = (count / 2).try_into().unwrap_or(i64::MAX);
        sqlx::query(ReconQuery::middle())
            .bind(range.start.as_bytes())
            .bind(range.end.as_bytes())
            .bind(half)
            .fetch_optional(pool.reader())
            .await?
            .map(|row| {
                let bytes: Vec<u8> = row.get(0);
                PeerKey::try_from(bytes)
            })
            .transpose()
            .map_err(|e| Error::new_app(anyhow!(e)))
    }

    pub(crate) async fn delete_range(pool: &SqlitePool, range: Range<&PeerKey>) -> Result<()> {
        let query = sqlx::query(ReconQuery::delete_range());
        query
            .bind(range.start.as_bytes())
            .bind(range.end.as_bytes())
            .execute(pool.writer())
            .await?;
        Ok(())
    }

    /// Count the number of keys in a given range
    pub async fn count(pool: &SqlitePool, range: Range<&PeerKey>) -> Result<usize> {
        let row = sqlx::query(ReconQuery::count(SqlBackend::Sqlite))
            .bind(range.start.as_bytes())
            .bind(range.end.as_bytes())
            .fetch_one(pool.reader())
            .await
            .map_err(Error::from)?;

        Ok(row.get::<'_, i64, _>(0) as usize)
    }
}
