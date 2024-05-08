#![warn(missing_docs, missing_debug_implementations, clippy::all)]

use std::ops::Range;

use anyhow::anyhow;
use async_trait::async_trait;
use ceramic_core::Interest;
use recon::{
    AssociativeHash, Error as ReconError, HashCount, InsertResult, Key, ReconItem,
    Result as ReconResult, Sha256a,
};
use sqlx::Row;
use tracing::instrument;

use crate::{
    sql::{ReconHash, ReconQuery, ReconType, SqlBackend},
    DbTxSqlite, Error, Result, SqlitePool,
};

#[derive(Debug, Clone)]
/// InterestStore is a [`recon::Store`] implementation for Interests.
pub struct SqliteInterestStore {
    pub(crate) pool: SqlitePool,
}

impl SqliteInterestStore {
    /// Make a new InterestSqliteStore from a connection pool.
    pub async fn new(pool: SqlitePool) -> Result<Self> {
        let store = SqliteInterestStore { pool };
        Ok(store)
    }
}

type InterestError = <Interest as TryFrom<Vec<u8>>>::Error;

impl SqliteInterestStore {
    async fn insert_item(&self, key: &Interest) -> Result<bool> {
        let mut tx = self.pool.writer().begin().await?;
        let new_key = self.insert_item_int(key, &mut tx).await?;
        tx.commit().await?;
        Ok(new_key)
    }

    /// returns (new_key, new_val) tuple
    async fn insert_item_int(&self, item: &Interest, conn: &mut DbTxSqlite<'_>) -> Result<bool> {
        let new_key = self.insert_key_int(item, conn).await?;
        Ok(new_key)
    }

    async fn insert_key_int(&self, key: &Interest, conn: &mut DbTxSqlite<'_>) -> Result<bool> {
        let key_insert = sqlx::query(ReconQuery::insert_interest());

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
            .execute(&mut **conn)
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

    async fn range_int(
        &self,
        range: Range<&Interest>,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Interest> + Send + 'static>> {
        let query = sqlx::query(ReconQuery::range(ReconType::Interest));
        let rows = query
            .bind(range.start.as_bytes())
            .bind(range.end.as_bytes())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(self.pool.reader())
            .await?;
        let rows = rows
            .into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row.get(0);
                Interest::try_from(bytes)
            })
            .collect::<std::result::Result<Vec<Interest>, InterestError>>()
            .map_err(|e| Error::new_app(anyhow!(e)))?;
        Ok(Box::new(rows.into_iter()))
    }
}

/// We intentionally expose the store to the API, separately from the recon::Store trait.
/// This allows us better control over the API functionality, particularly CRUD, that are related
/// to recon, but not explicitly part of the recon protocol. Eventually, it might be nice to reduce the
/// scope of the recon::Store trait (or remove the &mut self requirement), but for now we have both.
/// Anything that implements `ceramic_api::AccessInterestStore` should also implement `recon::Store`.
/// This guarantees that regardless of entry point (api or recon), the data is stored and retrieved in the same way.
#[async_trait::async_trait]
impl ceramic_api::AccessInterestStore for SqliteInterestStore {
    async fn insert(&self, key: Interest) -> anyhow::Result<bool> {
        Ok(self.insert_item(&key).await?)
    }
    async fn range(
        &self,
        start: &Interest,
        end: &Interest,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<Interest>> {
        Ok(self.range_int(start..end, offset, limit).await?.collect())
    }
}

#[async_trait]
impl recon::Store for SqliteInterestStore {
    type Key = Interest;
    type Hash = Sha256a;

    /// Returns true if the key was new. The value is always updated if included
    async fn insert(&self, item: &ReconItem<'_, Interest>) -> ReconResult<bool> {
        // interests don't have values, if someone gives us something we throw an error but allow vec![]
        if !item.value.is_empty() {
            return Err(ReconError::new_app(anyhow!(
                "Interests do not support values! Invalid request."
            )));
        }
        Ok(self.insert_item(item.key).await?)
    }

    /// Insert new keys into the key space.
    /// Returns true if a key did not previously exist.
    async fn insert_many(&self, items: &[ReconItem<'_, Interest>]) -> ReconResult<InsertResult> {
        match items.len() {
            0 => Ok(InsertResult::new(vec![])),
            _ => {
                let mut results = vec![false; items.len()];
                let mut tx = self.pool.writer().begin().await.map_err(Error::from)?;

                for (idx, item) in items.iter().enumerate() {
                    let new_key = self.insert_item_int(item.key, &mut tx).await?;
                    results[idx] = new_key;
                }
                tx.commit().await.map_err(Error::from)?;
                Ok(InsertResult::new(results))
            }
        }
    }

    /// return the hash and count for a range
    #[instrument(skip(self))]
    async fn hash_range(&self, range: Range<&Interest>) -> ReconResult<HashCount<Self::Hash>> {
        if range.start >= range.end {
            return Ok(HashCount::new(Sha256a::identity(), 0));
        }

        let res: ReconHash = sqlx::query_as(ReconQuery::hash_range(
            ReconType::Interest,
            SqlBackend::Sqlite,
        ))
        .bind(range.start.as_bytes())
        .bind(range.end.as_bytes())
        .fetch_one(self.pool.reader())
        .await
        .map_err(Error::from)?;
        let bytes = res.hash();
        Ok(HashCount::new(Self::Hash::from(bytes), res.count()))
    }

    #[instrument(skip(self))]
    async fn range(
        &self,
        range: Range<&Interest>,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = Interest> + Send + 'static>> {
        Ok(self.range_int(range, offset, limit).await?)
    }

    #[instrument(skip(self))]
    /// Interests don't have values, so the value will always be an empty vec. Use `range` instead.
    async fn range_with_values(
        &self,
        range: Range<&Interest>,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = (Interest, Vec<u8>)> + Send + 'static>> {
        let rows = self.range(range, offset, limit).await?;
        Ok(Box::new(rows.into_iter().map(|key| (key, Vec::new()))))
    }
    /// Return the number of keys within the range.
    #[instrument(skip(self))]
    async fn count(&self, range: Range<&Interest>) -> ReconResult<usize> {
        let row = sqlx::query(ReconQuery::count(ReconType::Interest, SqlBackend::Sqlite))
            .bind(range.start.as_bytes())
            .bind(range.end.as_bytes())
            .fetch_one(self.pool.reader())
            .await
            .map_err(Error::from)?;

        Ok(row.get::<'_, i64, _>(0) as usize)
    }

    #[instrument(skip(self))]
    async fn value_for_key(&self, _key: &Interest) -> ReconResult<Option<Vec<u8>>> {
        Ok(Some(vec![]))
    }
}
