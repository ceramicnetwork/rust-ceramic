#![warn(missing_docs, missing_debug_implementations, clippy::all)]

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use ceramic_core::{Interest, RangeOpen};
use recon::{AssociativeHash, HashCount, InsertResult, Key, ReconItem, Sha256a};
use sqlx::Row;
use tracing::instrument;

use crate::{
    sql::{ReconHash, ReconQuery, ReconType, SqlBackend},
    DbTxSqlite, SqlitePool, StoreError, StoreResult,
};

#[derive(Debug, Clone)]
/// InterestStore is a [`recon::Store`] implementation for Interests.
pub struct InterestStoreSqlite {
    pub(crate) pool: SqlitePool,
}

impl InterestStoreSqlite {
    /// Make a new InterestSqliteStore from a connection pool.
    pub async fn new(pool: SqlitePool) -> StoreResult<Self> {
        let store = InterestStoreSqlite { pool };
        Ok(store)
    }
}

type InterestError = <Interest as TryFrom<Vec<u8>>>::Error;

impl InterestStoreSqlite {
    async fn insert_item(&self, key: &Interest) -> StoreResult<bool> {
        let mut tx = self.pool.writer().begin().await?;
        let new_key = self.insert_item_int(key, &mut tx).await?;
        tx.commit().await?;
        Ok(new_key)
    }

    /// returns (new_key, new_val) tuple
    async fn insert_item_int(
        &self,
        item: &Interest,
        conn: &mut DbTxSqlite<'_>,
    ) -> StoreResult<bool> {
        let new_key = self.insert_key_int(item, conn).await?;
        Ok(new_key)
    }

    async fn insert_key_int(&self, key: &Interest, conn: &mut DbTxSqlite<'_>) -> StoreResult<bool> {
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
                    Err(StoreError::from(sqlx::Error::Database(err)))
                }
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn range_int(
        &self,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
        offset: usize,
        limit: usize,
    ) -> StoreResult<Box<dyn Iterator<Item = Interest> + Send + 'static>> {
        let query = sqlx::query(ReconQuery::range(ReconType::Interest));
        let rows = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
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
            .map_err(|e| StoreError::new_app(anyhow!(e)))?;
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
impl ceramic_api::AccessInterestStore for InterestStoreSqlite {
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
        Ok(self.range_int(start, end, offset, limit).await?.collect())
    }
}

#[async_trait]
impl recon::Store for InterestStoreSqlite {
    type Key = Interest;
    type Hash = Sha256a;

    /// Returns true if the key was new. The value is always updated if included
    async fn insert(&self, item: &ReconItem<'_, Interest>) -> anyhow::Result<bool> {
        // interests don't have values, if someone gives us something we throw an error but allow None/vec![]
        if let Some(val) = item.value {
            if !val.is_empty() {
                return Err(anyhow!("Interests do not support values! Invalid request."));
            }
        }
        Ok(self.insert_item(item.key).await?)
    }

    /// Insert new keys into the key space.
    /// Returns true if a key did not previously exist.
    async fn insert_many(&self, items: &[ReconItem<'_, Interest>]) -> anyhow::Result<InsertResult> {
        match items.len() {
            0 => Ok(InsertResult::new(vec![], 0)),
            _ => {
                let mut results = vec![false; items.len()];
                let mut new_val_cnt = 0;
                let mut tx = self.pool.writer().begin().await?;

                for (idx, item) in items.iter().enumerate() {
                    let new_key = self.insert_item_int(item.key, &mut tx).await?;
                    results[idx] = new_key;
                    if item.value.is_some() {
                        new_val_cnt += 1;
                    }
                }
                tx.commit().await?;
                Ok(InsertResult::new(results, new_val_cnt))
            }
        }
    }

    /// return the hash and count for a range
    #[instrument(skip(self))]
    async fn hash_range(
        &self,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
    ) -> anyhow::Result<HashCount<Self::Hash>> {
        if left_fencepost >= right_fencepost {
            return Ok(HashCount::new(Self::Hash::identity(), 0));
        }

        let res: ReconHash = sqlx::query_as(ReconQuery::hash_range(
            ReconType::Interest,
            SqlBackend::Sqlite,
        ))
        .bind(left_fencepost.as_bytes())
        .bind(right_fencepost.as_bytes())
        .fetch_one(self.pool.reader())
        .await
        .context("interest range")?;
        let bytes = res.hash();
        Ok(HashCount::new(Self::Hash::from(bytes), res.count()))
    }

    #[instrument(skip(self))]
    async fn range(
        &self,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Box<dyn Iterator<Item = Interest> + Send + 'static>> {
        Ok(self
            .range_int(left_fencepost, right_fencepost, offset, limit)
            .await?)
    }

    #[instrument(skip(self))]
    /// Interests don't have values, so the value will always be an empty vec. Use `range` instead.
    async fn range_with_values(
        &self,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Box<dyn Iterator<Item = (Interest, Vec<u8>)> + Send + 'static>> {
        let rows = self
            .range(left_fencepost, right_fencepost, offset, limit)
            .await?;
        Ok(Box::new(rows.into_iter().map(|key| (key, Vec::new()))))
    }
    /// Return the number of keys within the range.
    #[instrument(skip(self))]
    async fn count(
        &self,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
    ) -> anyhow::Result<usize> {
        let row = sqlx::query(ReconQuery::count(ReconType::Interest, SqlBackend::Sqlite))
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_one(self.pool.reader())
            .await?;

        Ok(row.get::<'_, i64, _>(0) as usize)
    }

    /// Return the first key within the range.
    #[instrument(skip(self))]
    async fn first(
        &self,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
    ) -> anyhow::Result<Option<Interest>> {
        let query = sqlx::query(ReconQuery::first_key(ReconType::Interest));

        let rows = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        Ok(rows
            .first()
            .map(|row| {
                let bytes: Vec<u8> = row.get(0);
                Interest::try_from(bytes)
            })
            .transpose()?)
    }

    #[instrument(skip(self))]
    async fn last(
        &self,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
    ) -> anyhow::Result<Option<Interest>> {
        let query = sqlx::query(ReconQuery::last_key(ReconType::Interest));
        let rows = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        Ok(rows
            .first()
            .map(|row| {
                let bytes: Vec<u8> = row.get(0);
                Interest::try_from(bytes)
            })
            .transpose()?)
    }

    #[instrument(skip(self))]
    async fn first_and_last(
        &self,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
    ) -> anyhow::Result<Option<(Interest, Interest)>> {
        let query = sqlx::query(ReconQuery::first_and_last(
            ReconType::Interest,
            SqlBackend::Sqlite,
        ));
        let rows = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        if let Some(row) = rows.first() {
            let f_bytes: Vec<u8> = row.get(0);
            let l_bytes: Vec<u8> = row.get(1);
            let first = Interest::try_from(f_bytes)?;
            let last = Interest::try_from(l_bytes)?;
            Ok(Some((first, last)))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self))]
    async fn value_for_key(&self, _key: &Interest) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(Some(vec![]))
    }

    #[instrument(skip(self))]
    async fn keys_with_missing_values(
        &self,
        _range: RangeOpen<Interest>,
    ) -> anyhow::Result<Vec<Interest>> {
        Ok(vec![])
    }
}
