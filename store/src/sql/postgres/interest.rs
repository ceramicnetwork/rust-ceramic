#![warn(missing_docs, missing_debug_implementations, clippy::all)]

use anyhow::anyhow;
use async_trait::async_trait;
use ceramic_core::{Interest, RangeOpen};
use recon::{
    AssociativeHash, HashCount, InsertResult, Key, ReconItem, Result as ReconResult, Sha256a,
};
use sqlx::{Executor, Row};
use tracing::instrument;

use crate::{
    sql::{ReconHash, ReconQuery, ReconType, SqlBackend},
    DbTxPg, Error, PostgresPool, Result,
};

#[derive(Debug, Clone)]
/// InterestStore is a [`recon::Store`] implementation for Interests.
pub struct InterestStorePostgres {
    pub(crate) pool: PostgresPool,
}

impl InterestStorePostgres {
    /// Make a new InterestSqliteStore from a connection pool.
    pub async fn new(pool: PostgresPool) -> Result<Self> {
        let store = Self { pool };
        Ok(store)
    }
}

impl InterestStorePostgres {
    async fn insert_item(&self, key: &Interest) -> Result<bool> {
        let mut tx = self.pool.writer().begin().await?;
        let new_key = self.insert_item_int(key, &mut tx).await?;
        tx.commit().await?;
        Ok(new_key)
    }

    /// returns (new_key, new_val) tuple
    async fn insert_item_int(&self, item: &Interest, conn: &mut DbTxPg<'_>) -> Result<bool> {
        let new_key = self.insert_key_int(item, conn).await?;
        Ok(new_key)
    }

    async fn insert_key_int(&self, key: &Interest, conn: &mut DbTxPg<'_>) -> Result<bool> {
        conn.execute("SAVEPOINT insert_interest;")
            .await
            .map_err(|e| Error::from(e).context("interest savepoint error"))?;
        let key_insert = sqlx::query(ReconQuery::insert_interest());

        let hash = Sha256a::digest(key);
        let resp = key_insert
            .bind(key.as_bytes())
            .bind(hash.as_u32s()[0] as i64)
            .bind(hash.as_u32s()[1] as i64)
            .bind(hash.as_u32s()[2] as i64)
            .bind(hash.as_u32s()[3] as i64)
            .bind(hash.as_u32s()[4] as i64)
            .bind(hash.as_u32s()[5] as i64)
            .bind(hash.as_u32s()[6] as i64)
            .bind(hash.as_u32s()[7] as i64)
            .execute(&mut **conn)
            .await;
        match resp {
            std::result::Result::Ok(_rows) => Ok(true),
            Err(sqlx::Error::Database(err)) => {
                if err.is_unique_violation() {
                    conn.execute("ROLLBACK TO SAVEPOINT insert_interest;")
                        .await
                        .map_err(|e| Error::from(e).context("rollback error"))?;
                    Ok(false)
                } else {
                    Err(sqlx::Error::Database(err).into())
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
    ) -> Result<Box<dyn Iterator<Item = Interest> + Send + 'static>> {
        let offset: i64 = offset
            .try_into()
            .map_err(|_e| Error::new_app(anyhow!("offset too large")))?;
        let limit = limit.try_into().unwrap_or(i64::MAX);
        let query = sqlx::query(ReconQuery::range(ReconType::Interest));
        let rows = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .bind(limit)
            .bind(offset)
            .fetch_all(self.pool.reader())
            .await?;
        let rows = rows
            .into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row.get(0);
                Interest::try_from(bytes).map_err(|e| Error::new_app(anyhow!(e)))
            })
            .collect::<Result<Vec<Interest>>>()?;
        Ok(Box::new(rows.into_iter()))
    }
}

/// We intentionally expose the store to the API, separately from the recon::Store trait.
/// This allows us better control over the API functionality, particularly CRUD, that are related
/// to recon, but not explicitly part of the recon protocol. Eventually, it might be nice to reduce the
/// scope of the recon::Store trait (or remove the &self requirement), but for now we have both.
/// Anything that implements `ceramic_api::AccessInterestStore` should also implement `recon::Store`.
/// This guarantees that regardless of entry point (api or recon), the data is stored and retrieved in the same way.
#[async_trait::async_trait]
impl ceramic_api::AccessInterestStore for InterestStorePostgres {
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
impl recon::Store for InterestStorePostgres {
    type Key = Interest;
    type Hash = Sha256a;

    /// Returns true if the key was new. The value is always updated if included
    async fn insert(&self, item: &ReconItem<'_, Interest>) -> ReconResult<bool> {
        // interests don't have values, if someone gives us something we throw an error but allow None/vec![]
        if let Some(val) = item.value {
            if !val.is_empty() {
                return Err(recon::Error::new_app(anyhow!(
                    "Interests do not support values! Invalid request."
                )));
            }
        }
        Ok(self.insert_item(item.key).await?)
    }

    /// Insert new keys into the key space.
    /// Returns true if a key did not previously exist.
    async fn insert_many(&self, items: &[ReconItem<'_, Interest>]) -> ReconResult<InsertResult> {
        match items.len() {
            0 => Ok(InsertResult::new(vec![], 0)),
            _ => {
                let mut results = vec![false; items.len()];
                let mut new_val_cnt = 0;
                let mut tx = self.pool.writer().begin().await.map_err(Error::from)?;

                for (idx, item) in items.iter().enumerate() {
                    let new_key = self.insert_item_int(item.key, &mut tx).await?;
                    results[idx] = new_key;
                    if item.value.is_some() {
                        new_val_cnt += 1;
                    }
                }
                tx.commit().await.map_err(Error::from)?;
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
    ) -> ReconResult<HashCount<Self::Hash>> {
        if left_fencepost >= right_fencepost {
            return Ok(HashCount::new(Self::Hash::identity(), 0));
        }

        let res: ReconHash = sqlx::query_as(ReconQuery::hash_range(
            ReconType::Interest,
            SqlBackend::Postgres,
        ))
        .bind(left_fencepost.as_bytes())
        .bind(right_fencepost.as_bytes())
        .fetch_one(self.pool.reader())
        .await
        .map_err(Error::from)?;
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
    ) -> ReconResult<Box<dyn Iterator<Item = Interest> + Send + 'static>> {
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
    ) -> ReconResult<Box<dyn Iterator<Item = (Interest, Vec<u8>)> + Send + 'static>> {
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
    ) -> ReconResult<usize> {
        let query = sqlx::query(ReconQuery::count(ReconType::Interest, SqlBackend::Postgres));
        let row = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_one(self.pool.reader())
            .await
            .map_err(Error::from)?;
        Ok(row.get::<'_, i64, _>(0) as usize)
    }

    /// Return the first key within the range.
    #[instrument(skip(self))]
    async fn first(
        &self,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
    ) -> ReconResult<Option<Interest>> {
        let query = sqlx::query(ReconQuery::first_key(ReconType::Interest));
        let rows = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_all(self.pool.reader())
            .await
            .map_err(Error::from)?;
        Ok(rows
            .first()
            .map(|row| {
                let bytes: Vec<u8> = row.get(0);
                Interest::try_from(bytes).map_err(|e| Error::new_app(anyhow!(e)))
            })
            .transpose()?)
    }

    #[instrument(skip(self))]
    async fn last(
        &self,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
    ) -> ReconResult<Option<Interest>> {
        let query = sqlx::query(ReconQuery::last_key(ReconType::Interest));
        let rows = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_all(self.pool.reader())
            .await
            .map_err(Error::from)?;
        Ok(rows
            .first()
            .map(|row| {
                let bytes: Vec<u8> = row.get(0);
                Interest::try_from(bytes).map_err(|e| Error::new_app(anyhow!(e)))
            })
            .transpose()?)
    }

    #[instrument(skip(self))]
    async fn first_and_last(
        &self,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
    ) -> ReconResult<Option<(Interest, Interest)>> {
        let query = sqlx::query(ReconQuery::first_and_last(
            ReconType::Interest,
            SqlBackend::Postgres,
        ));
        let rows = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_all(self.pool.reader())
            .await
            .map_err(Error::from)?;
        if let Some(row) = rows.first() {
            let f_bytes: Vec<u8> = row.get(0);
            let l_bytes: Vec<u8> = row.get(1);
            let first = Interest::try_from(f_bytes).map_err(|e| Error::new_app(anyhow!(e)))?;
            let last = Interest::try_from(l_bytes).map_err(|e| Error::new_app(anyhow!(e)))?;
            Ok(Some((first, last)))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self))]
    async fn value_for_key(&self, _key: &Interest) -> ReconResult<Option<Vec<u8>>> {
        Ok(Some(vec![]))
    }

    #[instrument(skip(self))]
    async fn keys_with_missing_values(
        &self,
        _range: RangeOpen<Interest>,
    ) -> ReconResult<Vec<Interest>> {
        Ok(vec![])
    }
}
