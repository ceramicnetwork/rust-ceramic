#![warn(missing_docs, missing_debug_implementations, clippy::all)]

use anyhow::anyhow;

use ceramic_core::Interest;
use recon::{AssociativeHash, HashCount, InsertResult, Key, Sha256a};
use sqlx::Row;

use crate::{
    sql::{ReconHash, ReconQuery, ReconType, SqlBackend},
    DbTxSqlite, Error, Result, SqlitePool,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Entity for storing Interests in the database.
pub struct CeramicOneInterest {}

type InterestError = <Interest as TryFrom<Vec<u8>>>::Error;

impl CeramicOneInterest {
    async fn insert_tx<'a>(conn: &mut DbTxSqlite<'a>, key: &Interest) -> Result<bool> {
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
}

impl CeramicOneInterest {
    /// Insert a single interest into the database.
    pub async fn insert(pool: &SqlitePool, key: &Interest) -> Result<bool> {
        let mut tx = pool.writer().begin().await.map_err(Error::from)?;
        let new_key = CeramicOneInterest::insert_tx(&mut tx, key).await?;
        tx.commit().await.map_err(Error::from)?;
        Ok(new_key)
    }

    /// Insert a multiple interests into the database.
    pub async fn insert_many(pool: &SqlitePool, items: &[&Interest]) -> Result<InsertResult> {
        match items.len() {
            0 => Ok(InsertResult::new(vec![], 0)),
            _ => {
                let mut results = vec![false; items.len()];
                let mut new_val_cnt = 0; // interests don't have values, so new keys count for both
                let mut tx = pool.writer().begin().await.map_err(Error::from)?;

                for (idx, item) in items.iter().enumerate() {
                    let new_key = CeramicOneInterest::insert_tx(&mut tx, item).await?;
                    results[idx] = new_key;
                    if new_key {
                        new_val_cnt += 1;
                    }
                }
                tx.commit().await.map_err(Error::from)?;
                Ok(InsertResult::new(results, new_val_cnt))
            }
        }
    }

    /// Calculate the Sha256a hash of all keys in the range between left_fencepost and right_fencepost.
    pub async fn hash_range(
        pool: &SqlitePool,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
    ) -> Result<HashCount<Sha256a>> {
        if left_fencepost >= right_fencepost {
            return Ok(HashCount::new(Sha256a::identity(), 0));
        }

        let res: ReconHash = sqlx::query_as(ReconQuery::hash_range(
            ReconType::Interest,
            SqlBackend::Sqlite,
        ))
        .bind(left_fencepost.as_bytes())
        .bind(right_fencepost.as_bytes())
        .fetch_one(pool.reader())
        .await
        .map_err(Error::from)?;
        let bytes = res.hash();
        Ok(HashCount::new(Sha256a::from(bytes), res.count()))
    }

    /// Find the interestsin the range between left_fencepost and right_fencepost.
    pub async fn range(
        pool: &SqlitePool,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Interest>> {
        let query = sqlx::query(ReconQuery::range(ReconType::Interest));
        let rows = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(pool.reader())
            .await?;
        let rows = rows
            .into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row.get(0);
                Interest::try_from(bytes)
            })
            .collect::<std::result::Result<Vec<Interest>, InterestError>>()
            .map_err(|e| Error::new_app(anyhow!(e)))?;
        Ok(rows)
    }

    /// Count the number of keys in a given range
    pub async fn count(
        pool: &SqlitePool,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
    ) -> Result<usize> {
        let row = sqlx::query(ReconQuery::count(ReconType::Interest, SqlBackend::Sqlite))
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_one(pool.reader())
            .await
            .map_err(Error::from)?;

        Ok(row.get::<'_, i64, _>(0) as usize)
    }

    /// Return the first key within the range.
    pub async fn first(
        pool: &SqlitePool,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
    ) -> Result<Option<Interest>> {
        let query = sqlx::query(ReconQuery::first_key(ReconType::Interest));

        let row = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_optional(pool.reader())
            .await
            .map_err(Error::from)?;
        row.map(|row| {
            let bytes: Vec<u8> = row.get(0);
            Interest::try_from(bytes).map_err(|e| Error::new_app(anyhow!(e)))
        })
        .transpose()
    }

    /// Return the last key in the range
    pub async fn last(
        pool: &SqlitePool,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
    ) -> Result<Option<Interest>> {
        let query = sqlx::query(ReconQuery::last_key(ReconType::Interest));
        let row = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_optional(pool.reader())
            .await
            .map_err(Error::from)?;
        row.map(|row| {
            let bytes: Vec<u8> = row.get(0);
            Interest::try_from(bytes).map_err(|e| Error::new_app(anyhow!(e)))
        })
        .transpose()
    }

    /// Return the first and last key in the range
    pub async fn first_and_last(
        pool: &SqlitePool,
        left_fencepost: &Interest,
        right_fencepost: &Interest,
    ) -> Result<Option<(Interest, Interest)>> {
        let query = sqlx::query(ReconQuery::first_and_last(
            ReconType::Interest,
            SqlBackend::Sqlite,
        ));
        let rows = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_all(pool.reader())
            .await
            .map_err(|e| Error::new_app(anyhow!(e)))?;
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
}
