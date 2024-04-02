#![warn(missing_docs, missing_debug_implementations, clippy::all)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use ceramic_core::{Interest, RangeOpen};
use recon::{AssociativeHash, HashCount, InsertResult, Key, ReconItem};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgRow, Executor, Row};
use std::marker::PhantomData;
use tracing::instrument;

use crate::{DbTx, SqlitePool};

#[derive(Debug, Clone)]
/// InterestStore is a [`recon::Store`] implementation for Interests.
pub struct InterestStore<H>
where
    H: AssociativeHash,
{
    hash: PhantomData<H>,
    // pool: SqlitePool,
    pool: super::postgres::PostgresPool,
}

impl<H> InterestStore<H>
where
    H: AssociativeHash,
{
    /// Make a new InterestSqliteStore from a connection pool.
    pub async fn new(pool: super::postgres::PostgresPool) -> Result<Self> {
        let store = InterestStore {
            pool,
            hash: PhantomData,
        };
        Ok(store)
    }
}

#[derive(Debug, Clone)]
pub struct InterestRow {
    count: i64,
    ahash_0: u32,
    ahash_1: u32,
    ahash_2: u32,
    ahash_3: u32,
    ahash_4: u32,
    ahash_5: u32,
    ahash_6: u32,
    ahash_7: u32,
}

impl InterestRow {
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

fn into_u32s(val: Option<i64>, col: &str) -> Result<u32, sqlx::Error> {
    let ahash_0 = val
        .map(u32::try_from)
        .transpose()
        .map_err(|e| sqlx::Error::Decode(Box::new(e)))?
        .ok_or_else(|| sqlx::Error::ColumnNotFound(format!("{} was not found", col)))?;
    Ok(ahash_0)
}

impl sqlx::FromRow<'_, PgRow> for InterestRow {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        let ahash_0 = into_u32s(row.try_get("ahash_0")?, "ahash_0")?;
        let ahash_1 = into_u32s(row.try_get("ahash_1")?, "ahash_1")?;
        let ahash_2 = into_u32s(row.try_get("ahash_2")?, "ahash_2")?;
        let ahash_3 = into_u32s(row.try_get("ahash_3")?, "ahash_3")?;
        let ahash_4 = into_u32s(row.try_get("ahash_4")?, "ahash_4")?;
        let ahash_5 = into_u32s(row.try_get("ahash_5")?, "ahash_5")?;
        let ahash_6 = into_u32s(row.try_get("ahash_6")?, "ahash_6")?;
        let ahash_7 = into_u32s(row.try_get("ahash_7")?, "ahash_7")?;
        Ok(Self {
            count: row.try_get("count")?,
            ahash_0,
            ahash_1,
            ahash_2,
            ahash_3,
            ahash_4,
            ahash_5,
            ahash_6,
            ahash_7,
        })
    }
}

type InterestError = <Interest as TryFrom<Vec<u8>>>::Error;

impl<H> InterestStore<H>
where
    H: AssociativeHash + std::convert::From<[u32; 8]>,
{
    async fn insert_item(&self, key: &Interest) -> Result<bool> {
        let mut tx = self.pool.writer().begin().await?;
        let new_key = self.insert_item_int(key, &mut tx).await?;
        tx.commit().await?;
        Ok(new_key)
    }

    /// returns (new_key, new_val) tuple
    async fn insert_item_int(&self, item: &Interest, conn: &mut DbTx<'_>) -> Result<bool> {
        let new_key = self.insert_key_int(item, conn).await?;
        Ok(new_key)
    }

    async fn insert_key_int(&self, key: &Interest, conn: &mut DbTx<'_>) -> Result<bool> {
        conn.execute("SAVEPOINT insert_interest;")
            .await
            .context("interest savepoint error")?;
        let key_insert = sqlx::query(
            "INSERT INTO interest (
                    order_key,
                    ahash_0, ahash_1, ahash_2, ahash_3,
                    ahash_4, ahash_5, ahash_6, ahash_7
                ) VALUES (
                    $1, 
                    $2, $3, $4, $5,
                    $6, $7, $8, $9
                );",
        );

        let hash = H::digest(key);
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
                        .context("rollback error")?;
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
        let offset: i64 = offset.try_into().context("offset too large")?;
        let limit = limit.try_into().unwrap_or(i64::MAX);
        let query = sqlx::query(
            "SELECT
            order_key
        FROM
            interest
        WHERE
            order_key > $1 AND order_key < $2
        ORDER BY
            order_key ASC
        LIMIT
            $3
        OFFSET 
            $4",
        );
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
                Interest::try_from(bytes)
            })
            .collect::<Result<Vec<Interest>, InterestError>>()?;
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
impl<H> ceramic_api::AccessInterestStore for InterestStore<H>
where
    H: AssociativeHash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>,
{
    type Key = Interest;
    type Hash = H;

    async fn insert(&self, key: Self::Key) -> Result<bool> {
        self.insert_item(&key).await
    }
    async fn range(
        &self,
        start: Self::Key,
        end: Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Self::Key>> {
        Ok(self.range_int(&start, &end, offset, limit).await?.collect())
    }
}

#[async_trait]
impl<H> recon::Store for InterestStore<H>
where
    H: AssociativeHash,
{
    type Key = Interest;
    type Hash = H;

    /// Returns true if the key was new. The value is always updated if included
    async fn insert(&mut self, item: ReconItem<'_, Self::Key>) -> Result<bool> {
        // interests don't have values, if someone gives us something we throw an error but allow None/vec![]
        if let Some(val) = item.value {
            if !val.is_empty() {
                return Err(anyhow::anyhow!(
                    "Interests do not support values! Invalid request."
                ));
            }
        }
        self.insert_item(item.key).await
    }

    /// Insert new keys into the key space.
    /// Returns true if a key did not previously exist.
    async fn insert_many<'a, I>(&mut self, items: I) -> Result<InsertResult>
    where
        I: ExactSizeIterator<Item = ReconItem<'a, Interest>> + Send + Sync,
    {
        match items.len() {
            0 => Ok(InsertResult::new(vec![], 0)),
            _ => {
                let mut results = vec![false; items.len()];
                let mut new_val_cnt = 0;
                let mut tx = self.pool.writer().begin().await?;

                for (idx, item) in items.enumerate() {
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
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<HashCount<Self::Hash>> {
        if left_fencepost >= right_fencepost {
            return Ok(HashCount::new(H::identity(), 0));
        }

        let res: InterestRow = sqlx::query_as(
            "SELECT
                COALESCE(SUM(ahash_0), 0)::bigint & 4294967295 as ahash_0, COALESCE(SUM(ahash_1), 0)::bigint & 4294967295 as ahash_1,
                COALESCE(SUM(ahash_2), 0)::bigint & 4294967295 as ahash_2, COALESCE(SUM(ahash_3), 0)::bigint & 4294967295 as ahash_3,
                COALESCE(SUM(ahash_4), 0)::bigint & 4294967295 as ahash_4, COALESCE(SUM(ahash_5), 0)::bigint & 4294967295 as ahash_5,
                COALESCE(SUM(ahash_6), 0)::bigint & 4294967295 as ahash_6, COALESCE(SUM(ahash_7), 0)::bigint & 4294967295 as ahash_7,
                COUNT(1) as count
             FROM interest WHERE order_key > $1 AND order_key < $2;",
        )
        .bind(left_fencepost.as_bytes())
        .bind(right_fencepost.as_bytes())
        .fetch_one(self.pool.reader())
        .await.context("interest range")?;
        let bytes = res.hash();
        Ok(HashCount::new(H::from(bytes), res.count()))
    }

    #[instrument(skip(self))]
    async fn range(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        self.range_int(left_fencepost, right_fencepost, offset, limit)
            .await
    }

    #[instrument(skip(self))]
    /// Interests don't have values, so the value will always be an empty vec. Use `range` instead.
    async fn range_with_values(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        let rows = self
            .range(left_fencepost, right_fencepost, offset, limit)
            .await?;
        Ok(Box::new(rows.into_iter().map(|key| (key, Vec::new()))))
    }
    /// Return the number of keys within the range.
    #[instrument(skip(self))]
    async fn count(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<usize> {
        let query = sqlx::query(
            "
        SELECT
            count(order_key)
        FROM
            interest
        WHERE
            order_key > $1 AND order_key < $2
        ;",
        );
        let row = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_one(self.pool.reader())
            .await?;
        Ok(row.get::<'_, i64, _>(0) as usize)
    }

    /// Return the first key within the range.
    #[instrument(skip(self))]
    async fn first(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        let query = sqlx::query(
            "
    SELECT
        order_key
    FROM
        interest
    WHERE
        order_key > $1 AND order_key < $2
    ORDER BY
        order_key ASC
    LIMIT
        1
    ; ",
        );
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
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        let query = sqlx::query(
            "
        SELECT
            order_key
        FROM
            interest
        WHERE
            order_key > $1 AND order_key < $2
        ORDER BY
            order_key DESC
        LIMIT
            1
        ;",
        );
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
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<(Self::Key, Self::Key)>> {
        let query = sqlx::query(
            ";with first as (
                SELECT order_key
                FROM interest
                WHERE
                    order_key > $1 AND order_key < $2
                ORDER BY order_key ASC
                LIMIT 1
            ), last as (
                SELECT order_key
                FROM interest
                WHERE
                    order_key > $3 AND order_key < $4
                ORDER BY order_key DESC
                LIMIT 1
            ) select first.order_key, last.order_key from first, last;",
        );
        let rows = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
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
    async fn value_for_key(&mut self, _key: &Self::Key) -> Result<Option<Vec<u8>>> {
        Ok(Some(vec![]))
    }

    #[instrument(skip(self))]
    async fn keys_with_missing_values(
        &mut self,
        _range: RangeOpen<Self::Key>,
    ) -> Result<Vec<Self::Key>> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod interest_tests {
    use std::{collections::BTreeSet, str::FromStr, sync::OnceLock};

    use crate::PostgresPool;

    use super::*;

    use ceramic_api::AccessInterestStore;
    use ceramic_core::{
        interest::{Builder, WithPeerId},
        PeerId,
    };
    use rand::{thread_rng, Rng};
    use recon::{AssociativeHash, FullInterests, InterestProvider, Key, ReconItem, Sha256a, Store};

    use expect_test::expect;
    use test_log::test;
    use tokio::sync::{Mutex, MutexGuard};

    const SORT_KEY: &str = "model";
    const PEER_ID: &str = "1AdgHpWeBKTU3F2tUkAQqL2Y2Geh4QgHJwcWMPuiY1qiRQ";

    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    async fn prep_test(store: &InterestStore<Sha256a>) -> MutexGuard<'static, ()> {
        let lock = LOCK.get_or_init(|| Mutex::new(())).lock().await;
        store
            .pool
            .run_statement("truncate table interest;")
            .await
            .unwrap();
        lock
    }

    // Return an builder for an event with the same network,model,controller,stream.
    pub(crate) fn interest_builder() -> Builder<WithPeerId> {
        Interest::builder()
            .with_sort_key(SORT_KEY)
            .with_peer_id(&PeerId::from_str(PEER_ID).unwrap())
    }

    // Generate an event for the same network,model,controller,stream
    // The event and height are random when when its None.
    pub(crate) fn random_interest<'a>(
        range: Option<(&'a [u8], &'a [u8])>,
        not_after: Option<u64>,
    ) -> Interest {
        let rand_range = (&[0u8][..], &[thread_rng().gen::<u8>()][..]);
        interest_builder()
            .with_range(range.unwrap_or(rand_range))
            .with_not_after(not_after.unwrap_or_else(|| thread_rng().gen()))
            .build()
    }
    // The EventId that is the minumum of all possible random event ids
    pub(crate) fn random_interest_min() -> Interest {
        interest_builder().with_min_range().build_fencepost()
    }
    // The EventId that is the maximum of all possible random event ids
    pub(crate) fn random_interest_max() -> Interest {
        interest_builder().with_max_range().build_fencepost()
    }

    async fn new_store() -> InterestStore<Sha256a> {
        let conn = PostgresPool::connect_in_memory().await.unwrap();
        InterestStore::<Sha256a>::new(conn).await.unwrap()
    }

    #[test(tokio::test)]
    // This is the same as the recon::Store range test, but with the interest store (hits all its methods)
    async fn access_interest_model() {
        let store = new_store().await;
        let _lock = prep_test(&store).await;

        let interest_0 = random_interest(None, None);
        let interest_1 = random_interest(None, None);
        AccessInterestStore::insert(&store, interest_0.clone())
            .await
            .unwrap();
        AccessInterestStore::insert(&store, interest_1.clone())
            .await
            .unwrap();
        let interests = AccessInterestStore::range(
            &store,
            random_interest_min(),
            random_interest_max(),
            0,
            usize::MAX,
        )
        .await
        .unwrap();
        assert_eq!(
            BTreeSet::from_iter([interest_0, interest_1]),
            BTreeSet::from_iter(interests)
        );
    }

    #[test(tokio::test)]
    async fn test_hash_range_query() {
        let mut store = new_store().await;
        let _lock = prep_test(&store).await;
        recon::Store::insert(
            &mut store,
            ReconItem::new_key(&random_interest(Some((&[0], &[1])), Some(42))),
        )
        .await
        .unwrap();

        recon::Store::insert(
            &mut store,
            ReconItem::new_key(&random_interest(Some((&[0], &[1])), Some(24))),
        )
        .await
        .unwrap();
        let hash_cnt = store
            .hash_range(&random_interest_min(), &random_interest_max())
            .await
            .unwrap();
        expect!["D6C3CBCCE02E4AF2900ACF7FC84BE91168A42A0B1164534C426C782057E13BBC"]
            .assert_eq(&hash_cnt.hash().to_hex());
    }

    #[test(tokio::test)]
    async fn test_hash_range_query_defaults() {
        let mut store = new_store().await;
        let _lock = prep_test(&store).await;
        store.pool.run_statement(r#"INSERT INTO public.interest
        (order_key, ahash_0, ahash_1, ahash_2, ahash_3, ahash_4, ahash_5, ahash_6, ahash_7)
        VALUES(decode('480F70D652B6B825E45826002408011220FCD119D77CA668F157CED3FB79498A82B41CA4D7DC05F90B6B227196F3EC856401581DCE010580808080107E710E217FA0E25900000000000000000000000000581DCE010580808080107E710E217FA0E259FFFFFFFFFFFFFFFFFFFFFFFFFF00','hex'), 3708858614, 3187057933, 90630269, 2944836858, 2664423810, 3186949905, 2792292527, 515406059);"#).await.unwrap();

        store.pool.run_statement("INSERT INTO public.interest
        (order_key, ahash_0, ahash_1, ahash_2, ahash_3, ahash_4, ahash_5, ahash_6, ahash_7)
        VALUES(decode('480F70D652B6B825E45826002408011220FCD119D77CA668F157CED3FB79498A82B41CA4D7DC05F90B6B227196F3EC856401581DCE01058080808010A252AB059F8F49FD00000000000000000000000000581DCE01058080808010A252AB059F8F49FDFFFFFFFFFFFFFFFFFFFFFFFFFF00','hex'), 2841278, 2946150166, 1420163820, 754142617, 2283458068, 1856053704, 3039129056, 3387910774);").await.unwrap();
        let interests = FullInterests::<Interest>::default()
            .interests()
            .await
            .unwrap();

        for int in interests {
            let hash_cnt = store.hash_range(&int.start, &int.end).await.unwrap();
            assert_eq!(0, hash_cnt.count())
        }
    }

    #[test(tokio::test)]
    async fn test_range_query() {
        let mut store = new_store().await;
        let interest_0 = random_interest(None, None);
        let interest_1 = random_interest(None, None);
        let _lock = prep_test(&store).await;

        recon::Store::insert(&mut store, ReconItem::new_key(&interest_0))
            .await
            .unwrap();
        recon::Store::insert(&mut store, ReconItem::new_key(&interest_1))
            .await
            .unwrap();
        let ids = recon::Store::range(
            &mut store,
            &random_interest_min(),
            &random_interest_max(),
            0,
            usize::MAX,
        )
        .await
        .unwrap();
        let interests = ids.collect::<BTreeSet<Interest>>();
        assert_eq!(BTreeSet::from_iter([interest_0, interest_1]), interests);
    }

    #[test(tokio::test)]
    async fn test_range_with_values_query() {
        let mut store = new_store().await;
        let _lock = prep_test(&store).await;
        let interest_0 = random_interest(None, None);
        let interest_1 = random_interest(None, None);

        store.insert(interest_0.clone()).await.unwrap();
        store.insert(interest_1.clone()).await.unwrap();
        let ids = store
            .range_with_values(
                &random_interest_min(),
                &random_interest_max(),
                0,
                usize::MAX,
            )
            .await
            .unwrap();
        let interests = ids
            .into_iter()
            .map(|(i, _v)| i)
            .collect::<BTreeSet<Interest>>();
        assert_eq!(BTreeSet::from_iter([interest_0, interest_1]), interests);
    }

    #[test(tokio::test)]
    async fn test_double_insert() {
        let mut store = new_store().await;
        let _lock = prep_test(&store).await;

        let interest = random_interest(None, None);
        // do take the first one
        expect![
            r#"
        Ok(
            true,
        )
        "#
        ]
        .assert_debug_eq(&recon::Store::insert(&mut store, ReconItem::new_key(&interest)).await);

        // reject the second insert of same key
        expect![
            r#"
        Ok(
            false,
        )
        "#
        ]
        .assert_debug_eq(&recon::Store::insert(&mut store, ReconItem::new_key(&interest)).await);
    }

    #[test(tokio::test)]
    async fn test_first_and_last() {
        let mut store = new_store().await;
        let _lock = prep_test(&store).await;

        let interest_0 = random_interest(Some((&[], &[])), Some(42));
        let interest_1 = random_interest(Some((&[], &[])), Some(43));
        recon::Store::insert(&mut store, ReconItem::new_key(&interest_0))
            .await
            .unwrap();
        recon::Store::insert(&mut store, ReconItem::new_key(&interest_1))
            .await
            .unwrap();

        // Only one key in range, we expect to get the same key as first and last
        let ret = store
            .first_and_last(
                &random_interest(Some((&[], &[])), Some(40)),
                &random_interest(Some((&[], &[])), Some(43)),
            )
            .await
            .unwrap()
            .unwrap();

        expect![[r#"
            (
                Interest {
                    bytes: "480f70d652b6b825e4582200206ce9f954100188eb6e7939dbf45ac845d3399dba29da4e0b6ef1fdd8636a326b014040182a",
                    sort_key_hash: "0f70d652b6b825e4",
                    peer_id: PeerId(
                        "1AdgHpWeBKTU3F2tUkAQqL2Y2Geh4QgHJwcWMPuiY1qiRQ",
                    ),
                    range: RangeOpen {
                        start: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                        end: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                    },
                    not_after: 42,
                },
                Interest {
                    bytes: "480f70d652b6b825e4582200206ce9f954100188eb6e7939dbf45ac845d3399dba29da4e0b6ef1fdd8636a326b014040182a",
                    sort_key_hash: "0f70d652b6b825e4",
                    peer_id: PeerId(
                        "1AdgHpWeBKTU3F2tUkAQqL2Y2Geh4QgHJwcWMPuiY1qiRQ",
                    ),
                    range: RangeOpen {
                        start: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                        end: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                    },
                    not_after: 42,
                },
            )
        "#]]
        .assert_debug_eq(&ret);

        // No keys in range
        let ret = store
            .first_and_last(
                &random_interest(Some((&[], &[])), Some(50)),
                &random_interest(Some((&[], &[])), Some(53)),
            )
            .await
            .unwrap();
        expect![[r#"
            None
        "#]]
        .assert_debug_eq(&ret);

        // Two keys in range
        let ret = store
            .first_and_last(
                &random_interest(Some((&[], &[])), Some(40)),
                &random_interest(Some((&[], &[])), Some(50)),
            )
            .await
            .unwrap()
            .unwrap();
        // both keys exist
        expect![[r#"
            (
                Interest {
                    bytes: "480f70d652b6b825e4582200206ce9f954100188eb6e7939dbf45ac845d3399dba29da4e0b6ef1fdd8636a326b014040182a",
                    sort_key_hash: "0f70d652b6b825e4",
                    peer_id: PeerId(
                        "1AdgHpWeBKTU3F2tUkAQqL2Y2Geh4QgHJwcWMPuiY1qiRQ",
                    ),
                    range: RangeOpen {
                        start: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                        end: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                    },
                    not_after: 42,
                },
                Interest {
                    bytes: "480f70d652b6b825e4582200206ce9f954100188eb6e7939dbf45ac845d3399dba29da4e0b6ef1fdd8636a326b014040182b",
                    sort_key_hash: "0f70d652b6b825e4",
                    peer_id: PeerId(
                        "1AdgHpWeBKTU3F2tUkAQqL2Y2Geh4QgHJwcWMPuiY1qiRQ",
                    ),
                    range: RangeOpen {
                        start: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                        end: Ok(
                            EventId {
                                bytes: "MIN",
                            },
                        ),
                    },
                    not_after: 43,
                },
            )
        "#]]
        .assert_debug_eq(&ret);
    }

    #[test(tokio::test)]
    #[should_panic(expected = "Interests do not support values! Invalid request.")]
    async fn test_store_value_for_key_error() {
        let mut store = new_store().await;
        let _lock = prep_test(&store).await;
        let key = random_interest(None, None);
        let store_value = random_interest(None, None);
        recon::Store::insert(
            &mut store,
            ReconItem::new_with_value(&key, store_value.as_slice()),
        )
        .await
        .unwrap();
    }

    #[test(tokio::test)]
    async fn test_keys_with_missing_value() {
        let mut store = new_store().await;
        let _lock = prep_test(&store).await;
        let key = random_interest(None, None);
        recon::Store::insert(&mut store, ReconItem::new(&key, None))
            .await
            .unwrap();
        let missing_keys = store
            .keys_with_missing_values((Interest::min_value(), Interest::max_value()).into())
            .await
            .unwrap();
        expect![[r#"
            []
        "#]]
        .assert_debug_eq(&missing_keys);

        recon::Store::insert(&mut store, ReconItem::new(&key, Some(&[])))
            .await
            .unwrap();
        let missing_keys = store
            .keys_with_missing_values((Interest::min_value(), Interest::max_value()).into())
            .await
            .unwrap();
        expect![[r#"
            []
        "#]]
        .assert_debug_eq(&missing_keys);
    }

    #[test(tokio::test)]
    async fn test_value_for_key() {
        let mut store = new_store().await;
        let _lock = prep_test(&store).await;
        let key = random_interest(None, None);
        recon::Store::insert(&mut store, ReconItem::new(&key, None))
            .await
            .unwrap();
        let value = store.value_for_key(&key).await.unwrap();
        let val = value.unwrap();
        let empty: Vec<u8> = vec![];
        assert_eq!(empty, val);
    }
}
