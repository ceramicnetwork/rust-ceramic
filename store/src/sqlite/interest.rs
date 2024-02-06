#![warn(missing_docs, missing_debug_implementations, clippy::all)]

use anyhow::Result;
use async_trait::async_trait;
use ceramic_api::AccessInterestStore;
use ceramic_core::{Interest, RangeOpen};
use recon::{AssociativeHash, HashCount, InsertResult, Key, ReconItem, Store};
use serde::{Deserialize, Serialize};
use sqlx::Row;
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
    pool: SqlitePool,
}

impl<H> InterestStore<H>
where
    H: AssociativeHash,
{
    /// Make a new InterestSqliteStore from a connection pool.
    /// This will create the interest_key table if it does not already exist.
    pub async fn new(pool: SqlitePool) -> Result<Self> {
        let store = InterestStore {
            pool,
            hash: PhantomData,
        };
        store.create_table_if_not_exists().await?;
        Ok(store)
    }
}

impl<H> InterestStore<H>
where
    H: AssociativeHash + std::convert::From<[u32; 8]>,
{
    /// Initialize the interest_key table.
    async fn create_table_if_not_exists(&self) -> Result<()> {
        const CREATE_INTEREST_KEY_TABLE: &str = "CREATE TABLE IF NOT EXISTS interest_key (
            key BLOB, -- network_id sort_value controller StreamID height event_cid
            ahash_0 INTEGER, -- the ahash is decomposed as [u32; 8]
            ahash_1 INTEGER,
            ahash_2 INTEGER,
            ahash_3 INTEGER,
            ahash_4 INTEGER,
            ahash_5 INTEGER,
            ahash_6 INTEGER,
            ahash_7 INTEGER,
            PRIMARY KEY(key)
        )";

        let mut tx = self.pool.tx().await?;
        sqlx::query(CREATE_INTEREST_KEY_TABLE)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

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
        let key_insert = sqlx::query(
            "INSERT INTO interest_key (
                    key,
                    ahash_0, ahash_1, ahash_2, ahash_3,
                    ahash_4, ahash_5, ahash_6, ahash_7
                ) VALUES (
                    ?, 
                    ?, ?, ?, ?,
                    ?, ?, ?, ?
                );",
        );

        let hash = H::digest(key);
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
        let query = sqlx::query(
            "
        SELECT
            key
        FROM
            interest_key
        WHERE
            key > ? AND key < ?
        ORDER BY
            key ASC
        LIMIT
            ?
        OFFSET
            ?;
        ",
        );
        let rows = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(self.pool.reader())
            .await?;
        //debug!(count = rows.len(), "rows");
        Ok(Box::new(rows.into_iter().map(|row| {
            let bytes: Vec<u8> = row.get(0);
            Interest::from(bytes)
        })))
    }
}

#[async_trait::async_trait]
impl<H> AccessInterestStore for InterestStore<H>
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
impl<H> Store for InterestStore<H>
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

        let query = sqlx::query(
            "SELECT
               TOTAL(ahash_0) & 0xFFFFFFFF, TOTAL(ahash_1) & 0xFFFFFFFF,
               TOTAL(ahash_2) & 0xFFFFFFFF, TOTAL(ahash_3) & 0xFFFFFFFF,
               TOTAL(ahash_4) & 0xFFFFFFFF, TOTAL(ahash_5) & 0xFFFFFFFF,
               TOTAL(ahash_6) & 0xFFFFFFFF, TOTAL(ahash_7) & 0xFFFFFFFF,
               COUNT(1)
             FROM interest_key WHERE key > ? AND key < ?;",
        );
        let row = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_one(self.pool.reader())
            .await?;
        let bytes: [u32; 8] = [
            row.get(0),
            row.get(1),
            row.get(2),
            row.get(3),
            row.get(4),
            row.get(5),
            row.get(6),
            row.get(7),
        ];
        let count: i64 = row.get(8); // sql int type is signed
        let count: u64 = count
            .try_into()
            .expect("COUNT(1) should never return a negative number");
        Ok(HashCount::new(H::from(bytes), count))
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
            count(key)
        FROM
            interest_key
        WHERE
            key > ? AND key < ?
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
        key
    FROM
        interest_key
    WHERE
        key > ? AND key < ?
    ORDER BY
        key ASC
    LIMIT
        1
    ; ",
        );
        let rows = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        Ok(rows.first().map(|row| {
            let bytes: Vec<u8> = row.get(0);
            Interest::from(bytes)
        }))
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
            key
        FROM
            interest_key
        WHERE
            key > ? AND key < ?
        ORDER BY
            key DESC
        LIMIT
            1
        ;",
        );
        let rows = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        Ok(rows.first().map(|row| {
            let bytes: Vec<u8> = row.get(0);
            Interest::from(bytes)
        }))
    }

    #[instrument(skip(self))]
    async fn first_and_last(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<(Self::Key, Self::Key)>> {
        let query = sqlx::query(
            "
        SELECT first.key, last.key
        FROM
            (
                SELECT key
                FROM interest_key
                WHERE
                            key > ? AND key < ?
                ORDER BY key ASC
                LIMIT 1
            ) as first
        JOIN
            (
                SELECT key
                FROM interest_key
                WHERE
                            key > ? AND key < ?
                ORDER BY key DESC
                LIMIT 1
            ) as last
        ;",
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
            let first = Interest::from(f_bytes);
            let last = Interest::from(l_bytes);
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
    use super::*;

    use recon::{AssociativeHash, Key, ReconItem, Sha256a, Store};

    use expect_test::expect;
    use test_log::test;

    async fn new_store() -> InterestStore<Sha256a> {
        let conn = SqlitePool::connect("sqlite::memory:").await.unwrap();
        InterestStore::<Sha256a>::new(conn).await.unwrap()
    }

    #[test(tokio::test)]
    // This is the same as the recon::Store range test, but with the interest store (hits all its methods)
    async fn access_interest_model() {
        let store = new_store().await;
        let hello_interest = Interest::from("hello".as_bytes());
        let world_interest = Interest::from("world".as_bytes());
        AccessInterestStore::insert(&store, hello_interest.clone())
            .await
            .unwrap();
        AccessInterestStore::insert(&store, world_interest.clone())
            .await
            .unwrap();
        let interests = AccessInterestStore::range(
            &store,
            b"a".as_slice().into(),
            b"z".as_slice().into(),
            0,
            usize::MAX,
        )
        .await
        .unwrap();
        assert_eq!(2, interests.len());
        assert_eq!(vec![hello_interest, world_interest], interests);
    }

    #[test(tokio::test)]
    async fn test_hash_range_query() {
        let mut store = new_store().await;
        recon::Store::insert(
            &mut store,
            ReconItem::new_key(&Interest::from("hello".as_bytes())),
        )
        .await
        .unwrap();
        recon::Store::insert(
            &mut store,
            ReconItem::new_key(&Interest::from("world".as_bytes())),
        )
        .await
        .unwrap();
        let hash_cnt = store
            .hash_range(&b"a".as_slice().into(), &b"z".as_slice().into())
            .await
            .unwrap();
        expect![[r#"7460F21C83815F5EDC682F7A4154BC09AA3A0AE5DD1A2DEDCD709888A12751CC"#]]
            .assert_eq(&hash_cnt.hash().to_hex());
    }

    #[test(tokio::test)]
    async fn test_range_query() {
        let mut store = new_store().await;
        let hello_interest = Interest::from("hello".as_bytes());
        let world_interest = Interest::from("world".as_bytes());
        recon::Store::insert(&mut store, ReconItem::new_key(&hello_interest))
            .await
            .unwrap();
        recon::Store::insert(&mut store, ReconItem::new_key(&world_interest))
            .await
            .unwrap();
        let ids = recon::Store::range(
            &mut store,
            &b"a".as_slice().into(),
            &b"z".as_slice().into(),
            0,
            usize::MAX,
        )
        .await
        .unwrap();
        let interests = ids.collect::<Vec<Interest>>();
        assert_eq!(2, interests.len());
        assert_eq!(vec![hello_interest, world_interest], interests);
        // TODO: need to fix bug in interests format impl and regenerate/fix these expects
        // expect![[r#"
        // [
        //     Bytes(
        //         "hello",
        //     ),
        //     Bytes(
        //         "world",
        //     ),
        // ]
        // "#]]
        // .assert_debug_eq(&ids.collect::<Vec<Interest>>());
    }

    #[test(tokio::test)]
    async fn test_range_with_values_query() {
        let mut store = new_store().await;
        let hello_interest = Interest::from("hello".as_bytes());
        let world_interest = Interest::from("world".as_bytes());
        recon::Store::insert(&mut store, ReconItem::new_key(&hello_interest))
            .await
            .unwrap();
        recon::Store::insert(&mut store, ReconItem::new_key(&world_interest))
            .await
            .unwrap();
        let ids = store
            .range_with_values(
                &b"a".as_slice().into(),
                &b"z".as_slice().into(),
                0,
                usize::MAX,
            )
            .await
            .unwrap();
        let interests = ids.into_iter().map(|(i, _v)| i).collect::<Vec<Interest>>();
        assert_eq!(2, interests.len());
        assert_eq!(vec![hello_interest, world_interest], interests);
        // TODO: need to fix bug in interests format impl and regenerate/fix these expects
        // expect![[r#"
        // [
        //     Bytes(
        //         "hello",
        //     ),
        //     Bytes(
        //         "world",
        //     ),
        // ]
        // "#]]
        // .assert_debug_eq(&ids.collect::<Vec<Interest>>());
    }

    #[test(tokio::test)]
    async fn test_double_insert() {
        let mut store = new_store().await;

        // do take the first one
        expect![
            r#"
        Ok(
            true,
        )
        "#
        ]
        .assert_debug_eq(
            &recon::Store::insert(
                &mut store,
                ReconItem::new_key(&Interest::from("hello".as_bytes())),
            )
            .await,
        );

        // reject the second insert of same key
        expect![
            r#"
        Ok(
            false,
        )
        "#
        ]
        .assert_debug_eq(
            &recon::Store::insert(
                &mut store,
                ReconItem::new_key(&Interest::from("hello".as_bytes())),
            )
            .await,
        );
    }

    #[test(tokio::test)]
    async fn test_first_and_last() {
        let mut store = new_store().await;
        let hello_interest = Interest::from("hello".as_bytes());
        let world_interest = Interest::from("world".as_bytes());
        recon::Store::insert(&mut store, ReconItem::new_key(&hello_interest))
            .await
            .unwrap();
        recon::Store::insert(&mut store, ReconItem::new_key(&world_interest))
            .await
            .unwrap();

        // Only one key in range
        let ret = store
            .first_and_last(
                &Interest::from("a".as_bytes()),
                &Interest::from("j".as_bytes()),
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(hello_interest, ret.0);
        assert_eq!(hello_interest, ret.1);
        // expect![[r#"
        //     Some(
        //         (
        //             Bytes(
        //                 "hello",
        //             ),
        //             Bytes(
        //                 "hello",
        //             ),
        //         ),
        //     )
        // "#]]
        // .assert_debug_eq(&ret);

        // No keys in range
        let ret = store
            .first_and_last(
                &Interest::from("j".as_bytes()),
                &Interest::from("p".as_bytes()),
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
                &Interest::from("a".as_bytes()),
                &Interest::from("z".as_bytes()),
            )
            .await
            .unwrap()
            .unwrap();
        // both keys exist
        assert_eq!(hello_interest, ret.0);
        assert_eq!(world_interest, ret.1);
        // expect![[r#"
        //     Some(
        //         (
        //             Bytes(
        //                 "hello",
        //             ),
        //             Bytes(
        //                 "world",
        //             ),
        //         ),
        //     )
        // "#]]
        // .assert_debug_eq(&ret);
    }

    #[test(tokio::test)]
    #[should_panic(expected = "Interests do not support values! Invalid request.")]
    async fn test_store_value_for_key_error() {
        let mut store = new_store().await;
        let key = Interest::from("hello".as_bytes());
        let store_value = Interest::from("world".as_bytes());
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
        let key = Interest::from("hello".as_bytes());
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
        let key = Interest::from("hello".as_bytes());
        recon::Store::insert(&mut store, ReconItem::new(&key, None))
            .await
            .unwrap();
        let value = store.value_for_key(&key).await.unwrap();
        let val = value.unwrap();
        let empty: Vec<u8> = vec![];
        assert_eq!(empty, val);
    }
}
