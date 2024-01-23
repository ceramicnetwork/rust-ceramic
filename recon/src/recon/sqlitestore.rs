#![warn(missing_docs, missing_debug_implementations, clippy::all)]

use super::{HashCount, InsertResult, ReconItem};
use crate::{AssociativeHash, Key, Store};
use anyhow::Result;
use async_trait::async_trait;
use ceramic_core::{DbTx, RangeOpen, SqlitePool};
use sqlx::Row;
use std::marker::PhantomData;
use std::result::Result::Ok;
use tracing::instrument;

/// ReconSQLite is a implementation of Recon store
#[derive(Debug)]
pub struct SQLiteStore<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    key: PhantomData<K>,
    hash: PhantomData<H>,
    pool: SqlitePool,
    sort_key: String,
}

impl<K, H> SQLiteStore<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    /// Make a new SQLiteStore from a connection and sort_key.
    /// This will create the recon table if it does not already exist.
    pub async fn new(pool: SqlitePool, sort_key: String) -> Result<Self> {
        let mut store = SQLiteStore {
            pool,
            sort_key,
            key: PhantomData,
            hash: PhantomData,
        };
        store.create_table_if_not_exists().await?;
        Ok(store)
    }
}

impl<K, H> SQLiteStore<K, H>
where
    K: Key,
    H: AssociativeHash + std::convert::From<[u32; 8]>,
{
    /// Initialize the recon table.
    async fn create_table_if_not_exists(&mut self) -> Result<()> {
        // Do we want to remove CID from the table?
        const CREATE_RECON_TABLE: &str = "CREATE TABLE IF NOT EXISTS recon (
            sort_key TEXT, -- the field in the event header to sort by e.g. model
            key BLOB, -- network_id sort_value controller StreamID height event_cid
            ahash_0 INTEGER, -- the ahash is decomposed as [u32; 8]
            ahash_1 INTEGER,
            ahash_2 INTEGER,
            ahash_3 INTEGER,
            ahash_4 INTEGER,
            ahash_5 INTEGER,
            ahash_6 INTEGER,
            ahash_7 INTEGER,
            CID TEXT,
            value_retrieved BOOL, -- indicates if we still want the value
            PRIMARY KEY(sort_key, key)
        )";
        const CREATE_VALUE_RETRIEVED_INDEX: &str =
            "CREATE INDEX IF NOT EXISTS idx_recon_value_retrieved
            ON recon (sort_key, key, value_retrieved)";

        const CREATE_RECON_VALUE_TABLE: &str = "CREATE TABLE IF NOT EXISTS recon_value (
            sort_key TEXT,
            key BLOB,
            value BLOB,
            PRIMARY KEY(sort_key, key)
        )";

        let mut tx = self.pool.tx().await?;
        sqlx::query(CREATE_RECON_TABLE).execute(&mut *tx).await?;
        sqlx::query(CREATE_VALUE_RETRIEVED_INDEX)
            .execute(&mut *tx)
            .await?;
        sqlx::query(CREATE_RECON_VALUE_TABLE)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// returns (new_key, new_val) tuple
    async fn insert_item_int(
        &mut self,
        item: &ReconItem<'_, K>,
        conn: &mut DbTx<'_>,
    ) -> Result<(bool, bool)> {
        // we insert the value first as it's possible we already have the key and can skip that step
        // as it happens in a transaction, we'll roll back the value insert if the key insert fails and try again
        if let Some(val) = item.value {
            // Update the value_retrieved flag, and report if the key already exists.
            let key_exists = self.update_value_retrieved_int(item.key, conn).await?;
            self.insert_value_int(item.key, val, conn).await?;
            if key_exists {
                return Ok((false, true));
            }
        }
        let new_key = self
            .insert_key_int(item.key, item.value.is_some(), conn)
            .await?;
        Ok((new_key, item.value.is_some()))
    }

    // set value_retrieved to true and return if the key already exists
    async fn update_value_retrieved_int(&mut self, key: &K, conn: &mut DbTx<'_>) -> Result<bool> {
        let update =
            sqlx::query("UPDATE recon SET value_retrieved = true WHERE sort_key = ? AND key = ?");
        let resp = update
            .bind(&self.sort_key)
            .bind(key.as_bytes())
            .execute(&mut **conn)
            .await?;
        let rows_affected = resp.rows_affected();
        debug_assert!(rows_affected <= 1);
        Ok(rows_affected == 1)
    }

    /// returns true if the key already exists in the recon table
    async fn insert_value_int(&mut self, key: &K, val: &[u8], conn: &mut DbTx<'_>) -> Result<()> {
        let value_insert = sqlx::query(
            r#"INSERT INTO recon_value (value, sort_key, key)
                VALUES (?, ?, ?)
            ON CONFLICT (sort_key, key) DO UPDATE
                SET value=excluded.value"#,
        );

        value_insert
            .bind(val)
            .bind(&self.sort_key)
            .bind(key.as_bytes())
            .execute(&mut **conn)
            .await?;

        Ok(())
    }

    async fn insert_key_int(
        &mut self,
        key: &K,
        has_value: bool,
        conn: &mut DbTx<'_>,
    ) -> Result<bool> {
        let key_insert = sqlx::query(
            "INSERT INTO recon (
                    sort_key, key,
                    ahash_0, ahash_1, ahash_2, ahash_3,
                    ahash_4, ahash_5, ahash_6, ahash_7,
                    value_retrieved
                ) VALUES (
                    ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?
                );",
        );

        let hash = H::digest(key);
        let resp = key_insert
            .bind(&self.sort_key)
            .bind(key.as_bytes())
            .bind(hash.as_u32s()[0])
            .bind(hash.as_u32s()[1])
            .bind(hash.as_u32s()[2])
            .bind(hash.as_u32s()[3])
            .bind(hash.as_u32s()[4])
            .bind(hash.as_u32s()[5])
            .bind(hash.as_u32s()[6])
            .bind(hash.as_u32s()[7])
            .bind(has_value)
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
}

#[async_trait]
impl<K, H> Store for SQLiteStore<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    type Key = K;
    type Hash = H;

    /// Returns true if the key was new. The value is always updated if included
    async fn insert(&mut self, item: ReconItem<'_, Self::Key>) -> Result<bool> {
        let mut tx = self.pool.writer().begin().await?;
        let (new_key, _new_val) = self.insert_item_int(&item, &mut tx).await?;
        tx.commit().await?;
        Ok(new_key)
    }

    /// Insert new keys into the key space.
    /// Returns true if a key did not previously exist.
    async fn insert_many<'a, I>(&mut self, items: I) -> Result<InsertResult>
    where
        I: ExactSizeIterator<Item = ReconItem<'a, K>> + Send + Sync,
    {
        match items.len() {
            0 => Ok(InsertResult::new(vec![], 0)),
            _ => {
                let mut results = vec![false; items.len()];
                let mut new_val_cnt = 0;
                let mut tx = self.pool.writer().begin().await?;

                for (idx, item) in items.enumerate() {
                    let (new_key, new_val) = self.insert_item_int(&item, &mut tx).await?;
                    results[idx] = new_key;
                    if new_val {
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
            return Ok(HashCount {
                hash: H::identity(),
                count: 0,
            });
        }

        let query = sqlx::query(
            "SELECT
               TOTAL(ahash_0) & 0xFFFFFFFF, TOTAL(ahash_1) & 0xFFFFFFFF,
               TOTAL(ahash_2) & 0xFFFFFFFF, TOTAL(ahash_3) & 0xFFFFFFFF,
               TOTAL(ahash_4) & 0xFFFFFFFF, TOTAL(ahash_5) & 0xFFFFFFFF,
               TOTAL(ahash_6) & 0xFFFFFFFF, TOTAL(ahash_7) & 0xFFFFFFFF,
               COUNT(1)
             FROM recon WHERE sort_key = ? AND key > ? AND key < ?;",
        );
        let row = query
            .bind(&self.sort_key)
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
        Ok(HashCount {
            hash: H::from(bytes),
            count,
        })
    }

    #[instrument(skip(self))]
    async fn range(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        let query = sqlx::query(
            "
        SELECT
            key
        FROM
            recon
        WHERE
            sort_key = ? AND
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
            .bind(&self.sort_key)
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(self.pool.reader())
            .await?;
        //debug!(count = rows.len(), "rows");
        Ok(Box::new(rows.into_iter().map(|row| {
            let bytes: Vec<u8> = row.get(0);
            K::from(bytes)
        })))
    }
    #[instrument(skip(self))]
    async fn range_with_values(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        let query = sqlx::query(
            "
        SELECT
            key, value
        FROM
            recon_value
        WHERE
            sort_key = ? AND
            key > ? AND key < ?
            AND value IS NOT NULL
        ORDER BY
            key ASC
        LIMIT
            ?
        OFFSET
            ?;
        ",
        );
        let rows = query
            .bind(&self.sort_key)
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(self.pool.reader())
            .await?;
        Ok(Box::new(rows.into_iter().map(|row| {
            let key: Vec<u8> = row.get(0);
            let value: Vec<u8> = row.get(1);
            (K::from(key), value)
        })))
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
            recon
        WHERE
            sort_key = ? AND
            key > ? AND key < ?
        ;",
        );
        let row = query
            .bind(&self.sort_key)
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
        recon
    WHERE
        sort_key = ? AND
        key > ? AND key < ?
    ORDER BY
        key ASC
    LIMIT
        1
    ; ",
        );
        let rows = query
            .bind(&self.sort_key)
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        Ok(rows.first().map(|row| {
            let bytes: Vec<u8> = row.get(0);
            K::from(bytes)
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
            recon
        WHERE
            sort_key = ? AND
            key > ? AND key < ?
        ORDER BY
            key DESC
        LIMIT
            1
        ;",
        );
        let rows = query
            .bind(&self.sort_key)
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        Ok(rows.first().map(|row| {
            let bytes: Vec<u8> = row.get(0);
            K::from(bytes)
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
                FROM recon
                WHERE
                            sort_key = ? AND
                            key > ? AND key < ?
                ORDER BY key ASC
                LIMIT 1
            ) as first
        JOIN
            (
                SELECT key
                FROM recon
                WHERE
                            sort_key = ? AND
                            key > ? AND key < ?
                ORDER BY key DESC
                LIMIT 1
            ) as last
        ;",
        );
        let rows = query
            .bind(&self.sort_key)
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .bind(&self.sort_key)
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        if let Some(row) = rows.first() {
            let first = K::from(row.get(0));
            let last = K::from(row.get(1));
            Ok(Some((first, last)))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self))]
    async fn value_for_key(&mut self, key: &Self::Key) -> Result<Option<Vec<u8>>> {
        let query = sqlx::query("SELECT value FROM recon_value WHERE sort_key=? AND key=?;");
        let row = query
            .bind(&self.sort_key)
            .bind(key.as_bytes())
            .fetch_optional(self.pool.reader())
            .await?;
        Ok(row.map(|row| row.get(0)))
    }

    #[instrument(skip(self))]
    async fn keys_with_missing_values(
        &mut self,
        range: RangeOpen<Self::Key>,
    ) -> Result<Vec<Self::Key>> {
        if range.start >= range.end {
            return Ok(vec![]);
        };
        let query = sqlx::query(
            "
            SELECT key
            FROM recon
            WHERE
                sort_key=?
                AND key > ?
                AND key < ?
                AND value_retrieved = false
            ;",
        );
        let row = query
            .bind(&self.sort_key)
            .bind(range.start.as_bytes())
            .bind(range.end.as_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        Ok(row.into_iter().map(|row| K::from(row.get(0))).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::recon::ReconItem;
    use crate::tests::AlphaNumBytes;
    use crate::Sha256a;

    use expect_test::expect;
    use test_log::test;

    async fn new_store() -> SQLiteStore<AlphaNumBytes, Sha256a> {
        let conn = SqlitePool::connect("sqlite::memory:").await.unwrap();
        SQLiteStore::<AlphaNumBytes, Sha256a>::new(conn, "test".to_string())
            .await
            .unwrap()
    }

    #[test(tokio::test)]
    async fn test_hash_range_query() {
        let mut store = new_store().await;
        store
            .insert(ReconItem::new_key(&AlphaNumBytes::from("hello")))
            .await
            .unwrap();
        store
            .insert(ReconItem::new_key(&AlphaNumBytes::from("world")))
            .await
            .unwrap();
        let hash: Sha256a = store
            .hash_range(&b"a".as_slice().into(), &b"z".as_slice().into())
            .await
            .unwrap()
            .hash;
        expect![[r#"7460F21C83815F5EDC682F7A4154BC09AA3A0AE5DD1A2DEDCD709888A12751CC"#]]
            .assert_eq(&hash.to_hex());
    }

    #[test(tokio::test)]
    async fn test_range_query() {
        let mut store = new_store().await;
        store
            .insert(ReconItem::new_key(&AlphaNumBytes::from("hello")))
            .await
            .unwrap();
        store
            .insert(ReconItem::new_key(&AlphaNumBytes::from("world")))
            .await
            .unwrap();
        let ids = store
            .range(
                &b"a".as_slice().into(),
                &b"z".as_slice().into(),
                0,
                usize::MAX,
            )
            .await
            .unwrap();
        expect![[r#"
        [
            Bytes(
                "hello",
            ),
            Bytes(
                "world",
            ),
        ]
        "#]]
        .assert_debug_eq(&ids.collect::<Vec<AlphaNumBytes>>());
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
            &store
                .insert(ReconItem::new_key(&AlphaNumBytes::from("hello")))
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
            &store
                .insert(ReconItem::new_key(&AlphaNumBytes::from("hello")))
                .await,
        );
    }

    #[test(tokio::test)]
    async fn test_first_and_last() {
        let mut store = new_store().await;
        store
            .insert(ReconItem::new_key(&AlphaNumBytes::from("hello")))
            .await
            .unwrap();
        store
            .insert(ReconItem::new_key(&AlphaNumBytes::from("world")))
            .await
            .unwrap();

        // Only one key in range
        let ret = store
            .first_and_last(&AlphaNumBytes::from("a"), &AlphaNumBytes::from("j"))
            .await
            .unwrap();
        expect![[r#"
            Some(
                (
                    Bytes(
                        "hello",
                    ),
                    Bytes(
                        "hello",
                    ),
                ),
            )
        "#]]
        .assert_debug_eq(&ret);

        // No keys in range
        let ret = store
            .first_and_last(&AlphaNumBytes::from("j"), &AlphaNumBytes::from("p"))
            .await
            .unwrap();
        expect![[r#"
            None
        "#]]
        .assert_debug_eq(&ret);

        // Two keys in range
        let ret = store
            .first_and_last(&AlphaNumBytes::from("a"), &AlphaNumBytes::from("z"))
            .await
            .unwrap();
        expect![[r#"
            Some(
                (
                    Bytes(
                        "hello",
                    ),
                    Bytes(
                        "world",
                    ),
                ),
            )
        "#]]
        .assert_debug_eq(&ret);
    }

    #[test(tokio::test)]
    async fn test_store_value_for_key() {
        let mut store = new_store().await;
        let key = AlphaNumBytes::from("hello");
        let store_value = AlphaNumBytes::from("world");
        store
            .insert(ReconItem::new_with_value(&key, store_value.as_slice()))
            .await
            .unwrap();
        let value = store.value_for_key(&key).await.unwrap().unwrap();
        expect![[r#"776f726c64"#]].assert_eq(hex::encode(&value).as_str());
    }
    #[test(tokio::test)]
    async fn test_keys_with_missing_value() {
        let mut store = new_store().await;
        let key = AlphaNumBytes::from("hello");
        store.insert(ReconItem::new(&key, None)).await.unwrap();
        let missing_keys = store
            .keys_with_missing_values(
                (AlphaNumBytes::min_value(), AlphaNumBytes::max_value()).into(),
            )
            .await
            .unwrap();
        expect![[r#"
            [
                Bytes(
                    "hello",
                ),
            ]
        "#]]
        .assert_debug_eq(&missing_keys);

        store.insert(ReconItem::new(&key, Some(&[]))).await.unwrap();
        let missing_keys = store
            .keys_with_missing_values(
                (AlphaNumBytes::min_value(), AlphaNumBytes::max_value()).into(),
            )
            .await
            .unwrap();
        expect![[r#"
            []
        "#]]
        .assert_debug_eq(&missing_keys);
    }
}
