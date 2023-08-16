#![warn(missing_docs, missing_debug_implementations, clippy::all)]

use crate::{AssociativeHash, Key, Store};
use anyhow::Result;
use async_trait::async_trait;
use sqlx::{Row, SqlitePool};
use std::marker::PhantomData;
use tracing::{debug, instrument};

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
            block_retrieved BOOL, -- indicates if we still want the block
            PRIMARY KEY(sort_key, key)
        )";

        sqlx::query(CREATE_RECON_TABLE).execute(&self.pool).await?;
        Ok(())
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

    // Ok(true): inserted the key
    // Ok(false): did not insert the key ConstraintViolation
    // Err(e): sql error
    #[instrument(skip(self))]
    async fn insert(&mut self, key: &Self::Key) -> Result<bool> {
        let query = sqlx::query(
            "INSERT INTO recon (
                sort_key, key,
                ahash_0, ahash_1, ahash_2, ahash_3,
                ahash_4, ahash_5, ahash_6, ahash_7,
                block_retrieved
            ) VALUES (
                ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?
            );",
        );
        let hash = H::digest(key);
        let resp = query
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
            .bind(false)
            .fetch_all(&self.pool)
            .await;
        match resp {
            Ok(_) => Ok(true),
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

    #[instrument(skip(self))]
    async fn hash_range(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Self::Hash> {
        if left_fencepost >= right_fencepost {
            return Ok(H::identity());
        }

        let query = sqlx::query(
            "
        SELECT
            TOTAL(ahash_0) & 0xFFFFFFFF, TOTAL(ahash_1) & 0xFFFFFFFF,
            TOTAL(ahash_2) & 0xFFFFFFFF, TOTAL(ahash_3) & 0xFFFFFFFF,
            TOTAL(ahash_4) & 0xFFFFFFFF, TOTAL(ahash_5) & 0xFFFFFFFF,
            TOTAL(ahash_6) & 0xFFFFFFFF, TOTAL(ahash_7) & 0xFFFFFFFF
        FROM
            recon
        WHERE
            sort_key = ? AND
            key > ? AND key < ?;
        ",
        );
        let row = query
            .bind(&self.sort_key)
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_one(&self.pool)
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
        Ok(H::from(bytes))
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
            .fetch_all(&self.pool)
            .await?;
        debug!(count = rows.len(), "rows");
        Ok(Box::new(rows.into_iter().map(|row| {
            let bytes: Vec<u8> = row.get(0);
            K::from(bytes)
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
            .fetch_one(&self.pool)
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
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.get(0).map(|row| {
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
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.get(0).map(|row| {
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
            .fetch_all(&self.pool)
            .await?;
        if let Some(row) = rows.get(0) {
            let first = K::from(row.get(0));
            let last = K::from(row.get(1));
            Ok(Some((first, last)))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::Sha256a;

    use ceramic_core::Bytes;
    use expect_test::expect;
    use tokio::test;
    use tracing_test::traced_test;

    async fn new_store() -> SQLiteStore<Bytes, Sha256a> {
        let conn = SqlitePool::connect("sqlite::memory:").await.unwrap();
        SQLiteStore::<Bytes, Sha256a>::new(conn, "test".to_string())
            .await
            .unwrap()
    }

    #[test]
    #[traced_test]
    async fn test_hash_range_query() {
        let mut store = new_store().await;
        store.insert(&Bytes::from("hello")).await.unwrap();
        store.insert(&Bytes::from("world")).await.unwrap();
        let hash: Sha256a = store
            .hash_range(&b"a".as_slice().into(), &b"z".as_slice().into())
            .await
            .unwrap();
        expect![[r#"7460F21C83815F5EDC682F7A4154BC09AA3A0AE5DD1A2DEDCD709888A12751CC"#]]
            .assert_eq(&hash.to_hex())
    }

    #[test]
    #[traced_test]
    async fn test_range_query() {
        let mut store = new_store().await;
        store.insert(&Bytes::from("hello")).await.unwrap();
        store.insert(&Bytes::from("world")).await.unwrap();
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
        .assert_debug_eq(&ids.collect::<Vec<Bytes>>());
    }

    #[test]
    #[traced_test]
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
        .assert_debug_eq(&store.insert(&Bytes::from("hello")).await);

        // reject the second insert of same key
        expect![
            r#"
        Ok(
            false,
        )
        "#
        ]
        .assert_debug_eq(&store.insert(&Bytes::from("hello")).await);
    }

    #[test]
    #[traced_test]
    async fn test_first_and_last() {
        let mut store = new_store().await;
        store.insert(&Bytes::from("hello")).await.unwrap();
        store.insert(&Bytes::from("world")).await.unwrap();

        // Only one key in range
        let ret = store
            .first_and_last(&Bytes::from("a"), &Bytes::from("j"))
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
            .first_and_last(&Bytes::from("j"), &Bytes::from("p"))
            .await
            .unwrap();
        expect![[r#"
            None
        "#]]
        .assert_debug_eq(&ret);

        // Two keys in range
        let ret = store
            .first_and_last(&Bytes::from("a"), &Bytes::from("z"))
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
}
