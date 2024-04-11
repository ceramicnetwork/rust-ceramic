#![warn(missing_docs, missing_debug_implementations, clippy::all)]

use anyhow::Result;
use async_trait::async_trait;
use ceramic_core::{Interest, RangeOpen};
use recon::{AssociativeHash, HashCount, InsertResult, Key, ReconItem, Sha256a};
use sqlx::Row;
use tracing::instrument;

use crate::{DbTx, SqlitePool};

#[derive(Debug, Clone)]
/// InterestStore is a [`recon::Store`] implementation for Interests.
pub struct SqliteInterestStore {
    pool: SqlitePool,
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
    async fn insert_item_int(&self, item: &Interest, conn: &mut DbTx<'_>) -> Result<bool> {
        let new_key = self.insert_key_int(item, conn).await?;
        Ok(new_key)
    }

    async fn insert_key_int(&self, key: &Interest, conn: &mut DbTx<'_>) -> Result<bool> {
        let key_insert = sqlx::query(
            "INSERT INTO ceramic_one_interest (
                    order_key,
                    ahash_0, ahash_1, ahash_2, ahash_3,
                    ahash_4, ahash_5, ahash_6, ahash_7
                ) VALUES (
                    $1, 
                    $2, $3, $4, $5,
                    $6, $7, $8, $9
                );",
        );

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
            order_key
        FROM
            ceramic_one_interest
        WHERE
            order_key > $1 AND order_key < $2
        ORDER BY
            order_key ASC
        LIMIT
            $3
        OFFSET
            $4;
        ",
        );
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
impl ceramic_api::AccessInterestStore for SqliteInterestStore {
    async fn insert(&self, key: Interest) -> Result<bool> {
        self.insert_item(&key).await
    }
    async fn range(
        &self,
        start: &Interest,
        end: &Interest,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Interest>> {
        Ok(self.range_int(start, end, offset, limit).await?.collect())
    }
}

#[async_trait]
impl recon::Store for SqliteInterestStore {
    type Key = Interest;
    type Hash = Sha256a;

    /// Returns true if the key was new. The value is always updated if included
    async fn insert(&self, item: &ReconItem<'_, Self::Key>) -> Result<bool> {
        // interests don't have values, if someone gives us something we throw an error but allow None/vec![]
        if let Some(val) = item.value.as_ref() {
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
    async fn insert_many(&self, items: &[ReconItem<'_, Interest>]) -> Result<InsertResult> {
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
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<HashCount<Self::Hash>> {
        if left_fencepost >= right_fencepost {
            return Ok(HashCount::new(Sha256a::identity(), 0));
        }

        let query = sqlx::query(
            "SELECT
               TOTAL(ahash_0) & 0xFFFFFFFF, TOTAL(ahash_1) & 0xFFFFFFFF,
               TOTAL(ahash_2) & 0xFFFFFFFF, TOTAL(ahash_3) & 0xFFFFFFFF,
               TOTAL(ahash_4) & 0xFFFFFFFF, TOTAL(ahash_5) & 0xFFFFFFFF,
               TOTAL(ahash_6) & 0xFFFFFFFF, TOTAL(ahash_7) & 0xFFFFFFFF,
               COUNT(1)
             FROM ceramic_one_interest WHERE order_key > $1 AND order_key < $2;",
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
        Ok(HashCount::new(Sha256a::from(bytes), count))
    }

    #[instrument(skip(self))]
    async fn range(
        &self,
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
        &self,
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
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<usize> {
        let query = sqlx::query(
            "
        SELECT
            count(order_key)
        FROM
            ceramic_one_interest
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
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        let query = sqlx::query(
            "
    SELECT
        order_key
    FROM
        ceramic_one_interest
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
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        let query = sqlx::query(
            "
        SELECT
            order_key
        FROM
            ceramic_one_interest
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
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<(Self::Key, Self::Key)>> {
        let query = sqlx::query(
            "
        SELECT first.order_key, last.order_key
        FROM
            (
                SELECT order_key
                FROM ceramic_one_interest
                WHERE
                    order_key > $1 AND order_key < $2
                ORDER BY order_key ASC
                LIMIT 1
            ) as first
        JOIN
            (
                SELECT order_key
                FROM ceramic_one_interest
                WHERE
                    order_key > $1 AND order_key < $2
                ORDER BY order_key DESC
                LIMIT 1
            ) as last
        ;",
        );
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
    async fn value_for_key(&self, _key: &Self::Key) -> Result<Option<Vec<u8>>> {
        Ok(Some(vec![]))
    }

    #[instrument(skip(self))]
    async fn keys_with_missing_values(
        &self,
        _range: RangeOpen<Self::Key>,
    ) -> Result<Vec<Self::Key>> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod interest_tests {
    use std::{collections::BTreeSet, str::FromStr};

    use super::*;

    use ceramic_api::AccessInterestStore;
    use ceramic_core::{
        interest::{Builder, WithPeerId},
        PeerId,
    };
    use rand::{thread_rng, Rng};
    use recon::{AssociativeHash, Key, ReconItem, Sha256a, Store};

    use expect_test::expect;
    use test_log::test;

    const SORT_KEY: &str = "model";
    const PEER_ID: &str = "1AdgHpWeBKTU3F2tUkAQqL2Y2Geh4QgHJwcWMPuiY1qiRQ";

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

    async fn new_store() -> SqliteInterestStore {
        let conn = SqlitePool::connect_in_memory().await.unwrap();
        SqliteInterestStore::new(conn).await.unwrap()
    }

    #[test(tokio::test)]
    // This is the same as the recon::Store range test, but with the interest store (hits all its methods)
    async fn access_interest_model() {
        let store = new_store().await;
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
            &random_interest_min(),
            &random_interest_max(),
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
    async fn test_range_query() {
        let mut store = new_store().await;
        let interest_0 = random_interest(None, None);
        let interest_1 = random_interest(None, None);
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
        let store = new_store().await;
        let interest_0 = random_interest(None, None);
        let interest_1 = random_interest(None, None);
        AccessInterestStore::insert(&store, interest_0.clone())
            .await
            .unwrap();
        AccessInterestStore::insert(&store, interest_1.clone())
            .await
            .unwrap();
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
