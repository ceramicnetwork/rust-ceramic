use std::{collections::BTreeSet, marker::PhantomData};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use ceramic_api::AccessModelStore;
use ceramic_core::{EventId, RangeOpen};
use cid::Cid;
use iroh_bitswap::Block;
use iroh_car::{CarHeader, CarReader, CarWriter};
use itertools::{process_results, Itertools};
use multihash::{Code, MultihashDigest};
use recon::{AssociativeHash, HashCount, InsertResult, Key, ReconItem, Sha256a};
use sqlx::Row;
use tracing::instrument;

use crate::{DbTx, SqlitePool};

/// Unified implementation of [`recon::Store`] and [`iroh_bitswap::Store`] that can expose the
/// individual blocks from the CAR files directly.
#[derive(Clone, Debug)]
pub struct ModelStore<H>
where
    H: AssociativeHash,
{
    pool: SqlitePool,
    hash: PhantomData<H>,
}

#[derive(Debug)]
struct BlockRow {
    cid: Cid,
    root: bool,
    bytes: Vec<u8>,
}

impl<H> ModelStore<H>
where
    H: AssociativeHash + std::convert::From<[u32; 8]>,
{
    /// Create an instance of the store initializing any neccessary tables.
    pub async fn new(pool: SqlitePool) -> Result<Self> {
        let store = ModelStore {
            pool,
            hash: PhantomData,
        };
        store.create_table_if_not_exists().await?;
        Ok(store)
    }

    /// Initialize the recon table.
    async fn create_table_if_not_exists(&self) -> Result<()> {
        const CREATE_STORE_KEY_TABLE: &str = "CREATE TABLE IF NOT EXISTS model_key (
            key BLOB, -- network_id sort_value controller StreamID height event_cid
            ahash_0 INTEGER, -- the ahash is decomposed as [u32; 8]
            ahash_1 INTEGER,
            ahash_2 INTEGER,
            ahash_3 INTEGER,
            ahash_4 INTEGER,
            ahash_5 INTEGER,
            ahash_6 INTEGER,
            ahash_7 INTEGER,
            value_retrieved BOOL, -- indicates if we have the value
            PRIMARY KEY(key)
        )";
        const CREATE_VALUE_RETRIEVED_INDEX: &str =
            "CREATE INDEX IF NOT EXISTS idx_key_value_retrieved
            ON model_key (key, value_retrieved)";

        const CREATE_MODEL_BLOCK_TABLE: &str = "CREATE TABLE IF NOT EXISTS model_block (
            key BLOB, -- network_id sort_value controller StreamID height event_cid
            cid BLOB, -- the cid of the Block as bytes no 0x00 prefix
            idx INTEGER, -- the index of the block in the CAR file
            root BOOL, -- when true the block is a root in the CAR file
            bytes BLOB, -- the Block
            PRIMARY KEY(key, cid)
        )";
        // TODO should this include idx or not?
        const CREATE_BLOCK_ORDER_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_model_block_cid
            ON model_block (cid)";

        let mut tx = self.pool.tx().await?;
        sqlx::query(CREATE_STORE_KEY_TABLE)
            .execute(&mut *tx)
            .await?;
        sqlx::query(CREATE_VALUE_RETRIEVED_INDEX)
            .execute(&mut *tx)
            .await?;
        sqlx::query(CREATE_MODEL_BLOCK_TABLE)
            .execute(&mut *tx)
            .await?;
        sqlx::query(CREATE_BLOCK_ORDER_INDEX)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn insert_item(&self, item: &ReconItem<'_, EventId>) -> Result<(bool, bool)> {
        let mut tx = self.pool.writer().begin().await?;
        let (new_key, new_val) = self.insert_item_int(item, &mut tx).await?;
        tx.commit().await?;
        Ok((new_key, new_val))
    }

    async fn range_with_values_int(
        &self,
        left_fencepost: &EventId,
        right_fencepost: &EventId,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (EventId, Vec<u8>)> + Send + 'static>> {
        let query = sqlx::query(
            "
        SELECT
            model_block.key, model_block.cid, model_block.root, model_block.bytes
        FROM (
            SELECT
                key
            FROM model_key
            WHERE
                key > ? AND key < ?
                AND value_retrieved = true
            ORDER BY
                key ASC
            LIMIT
                ?
            OFFSET
                ?
        ) key
        JOIN
            model_block
        ON
            key.key = model_block.key
            ORDER BY model_block.key, model_block.idx
        ;",
        );
        let all_blocks = query
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(self.pool.reader())
            .await?;

        // Consume all block into groups of blocks by their key.
        let all_blocks: Vec<(EventId, Vec<BlockRow>)> = process_results(
            all_blocks.into_iter().map(|row| {
                Cid::read_bytes(row.get::<&[u8], _>(1))
                    .map_err(anyhow::Error::from)
                    .map(|cid| {
                        (
                            EventId::from(row.get::<Vec<u8>, _>(0)),
                            cid,
                            row.get(2),
                            row.get(3),
                        )
                    })
            }),
            |blocks| {
                blocks
                    .group_by(|(key, _, _, _)| key.clone())
                    .into_iter()
                    .map(|(key, group)| {
                        (
                            key,
                            group
                                .map(|(_key, cid, root, bytes)| BlockRow { cid, root, bytes })
                                .collect::<Vec<BlockRow>>(),
                        )
                    })
                    .collect()
            },
        )?;

        let mut values: Vec<(EventId, Vec<u8>)> = Vec::new();
        for (key, blocks) in all_blocks {
            if let Some(value) = self.rebuild_car(blocks).await? {
                values.push((key.clone(), value));
            }
        }
        Ok(Box::new(values.into_iter()))
    }

    async fn value_for_key_int(&self, key: &EventId) -> Result<Option<Vec<u8>>> {
        let query = sqlx::query(
            "
            SELECT
                cid, root, bytes
            FROM model_block
            WHERE
                key=?
            ORDER BY idx
            ;",
        );
        let blocks = query
            .bind(key.as_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        self.rebuild_car(
            blocks
                .into_iter()
                .map(|row| {
                    Cid::read_bytes(row.get::<&[u8], _>(0))
                        .map_err(anyhow::Error::from)
                        .map(|cid| BlockRow {
                            cid,
                            root: row.get(1),
                            bytes: row.get(2),
                        })
                })
                .collect::<Result<Vec<_>>>()?,
        )
        .await
    }

    /// returns (new_key, new_val) tuple
    async fn insert_item_int(
        &self,
        item: &ReconItem<'_, EventId>,
        conn: &mut DbTx<'_>,
    ) -> Result<(bool, bool)> {
        // we insert the value first as it's possible we already have the key and can skip that step
        // as it happens in a transaction, we'll roll back the value insert if the key insert fails and try again
        if let Some(val) = item.value {
            // Update the value_retrieved flag, and report if the key already exists.
            let key_exists = self.update_value_retrieved_int(item.key, conn).await?;

            // Put each block from the car file
            let mut reader = CarReader::new(val).await?;
            let roots: BTreeSet<Cid> = reader.header().roots().iter().cloned().collect();
            let mut idx = 0;
            while let Some((cid, data)) = reader.next_block().await? {
                self.insert_block_int(item.key, idx, roots.contains(&cid), cid, &data.into(), conn)
                    .await?;
                idx += 1;
            }

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
    async fn update_value_retrieved_int(&self, key: &EventId, conn: &mut DbTx<'_>) -> Result<bool> {
        let update = sqlx::query("UPDATE model_key SET value_retrieved = true WHERE key = ?");
        let resp = update.bind(key.as_bytes()).execute(&mut **conn).await?;
        let rows_affected = resp.rows_affected();
        debug_assert!(rows_affected <= 1);
        Ok(rows_affected == 1)
    }

    // store a block in the db.
    async fn insert_block_int(
        &self,
        key: &EventId,
        idx: i32,
        root: bool,
        cid: Cid,
        blob: &Bytes,
        conn: &mut DbTx<'_>,
    ) -> Result<()> {
        let hash = match cid.hash().code() {
            0x12 => Code::Sha2_256.digest(blob),
            0x1b => Code::Keccak256.digest(blob),
            0x11 => return Err(anyhow!("Sha1 not supported")),
            _ => {
                return Err(anyhow!(
                    "multihash type {:#x} not Sha2_256, Keccak256",
                    cid.hash().code(),
                ))
            }
        };
        if cid.hash().to_bytes() != hash.to_bytes() {
            return Err(anyhow!(
                "cid did not match blob {} != {}",
                hex::encode(cid.hash().to_bytes()),
                hex::encode(hash.to_bytes())
            ));
        }

        sqlx::query("INSERT INTO model_block (key, idx, root, cid, bytes) VALUES (?, ?, ?, ?, ?)")
            .bind(key.as_bytes())
            .bind(idx)
            .bind(root)
            .bind(cid.to_bytes())
            .bind(blob.to_vec())
            .execute(&mut **conn)
            .await?;
        Ok(())
    }

    async fn insert_key_int(
        &self,
        key: &EventId,
        has_value: bool,
        conn: &mut DbTx<'_>,
    ) -> Result<bool> {
        let key_insert = sqlx::query(
            "INSERT INTO model_key (
                    key,
                    ahash_0, ahash_1, ahash_2, ahash_3,
                    ahash_4, ahash_5, ahash_6, ahash_7,
                    value_retrieved
                ) VALUES (
                    ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?
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

    async fn rebuild_car(&self, blocks: Vec<BlockRow>) -> Result<Option<Vec<u8>>> {
        if blocks.is_empty() {
            return Ok(None);
        }

        let size = blocks.iter().fold(0, |sum, row| sum + row.bytes.len());
        let roots: Vec<Cid> = blocks
            .iter()
            .filter(|row| row.root)
            .map(|row| row.cid)
            .collect();
        // Reconstruct the car file
        // TODO figure out a better capacity calculation
        let mut car = Vec::with_capacity(size + 100 * blocks.len());
        let mut writer = CarWriter::new(CarHeader::V1(roots.into()), &mut car);
        for BlockRow {
            cid,
            bytes,
            root: _,
        } in blocks
        {
            writer.write(cid, bytes).await?;
        }
        writer.finish().await?;
        Ok(Some(car))
    }

    /// Returns all the keys found after the given row_id.
    /// Uses the rowid of the value (block) table and makes sure to flatten keys
    /// when there are multiple blocks for a single key. This relies on the fact that
    /// we insert the blocks in order inside a transaction and that we don't delete, which
    /// means that the all the entries for a key will be contiguous.
    pub async fn new_keys_since_value_rowid(
        &self,
        row_id: i64,
        limit: i64,
    ) -> Result<(i64, Vec<EventId>)> {
        let query = sqlx::query(
            "WITH entries AS (
                    SELECT key, MAX(rowid) as max_rowid
                FROM model_block
                    WHERE rowid >= ? -- we return rowid+1 so we must match it next search
                GROUP BY key
                ORDER BY rowid
                LIMIT ?
            )
            SELECT 
                key, 
                (SELECT MAX(max_rowid) + 1 FROM entries) as new_highwater_mark 
            from entries;",
        );
        let rows = query
            .bind(row_id)
            .bind(limit)
            .fetch_all(self.pool.reader())
            .await?;
        // every row has the same new_highwater_mark value
        let row_id: i64 = rows
            .first()
            .and_then(|r| r.get("new_highwater_mark"))
            .unwrap_or(0);
        let rows = rows
            .into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row.get(0);
                EventId::from(bytes)
            })
            .collect();

        Ok((row_id, rows))
    }
}

#[async_trait]
impl<H> recon::Store for ModelStore<H>
where
    H: AssociativeHash,
{
    type Key = EventId;
    type Hash = H;

    /// Returns true if the key was new. The value is always updated if included
    async fn insert(&mut self, item: ReconItem<'_, Self::Key>) -> Result<bool> {
        let (new, _new_val) = self.insert_item(&item).await?;
        Ok(new)
    }

    /// Insert new keys into the key space.
    /// Returns true if a key did not previously exist.
    async fn insert_many<'a, I>(&mut self, items: I) -> Result<InsertResult>
    where
        I: ExactSizeIterator<Item = ReconItem<'a, EventId>> + Send + Sync,
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
            return Ok(HashCount::new(H::identity(), 0));
        }

        let query = sqlx::query(
            "SELECT
               TOTAL(ahash_0) & 0xFFFFFFFF, TOTAL(ahash_1) & 0xFFFFFFFF,
               TOTAL(ahash_2) & 0xFFFFFFFF, TOTAL(ahash_3) & 0xFFFFFFFF,
               TOTAL(ahash_4) & 0xFFFFFFFF, TOTAL(ahash_5) & 0xFFFFFFFF,
               TOTAL(ahash_6) & 0xFFFFFFFF, TOTAL(ahash_7) & 0xFFFFFFFF,
               COUNT(1)
             FROM model_key WHERE key > ? AND key < ?;",
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
        let query = sqlx::query(
            "
        SELECT
            key
        FROM
            model_key
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
            EventId::from(bytes)
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
        self.range_with_values_int(left_fencepost, right_fencepost, offset, limit)
            .await
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
            model_key
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
        model_key
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
            EventId::from(bytes)
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
            model_key
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
            EventId::from(bytes)
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
                FROM model_key
                WHERE
                    key > ? AND key < ?
                ORDER BY key ASC
                LIMIT 1
            ) as first
        JOIN
            (
                SELECT key
                FROM model_key
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
            let first = EventId::from(row.get::<Vec<u8>, _>(0));
            let last = EventId::from(row.get::<Vec<u8>, _>(1));
            Ok(Some((first, last)))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self))]
    async fn value_for_key(&mut self, key: &Self::Key) -> Result<Option<Vec<u8>>> {
        self.value_for_key_int(key).await
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
            FROM model_key
            WHERE
                key > ?
                AND key < ?
                AND value_retrieved = false
            ;",
        );
        let row = query
            .bind(range.start.as_bytes())
            .bind(range.end.as_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        Ok(row
            .into_iter()
            .map(|row| EventId::from(row.get::<Vec<u8>, _>(0)))
            .collect())
    }
}

#[async_trait]
impl iroh_bitswap::Store for ModelStore<Sha256a> {
    async fn get_size(&self, cid: &Cid) -> Result<usize> {
        Ok(
            sqlx::query("SELECT length(bytes) FROM model_block WHERE cid = ?;")
                .bind(cid.to_bytes())
                .fetch_one(self.pool.reader())
                .await?
                .get::<'_, i64, _>(0) as usize,
        )
    }

    async fn get(&self, cid: &Cid) -> Result<Block> {
        Ok(Block::new(
            sqlx::query("SELECT bytes FROM model_block WHERE cid = ?;")
                .bind(cid.to_bytes())
                .fetch_one(self.pool.reader())
                .await?
                .get::<'_, Vec<u8>, _>(0)
                .into(),
            cid.to_owned(),
        ))
    }

    async fn has(&self, cid: &Cid) -> Result<bool> {
        Ok(
            sqlx::query("SELECT count(1) FROM model_block WHERE cid = ?;")
                .bind(cid.to_bytes())
                .fetch_one(self.pool.reader())
                .await?
                .get::<'_, i64, _>(0)
                > 0,
        )
    }
}

#[async_trait::async_trait]
impl AccessModelStore for ModelStore<Sha256a> {
    type Key = EventId;
    type Hash = Sha256a;

    async fn insert(&self, key: Self::Key, value: Option<Vec<u8>>) -> Result<(bool, bool)> {
        self.insert_item(&ReconItem::new(&key, value.as_deref()))
            .await
    }

    async fn range_with_values(
        &self,
        start: Self::Key,
        end: Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(Self::Key, Vec<u8>)>> {
        let res = self
            .range_with_values_int(&start, &end, offset, limit)
            .await?;
        Ok(res.collect())
    }
    async fn value_for_key(&self, key: Self::Key) -> Result<Option<Vec<u8>>> {
        self.value_for_key_int(&key).await
    }

    async fn keys_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
    ) -> anyhow::Result<(i64, Vec<Self::Key>)> {
        self.new_keys_since_value_rowid(highwater, limit).await
    }
}

#[cfg(test)]
mod test {

    use crate::tests::*;

    use super::*;
    use expect_test::expect;
    use recon::{Key, ReconItem};
    use test_log::test;

    #[test(tokio::test)]
    async fn hash_range_query() {
        let mut store = new_store().await;
        recon::Store::insert(
            &mut store,
            ReconItem::new_key(&random_event_id(
                Some(1),
                Some("baeabeiazgwnti363jifhxaeaegbluw4ogcd2t5hsjaglo46wuwcgajqa5u"),
            )),
        )
        .await
        .unwrap();
        recon::Store::insert(
            &mut store,
            ReconItem::new_key(&random_event_id(
                Some(2),
                Some("baeabeihyl35xdlfju3zrkvy2exmnl6wics3rc5ppz7hwg7l7g4brbtnpny"),
            )),
        )
        .await
        .unwrap();
        let hash =
            recon::Store::hash_range(&mut store, &random_event_id_min(), &random_event_id_max())
                .await
                .unwrap();
        expect!["65C7A25327CC05C19AB5812103EEB8D1156595832B453C7BAC6A186F4811FA0A#2"]
            .assert_eq(&format!("{hash}"));
    }

    #[test(tokio::test)]
    async fn range_query() {
        let mut store = new_store().await;
        recon::Store::insert(
            &mut store,
            ReconItem::new_key(&random_event_id(
                Some(1),
                Some("baeabeichhhmbhsic4maraneqf5gkhekgzcawhtpj3fh6opjtglznapz524"),
            )),
        )
        .await
        .unwrap();
        recon::Store::insert(
            &mut store,
            ReconItem::new_key(&random_event_id(
                Some(2),
                Some("baeabeibmek7v4ljsu575ohgjhovdxhcw6p6oivgb55hzkeap5po7ghzqty"),
            )),
        )
        .await
        .unwrap();
        let ids = recon::Store::range(
            &mut store,
            &random_event_id_min(),
            &random_event_id_max(),
            0,
            usize::MAX,
        )
        .await
        .unwrap();
        expect![[r#"
            [
                EventId {
                    network_id: Some(
                        2,
                    ),
                    separator: Some(
                        "b51217a029eb540d",
                    ),
                    controller: Some(
                        "4f16d8429ae87f86",
                    ),
                    stream_id: Some(
                        "ead3ca3c",
                    ),
                    event_height: Some(
                        1,
                    ),
                    cid: Some(
                        "baeabeichhhmbhsic4maraneqf5gkhekgzcawhtpj3fh6opjtglznapz524",
                    ),
                },
                EventId {
                    network_id: Some(
                        2,
                    ),
                    separator: Some(
                        "b51217a029eb540d",
                    ),
                    controller: Some(
                        "4f16d8429ae87f86",
                    ),
                    stream_id: Some(
                        "ead3ca3c",
                    ),
                    event_height: Some(
                        2,
                    ),
                    cid: Some(
                        "baeabeibmek7v4ljsu575ohgjhovdxhcw6p6oivgb55hzkeap5po7ghzqty",
                    ),
                },
            ]
        "#]]
        .assert_debug_eq(&ids.collect::<Vec<EventId>>());
    }

    #[test(tokio::test)]
    async fn range_query_with_values() {
        let mut store = new_store().await;
        // Write three keys, two with values and one without
        let one_id = random_event_id(
            Some(1),
            Some("baeabeichhhmbhsic4maraneqf5gkhekgzcawhtpj3fh6opjtglznapz524"),
        );
        let two_id = random_event_id(
            Some(2),
            Some("baeabeibmek7v4ljsu575ohgjhovdxhcw6p6oivgb55hzkeap5po7ghzqty"),
        );
        let (_one_blocks, one_car) = build_car_file(2).await;
        let (_two_blocks, two_car) = build_car_file(3).await;
        recon::Store::insert(&mut store, ReconItem::new(&one_id, Some(&one_car)))
            .await
            .unwrap();
        recon::Store::insert(&mut store, ReconItem::new(&two_id, Some(&two_car)))
            .await
            .unwrap();
        // Insert new event without a value to ensure we skip it in the query
        recon::Store::insert(
            &mut store,
            ReconItem::new(
                &random_event_id(
                    Some(2),
                    Some("baeabeicyxeqioadjgy6v6cpy62a3gngylax54sds7rols2b67yetzaw5r4"),
                ),
                None,
            ),
        )
        .await
        .unwrap();
        let values: Vec<(EventId, Vec<u8>)> = recon::Store::range_with_values(
            &mut store,
            &random_event_id_min(),
            &random_event_id_max(),
            0,
            usize::MAX,
        )
        .await
        .unwrap()
        .collect();

        assert_eq!(vec![(one_id, one_car), (two_id, two_car)], values);
    }

    #[test(tokio::test)]
    async fn double_insert() {
        let mut store = new_store().await;
        let id = random_event_id(Some(10), None);

        // first insert reports its a new key
        expect![
            r#"
            Ok(
                true,
            )
            "#
        ]
        .assert_debug_eq(&recon::Store::insert(&mut store, ReconItem::new_key(&id)).await);

        // second insert of same key reports it already existed
        expect![
            r#"
            Ok(
                false,
            )
            "#
        ]
        .assert_debug_eq(&recon::Store::insert(&mut store, ReconItem::new_key(&id)).await);
    }

    #[test(tokio::test)]
    async fn double_insert_with_value() {
        let mut store = new_store().await;
        let id = random_event_id(Some(10), None);
        let (_, car) = build_car_file(2).await;

        let item = ReconItem::new_with_value(&id, &car);

        // do take the first one
        expect![
            r#"
            Ok(
                true,
            )
            "#
        ]
        .assert_debug_eq(&recon::Store::insert(&mut store, item.clone()).await);

        // reject the second insert of same key with value. Do not override values
        expect![[r#"
            Err(
                Database(
                    SqliteError {
                        code: 1555,
                        message: "UNIQUE constraint failed: model_block.key, model_block.cid",
                    },
                ),
            )
        "#]]
        .assert_debug_eq(&recon::Store::insert(&mut store, item).await);
    }

    #[test(tokio::test)]
    async fn update_missing_value() {
        let mut store = new_store().await;
        let id = random_event_id(Some(10), None);
        let (_, car) = build_car_file(2).await;

        let item_without_value = ReconItem::new_key(&id);
        let item_with_value = ReconItem::new_with_value(&id, &car);

        // do take the first one
        expect![
            r#"
            Ok(
                true,
            )
            "#
        ]
        .assert_debug_eq(&recon::Store::insert(&mut store, item_without_value).await);

        // accept the second insert of same key with the value
        expect![[r#"
            Ok(
                false,
            )
        "#]]
        .assert_debug_eq(&recon::Store::insert(&mut store, item_with_value).await);
    }

    #[test(tokio::test)]
    async fn first_and_last() {
        let mut store = new_store().await;
        recon::Store::insert(
            &mut store,
            ReconItem::new_key(&random_event_id(
                Some(10),
                Some("baeabeie2bcird7765t7646jcoatd72tfn2tscdaap7g6kvvy7k43s34aau"),
            )),
        )
        .await
        .unwrap();
        recon::Store::insert(
            &mut store,
            ReconItem::new_key(&random_event_id(
                Some(11),
                Some("baeabeianftvrst5bja422dod6uf42pmwkwix6rprguanwsxylfut56e3ue"),
            )),
        )
        .await
        .unwrap();

        // Only one key in range
        let ret = recon::Store::first_and_last(
            &mut store,
            &event_id_builder().with_event_height(9).build_fencepost(),
            &event_id_builder().with_event_height(11).build_fencepost(),
        )
        .await
        .unwrap();
        expect![[r#"
            Some(
                (
                    EventId {
                        network_id: Some(
                            2,
                        ),
                        separator: Some(
                            "b51217a029eb540d",
                        ),
                        controller: Some(
                            "4f16d8429ae87f86",
                        ),
                        stream_id: Some(
                            "ead3ca3c",
                        ),
                        event_height: Some(
                            10,
                        ),
                        cid: Some(
                            "baeabeie2bcird7765t7646jcoatd72tfn2tscdaap7g6kvvy7k43s34aau",
                        ),
                    },
                    EventId {
                        network_id: Some(
                            2,
                        ),
                        separator: Some(
                            "b51217a029eb540d",
                        ),
                        controller: Some(
                            "4f16d8429ae87f86",
                        ),
                        stream_id: Some(
                            "ead3ca3c",
                        ),
                        event_height: Some(
                            10,
                        ),
                        cid: Some(
                            "baeabeie2bcird7765t7646jcoatd72tfn2tscdaap7g6kvvy7k43s34aau",
                        ),
                    },
                ),
            )
        "#]]
        .assert_debug_eq(&ret);

        // No keys in range
        let ret = recon::Store::first_and_last(
            &mut store,
            &event_id_builder().with_event_height(12).build_fencepost(),
            &event_id_builder().with_max_event_height().build_fencepost(),
        )
        .await
        .unwrap();
        expect![[r#"
            None
        "#]]
        .assert_debug_eq(&ret);

        // Two keys in range
        let ret = recon::Store::first_and_last(
            &mut store,
            &random_event_id_min(),
            &random_event_id_max(),
        )
        .await
        .unwrap();
        expect![[r#"
            Some(
                (
                    EventId {
                        network_id: Some(
                            2,
                        ),
                        separator: Some(
                            "b51217a029eb540d",
                        ),
                        controller: Some(
                            "4f16d8429ae87f86",
                        ),
                        stream_id: Some(
                            "ead3ca3c",
                        ),
                        event_height: Some(
                            10,
                        ),
                        cid: Some(
                            "baeabeie2bcird7765t7646jcoatd72tfn2tscdaap7g6kvvy7k43s34aau",
                        ),
                    },
                    EventId {
                        network_id: Some(
                            2,
                        ),
                        separator: Some(
                            "b51217a029eb540d",
                        ),
                        controller: Some(
                            "4f16d8429ae87f86",
                        ),
                        stream_id: Some(
                            "ead3ca3c",
                        ),
                        event_height: Some(
                            11,
                        ),
                        cid: Some(
                            "baeabeianftvrst5bja422dod6uf42pmwkwix6rprguanwsxylfut56e3ue",
                        ),
                    },
                ),
            )
        "#]]
        .assert_debug_eq(&ret);
    }

    #[test(tokio::test)]
    async fn store_value_for_key() {
        let mut store = new_store().await;
        let key = random_event_id(None, None);
        let (_, store_value) = build_car_file(3).await;
        recon::Store::insert(
            &mut store,
            ReconItem::new_with_value(&key, store_value.as_slice()),
        )
        .await
        .unwrap();
        let value = recon::Store::value_for_key(&mut store, &key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hex::encode(store_value), hex::encode(value));
    }
    #[test(tokio::test)]
    async fn keys_with_missing_value() {
        let mut store = new_store().await;
        let key = random_event_id(
            Some(4),
            Some("baeabeigc5edwvc47ul6belpxk3lgddipri5hw6f347s6ur4pdzwceprqbu"),
        );
        recon::Store::insert(&mut store, ReconItem::new(&key, None))
            .await
            .unwrap();
        let missing_keys = recon::Store::keys_with_missing_values(
            &mut store,
            (EventId::min_value(), EventId::max_value()).into(),
        )
        .await
        .unwrap();
        expect![[r#"
            [
                EventId {
                    network_id: Some(
                        2,
                    ),
                    separator: Some(
                        "b51217a029eb540d",
                    ),
                    controller: Some(
                        "4f16d8429ae87f86",
                    ),
                    stream_id: Some(
                        "ead3ca3c",
                    ),
                    event_height: Some(
                        4,
                    ),
                    cid: Some(
                        "baeabeigc5edwvc47ul6belpxk3lgddipri5hw6f347s6ur4pdzwceprqbu",
                    ),
                },
            ]
        "#]]
        .assert_debug_eq(&missing_keys);

        let (_, value) = build_car_file(2).await;
        recon::Store::insert(&mut store, ReconItem::new(&key, Some(&value)))
            .await
            .unwrap();
        let missing_keys = recon::Store::keys_with_missing_values(
            &mut store,
            (EventId::min_value(), EventId::max_value()).into(),
        )
        .await
        .unwrap();
        expect![[r#"
                []
            "#]]
        .assert_debug_eq(&missing_keys);
    }

    #[test(tokio::test)]
    async fn read_value_as_block() {
        let mut store = new_store().await;
        let key = random_event_id(None, None);
        let (blocks, store_value) = build_car_file(3).await;
        recon::Store::insert(
            &mut store,
            ReconItem::new_with_value(&key, store_value.as_slice()),
        )
        .await
        .unwrap();
        let value = recon::Store::value_for_key(&mut store, &key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hex::encode(store_value), hex::encode(value));

        // Read each block from the CAR
        for block in blocks {
            let value = iroh_bitswap::Store::get(&store, &block.cid).await.unwrap();
            assert_eq!(block, value);
        }
    }

    // stores 3 keys with 3,5,10 block long CAR files
    // each one takes n+1 blocks as it needs to store the root and all blocks so we expect 3+5+10+3=21
    async fn prep_highwater_tests(store: &mut ModelStore<Sha256a>) -> (EventId, EventId, EventId) {
        let key_a = random_event_id(None, None);
        let key_b = random_event_id(None, None);
        let key_c = random_event_id(None, None);
        for (x, key) in [3, 5, 10].into_iter().zip([&key_a, &key_b, &key_c]) {
            let (_blocks, store_value) = build_car_file(x).await;
            assert_eq!(_blocks.len(), x);
            recon::Store::insert(
                store,
                ReconItem::new_with_value(key, store_value.as_slice()),
            )
            .await
            .unwrap();
        }

        (key_a, key_b, key_c)
    }

    #[test(tokio::test)]
    async fn keys_since_highwater_mark_all() {
        let mut store: ModelStore<Sha256a> = new_store().await;
        let (key_a, key_b, key_c) = prep_highwater_tests(&mut store).await;

        let (hw, res) = store.new_keys_since_value_rowid(0, 10).await.unwrap();
        assert_eq!(3, res.len());
        assert_eq!(22, hw); // see comment in prep_highwater_tests
        assert_eq!([key_a, key_b, key_c], res.as_slice());
    }

    #[test(tokio::test)]
    async fn keys_since_highwater_mark_limit_1() {
        let mut store: ModelStore<Sha256a> = new_store().await;
        let (key_a, _key_b, _key_c) = prep_highwater_tests(&mut store).await;

        let (hw, res) = store.new_keys_since_value_rowid(0, 1).await.unwrap();
        assert_eq!(1, res.len());
        assert_eq!(5, hw); // see comment in prep_highwater_tests
        assert_eq!([key_a], res.as_slice());
    }

    #[test(tokio::test)]
    async fn keys_since_highwater_mark_middle_start() {
        let mut store: ModelStore<Sha256a> = new_store().await;
        let (key_a, key_b, key_c) = prep_highwater_tests(&mut store).await;

        // starting at rowid 1 which is in the middle of key A should still return key A
        let (hw, res) = store.new_keys_since_value_rowid(1, 2).await.unwrap();
        assert_eq!(2, res.len());
        assert_eq!(11, hw); // see comment in prep_highwater_tests
        assert_eq!([key_a, key_b], res.as_slice());

        let (hw, res) = store.new_keys_since_value_rowid(hw, 1).await.unwrap();
        assert_eq!(1, res.len());
        assert_eq!(22, hw); // see comment in prep_highwater_tests
        assert_eq!([key_c], res.as_slice());
    }
}
