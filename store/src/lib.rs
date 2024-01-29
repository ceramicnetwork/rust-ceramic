//! A unified implementation of both the [`recon::Store`] and [`iroh_bitswap::Store`] traits.
//! This unified implementation allows for exposing Recon values as IPFS blocks
#![warn(missing_docs)]

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use ceramic_core::{DbTx, EventId, RangeOpen, SqlitePool};
use cid::Cid;
use iroh_bitswap::Block;
use iroh_car::{CarHeader, CarReader, CarWriter};
use itertools::{process_results, Itertools};
use multihash::{Code, MultihashDigest};
use recon::{AssociativeHash, HashCount, InsertResult, Key, ReconItem, Sha256a};
use sqlx::Row;
use tracing::instrument;

const SORT_KEY: &str = "model";

/// Unified implementation of [`recon::Store`] and [`iroh_bitswap::Store`] that can expose the
/// individual blocks from the CAR files directly.
#[derive(Clone, Debug)]
pub struct Store {
    pool: SqlitePool,
}

#[derive(Debug)]
struct BlockRow {
    cid: Cid,
    root: bool,
    bytes: Vec<u8>,
}

impl Store {
    /// Create an instance of the store initializing any neccessary tables.
    pub async fn new(pool: SqlitePool) -> Result<Self> {
        let mut store = Store { pool };
        store.create_table_if_not_exists().await?;
        Ok(store)
    }

    /// Initialize the recon table.
    async fn create_table_if_not_exists(&mut self) -> Result<()> {
        // Do we want to remove CID from the table?
        const CREATE_STORE_KEY_TABLE: &str = "CREATE TABLE IF NOT EXISTS store_key (
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
            value_retrieved BOOL, -- indicates if we have the value
            PRIMARY KEY(sort_key, key)
        )";
        const CREATE_VALUE_RETRIEVED_INDEX: &str =
            "CREATE INDEX IF NOT EXISTS idx_key_value_retrieved
            ON store_key (sort_key, key, value_retrieved)";

        const CREATE_STORE_BLOCK_TABLE: &str = "CREATE TABLE IF NOT EXISTS store_block (
            sort_key TEXT, -- the field in the event header to sort by e.g. model
            key BLOB, -- network_id sort_value controller StreamID height event_cid
            idx INTEGER, -- the index of the block in the CAR file
            root BOOL, -- when true the block is a root in the CAR file
            cid BLOB, -- the cid of the Block as bytes no 0x00 prefix
            bytes BLOB, -- the Block
            PRIMARY KEY(cid)
        )";
        // TODO should this include idx or not?
        const CREATE_BLOCK_ORDER_INDEX: &str = "CREATE INDEX IF NOT EXISTS idx_block_idx
            ON store_block (sort_key, key)";

        let mut tx = self.pool.tx().await?;
        sqlx::query(CREATE_STORE_KEY_TABLE)
            .execute(&mut *tx)
            .await?;
        sqlx::query(CREATE_VALUE_RETRIEVED_INDEX)
            .execute(&mut *tx)
            .await?;
        sqlx::query(CREATE_STORE_BLOCK_TABLE)
            .execute(&mut *tx)
            .await?;
        sqlx::query(CREATE_BLOCK_ORDER_INDEX)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }
    /// returns (new_key, new_val) tuple
    async fn insert_item_int(
        &mut self,
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
    async fn update_value_retrieved_int(
        &mut self,
        key: &EventId,
        conn: &mut DbTx<'_>,
    ) -> Result<bool> {
        let update = sqlx::query(
            "UPDATE store_key SET value_retrieved = true WHERE sort_key = ? AND key = ?",
        );
        let resp = update
            .bind(SORT_KEY)
            .bind(key.as_bytes())
            .execute(&mut **conn)
            .await?;
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

        sqlx::query(
            "INSERT INTO store_block (sort_key, key, idx, root, cid, bytes) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(SORT_KEY)
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
        &mut self,
        key: &EventId,
        has_value: bool,
        conn: &mut DbTx<'_>,
    ) -> Result<bool> {
        let key_insert = sqlx::query(
            "INSERT INTO store_key (
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

        let hash = Sha256a::digest(key);
        let resp = key_insert
            .bind(SORT_KEY)
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

    async fn rebuild_car(&mut self, blocks: Vec<BlockRow>) -> Result<Option<Vec<u8>>> {
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
}

#[async_trait]
impl recon::Store for Store {
    type Key = EventId;
    type Hash = Sha256a;

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
            return Ok(HashCount::new(Sha256a::identity(), 0));
        }

        let query = sqlx::query(
            "SELECT
               TOTAL(ahash_0) & 0xFFFFFFFF, TOTAL(ahash_1) & 0xFFFFFFFF,
               TOTAL(ahash_2) & 0xFFFFFFFF, TOTAL(ahash_3) & 0xFFFFFFFF,
               TOTAL(ahash_4) & 0xFFFFFFFF, TOTAL(ahash_5) & 0xFFFFFFFF,
               TOTAL(ahash_6) & 0xFFFFFFFF, TOTAL(ahash_7) & 0xFFFFFFFF,
               COUNT(1)
             FROM store_key WHERE sort_key = ? AND key > ? AND key < ?;",
        );
        let row = query
            .bind(SORT_KEY)
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
            store_key
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
            .bind(SORT_KEY)
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
        let query = sqlx::query(
            "
        SELECT
            store_block.key, store_block.cid, store_block.root, store_block.bytes
        FROM (
            SELECT
                key
            FROM store_key
            WHERE
                sort_key = ?
                AND key > ? AND key < ?
                AND value_retrieved = true
            ORDER BY
                key ASC
            LIMIT
                ?
            OFFSET
                ?
        ) key
        JOIN
            store_block
        ON
            key.key = store_block.key
        ;",
        );
        let all_blocks = query
            .bind(SORT_KEY)
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .bind(limit as i64)
            .bind(offset as i64)
            .fetch_all(self.pool.reader())
            .await?;

        // Consume all block into groups of blocks by their key.
        let all_blocks: Vec<(Self::Key, Vec<BlockRow>)> = process_results(
            all_blocks.into_iter().map(|row| {
                Cid::read_bytes(row.get::<&[u8], _>(1))
                    .map_err(anyhow::Error::from)
                    .map(|cid| {
                        (
                            Self::Key::from(row.get::<Vec<u8>, _>(0)),
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

        let mut values: Vec<(Self::Key, Vec<u8>)> = Vec::new();
        for (key, blocks) in all_blocks {
            if let Some(value) = self.rebuild_car(blocks).await? {
                values.push((key.clone(), value));
            }
        }
        Ok(Box::new(values.into_iter()))
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
            store_key
        WHERE
            sort_key = ? AND
            key > ? AND key < ?
        ;",
        );
        let row = query
            .bind(SORT_KEY)
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
        store_key
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
            .bind(SORT_KEY)
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
            store_key
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
            .bind(SORT_KEY)
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
                FROM store_key
                WHERE
                            sort_key = ? AND
                            key > ? AND key < ?
                ORDER BY key ASC
                LIMIT 1
            ) as first
        JOIN
            (
                SELECT key
                FROM store_key
                WHERE
                            sort_key = ? AND
                            key > ? AND key < ?
                ORDER BY key DESC
                LIMIT 1
            ) as last
        ;",
        );
        let rows = query
            .bind(SORT_KEY)
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .bind(SORT_KEY)
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
        let query = sqlx::query(
            "
            SELECT
                cid, root, bytes
            FROM store_block
            WHERE
                sort_key=?
                AND key=?
            ORDER BY idx
            ;",
        );
        let blocks = query
            .bind(SORT_KEY)
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
            FROM store_key
            WHERE
                sort_key=?
                AND key > ?
                AND key < ?
                AND value_retrieved = false
            ;",
        );
        let row = query
            .bind(SORT_KEY)
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
impl iroh_bitswap::Store for Store {
    async fn get_size(&self, cid: &Cid) -> Result<usize> {
        Ok(
            sqlx::query("SELECT length(bytes) FROM store_block WHERE cid = ?;")
                .bind(cid.to_bytes())
                .fetch_one(self.pool.reader())
                .await?
                .get::<'_, i64, _>(0) as usize,
        )
    }

    async fn get(&self, cid: &Cid) -> Result<Block> {
        Ok(Block::new(
            sqlx::query("SELECT bytes FROM store_block WHERE cid = ?;")
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
            sqlx::query("SELECT count(1) FROM store_block WHERE cid = ?;")
                .bind(cid.to_bytes())
                .fetch_one(self.pool.reader())
                .await?
                .get::<'_, i64, _>(0)
                > 0,
        )
    }
}
