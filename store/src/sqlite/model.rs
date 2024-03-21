use std::{collections::BTreeSet, marker::PhantomData};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use ceramic_core::{EventId, RangeOpen};
use cid::Cid;
use futures::stream::BoxStream;
use iroh_bitswap::Block;
use iroh_car::{CarHeader, CarReader, CarWriter};
use itertools::{process_results, Itertools};
use multihash::{Code, Multihash, MultihashDigest};
use recon::{AssociativeHash, HashCount, InsertResult, Key, ReconItem, Sha256a};
use sqlx::{sqlite::SqliteRow, FromRow, Row};
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
/// A CID identified block of (ipfs) data
pub struct BlockRow {
    cid: Cid,
    root: bool,
    bytes: Vec<u8>,
}

// unfortunately, query! macros require the exact field names, not the FromRow implementation so we need to impl
// the decode/type stuff for CID/EventId to get that to work as expected
impl FromRow<'_, SqliteRow> for BlockRow {
    fn from_row(row: &SqliteRow) -> sqlx::Result<Self> {
        let cid: &[u8] = row.try_get("cid")?;
        let root: bool = row.try_get("root")?;
        let bytes = row.try_get("bytes")?;
        let cid = Cid::read_bytes(cid).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        Ok(Self { cid, root, bytes })
    }
}

type EventIdError = <EventId as TryFrom<Vec<u8>>>::Error;

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
        Ok(store)
    }

    /// Begin a database transaction.
    pub async fn begin_tx(&self) -> Result<DbTx<'_>> {
        self.pool.tx().await
    }

    /// Commit the database transaction.
    pub async fn commit_tx(&self, tx: DbTx<'_>) -> Result<()> {
        Ok(tx.commit().await?)
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
        // hit temporary dropped while in use errors even with the copy types
        // see https://github.com/launchbadge/sqlx/issues/1151
        let lfp = left_fencepost.as_bytes();
        let rfp = right_fencepost.as_bytes();
        let offset = offset as i64;
        let limit = limit as i64;

        let all_blocks = sqlx::query!(
            "SELECT
                event_block.id, event_block.cid, event_block.root, event_block.idx, block.bytes
            FROM (
                SELECT
                    id
                FROM event
                WHERE
                    id > ? AND id < ?
                    AND value_retrieved = true
                ORDER BY
                    id ASC
                LIMIT
                    ?
                OFFSET
                    ?
            ) key
            JOIN
                event_block ON key.id = event_block.id
            JOIN block on block.cid = event_block.cid
                ORDER BY event_block.id, event_block.idx
            ;",
            lfp,
            rfp,
            limit,
            offset
        )
        .fetch_all(self.pool.reader())
        .await?;

        // // Consume all block into groups of blocks by their key.
        let all_blocks: Vec<(EventId, Vec<BlockRow>)> = process_results(
            all_blocks.into_iter().map(
                |row| -> Result<(EventId, cid::CidGeneric<64>, bool, Vec<u8>), anyhow::Error> {
                    let event_id = EventId::try_from(row.id)?;
                    let cid = Cid::read_bytes(&row.cid[..])?;
                    Ok((event_id, cid, row.root, row.bytes))
                },
            ),
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
        let query = sqlx::query_as::<_, BlockRow>(
            "
            SELECT
                eb.cid, eb.root, b.bytes
            FROM event_block eb join block b on eb.cid = b.cid
            WHERE
                eb.id=?
            ORDER BY eb.idx
            ;",
        );

        let blocks = query
            .bind(key.as_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        self.rebuild_car(blocks).await
    }

    /// returns (new_key, new_val) tuple
    async fn insert_item_int(
        &self,
        item: &ReconItem<'_, EventId>,
        conn: &mut DbTx<'_>,
    ) -> Result<(bool, bool)> {
        // We make sure the key exists as we require it as an FK to add the event_block record.
        // It'd be nice to UPSERT here so we could set the value_retrieved value for an existing record,
        // but we can't easily determine if the key was new in that case in sqlite (could in postgres).
        // Instead we just run the update statement after if needed to make sure it's all set.
        let new_key = self
            .insert_key_int(item.key, item.value.is_some(), conn)
            .await?;

        if let Some(val) = item.value {
            self.update_value_retrieved_int(item.key, conn).await?;

            // Put each block from the car file. Should we check if value already existed and skip this?
            // It will no-op but will still try to insert the blocks again
            let mut reader = CarReader::new(val).await?;
            let roots: BTreeSet<Cid> = reader.header().roots().iter().cloned().collect();
            let mut idx = 0;
            while let Some((cid, data)) = reader.next_block().await? {
                self.insert_event_block_int(
                    item.key,
                    idx,
                    roots.contains(&cid),
                    cid,
                    &data.into(),
                    conn,
                )
                .await?;
                idx += 1;
            }
        }
        Ok((new_key, item.value.is_some()))
    }

    /// set value_retrieved to true
    async fn update_value_retrieved_int(&self, key: &EventId, conn: &mut DbTx<'_>) -> Result<()> {
        let id = key.as_bytes();
        let _update = sqlx::query!("UPDATE event SET value_retrieved = true WHERE id = ?", id)
            .execute(&mut **conn)
            .await?;
        Ok(())
    }

    /// Add a block, returns true if the block is new
    pub async fn put_block(&self, cid: &Cid, blob: &Bytes) -> Result<bool> {
        let mut tx = self.pool.tx().await?;
        let res = self.put_block_tx(cid, blob, &mut tx).await?;
        tx.commit().await?;
        Ok(res)
    }

    /// Add a block, returns true if the block is new
    pub async fn put_block_tx(&self, cid: &Cid, blob: &Bytes, conn: &mut DbTx<'_>) -> Result<bool> {
        let cid = cid.to_bytes();
        let blob = blob.to_vec();
        let resp = sqlx::query!("INSERT INTO block (cid, bytes) VALUES (?, ?);", cid, blob,)
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

    // store a block in the db.
    async fn insert_event_block_int(
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

        let _new = self.put_block_tx(&cid, blob, conn).await?;

        let cid = cid.to_bytes();
        let id = key.as_bytes();
        sqlx::query!(
            "INSERT INTO event_block (id, idx, root, cid) VALUES (?, ?, ?, ?) on conflict do nothing;",
            id,
            idx,
            root,
            cid
        )
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
        let id = key.as_bytes();
        let cid = key.cid().map(|cid| cid.to_bytes());
        let hash = Sha256a::digest(key);

        let resp = sqlx::query!(
            "INSERT INTO event (
                    id, cid,
                    ahash_0, ahash_1, ahash_2, ahash_3,
                    ahash_4, ahash_5, ahash_6, ahash_7,
                    value_retrieved
                ) VALUES (
                    ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?
                );",
            id,
            cid,
            hash.as_u32s()[0],
            hash.as_u32s()[1],
            hash.as_u32s()[2],
            hash.as_u32s()[3],
            hash.as_u32s()[4],
            hash.as_u32s()[5],
            hash.as_u32s()[6],
            hash.as_u32s()[7],
            has_value
        )
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
        struct Highwater {
            id: Vec<u8>,
            new_highwater_mark: Option<i64>,
        }
        // unable to get query! to coerce and keep getting `unsupported type NULL of column #2 ("new_highwater_mark")`
        let rows = sqlx::query_as!(
            Highwater,
            r#"WITH entries AS (
                    SELECT id, MAX(rowid) as max_rowid
                FROM event_block
                    WHERE rowid >= ? -- we return rowid+1 so we must match it next search
                GROUP BY id
                ORDER BY rowid
                LIMIT ?
            )
            SELECT 
                id, 
                (SELECT MAX(max_rowid) + 1 FROM entries) as "new_highwater_mark: _"
            from entries;"#,
            row_id,
            limit
        )
        .fetch_all(self.pool.reader())
        .await?;

        // every row has the same new_highwater_mark value
        let row_id: i64 = rows
            .first()
            .and_then(|r| r.new_highwater_mark)
            .unwrap_or(row_id);
        let rows = rows
            .into_iter()
            .map(|row| EventId::try_from(row.id))
            .collect::<Result<Vec<EventId>, EventIdError>>()?;

        Ok((row_id, rows))
    }

    /// Return a range of block hashes starting at hash exclusively.
    pub async fn block_range(
        &self,
        hash: Option<Multihash>,
        limit: i64,
    ) -> Result<(Vec<Multihash>, i64)> {
        let (hashes_query, remaining_query) = if let Some(hash) = hash {
            (
                sqlx::query("SELECT cid FROM block WHERE cid > ? ORDER BY cid LIMIT ?;")
                    .bind(hash.to_bytes())
                    .bind(limit),
                sqlx::query("SELECT count(cid) FROM block WHERE cid > ?").bind(hash.to_bytes()),
            )
        } else {
            (
                sqlx::query("SELECT DISTINCT cid FROM event_block ORDER BY cid LIMIT ?;")
                    .bind(limit),
                sqlx::query("SELECT count(DISTINCT cid) FROM blocks;"),
            )
        };
        let hashes = hashes_query
            .fetch_all(self.pool.reader())
            .await?
            .into_iter()
            .map(|row| Multihash::from_bytes(row.get::<'_, &[u8], _>(0)))
            .collect::<Result<Vec<Multihash>, multihash::Error>>()?;
        let remaining = remaining_query
                .fetch_one(self.pool.reader())
                .await?
                .get::<'_, i64, _>(0)
                // Do not count the hashes we just got in the remaining count.
                - (hashes.len() as i64);
        Ok((hashes, remaining))
    }

    /// Return rows in the blocks table as a stream to iterate.
    pub fn scan_blocks(&self) -> BoxStream<Result<BlockRow, sqlx::Error>> {
        // TODO: should this include idx (all event block info) or just be cid/bytes?
        sqlx::query_as(
            "SELECT cid, idx, root, bytes
        FROM event_block
        GROUP BY cid
        ORDER BY (key, cid, idx);",
        )
        .fetch(self.pool.reader())
    }

    /// merge_from_sqlite takes the filepath to a sqlite file.
    /// If the file dose not exist the ATTACH DATABASE command will create it.
    /// This function assumes that the database contains a table named blocks with cid, bytes columns.
    pub async fn merge_from_sqlite(&self, input_ceramic_db_filename: &str) -> Result<()> {
        sqlx::query(
            "
                    ATTACH DATABASE ? AS other;
                    INSERT OR IGNORE INTO blocks SELECT cid, bytes FROM other.blocks;
                ",
        )
        .bind(input_ceramic_db_filename)
        .execute(self.pool.writer())
        .await?;
        Ok(())
    }

    /// Backup the database to a filepath output_ceramic_db_filename.
    pub async fn backup_to_sqlite(&self, output_ceramic_db_filename: &str) -> Result<()> {
        sqlx::query(".backup ?")
            .bind(output_ceramic_db_filename)
            .execute(self.pool.writer())
            .await?;
        Ok(())
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
             FROM event WHERE id > ? AND id < ?;",
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
            id
        FROM
            event
        WHERE
            id > ? AND id < ?
        ORDER BY
            id ASC
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
        let rows = rows
            .into_iter()
            .map(|row| {
                let bytes: Vec<u8> = row.get(0);
                EventId::try_from(bytes)
            })
            .collect::<Result<Vec<Self::Key>, EventIdError>>()?;
        Ok(Box::new(rows.into_iter()))
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
        let lfp = left_fencepost.as_bytes();
        let rpf = right_fencepost.as_bytes();
        let row = sqlx::query!(
            "
        SELECT
            count(id) as cnt
        FROM
            event
        WHERE
            id > ? AND id < ?
        ;",
            lfp,
            rpf
        )
        .fetch_one(self.pool.reader())
        .await?;
        Ok(row.cnt as usize)
    }

    /// Return the first key within the range.
    #[instrument(skip(self))]
    async fn first(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        let lfp = left_fencepost.as_bytes();
        let rpf = right_fencepost.as_bytes();
        let row = sqlx::query!(
            "SELECT
                id
            FROM
                event
            WHERE
                id > ? AND id < ?
            ORDER BY
                id ASC
            LIMIT
            1",
            lfp,
            rpf
        )
        .fetch_optional(self.pool.reader())
        .await?;
        let res = row.map(|row| EventId::try_from(row.id)).transpose()?;
        Ok(res)
    }

    #[instrument(skip(self))]
    async fn last(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        let lfp = left_fencepost.as_bytes();
        let rpf = right_fencepost.as_bytes();
        let row = sqlx::query!(
            "
        SELECT
            id
        FROM
            event
        WHERE
            id > ? AND id < ?
        ORDER BY
            id DESC
        LIMIT
            1
        ;",
            lfp,
            rpf
        )
        .fetch_optional(self.pool.reader())
        .await?;
        let res = row.map(|row| EventId::try_from(row.id)).transpose()?;
        Ok(res)
    }

    #[instrument(skip(self))]
    async fn first_and_last(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<(Self::Key, Self::Key)>> {
        let lfp = left_fencepost.as_bytes();
        let rfp = right_fencepost.as_bytes();
        let row = sqlx::query!(
            "SELECT first.id as first, last.id as last
                FROM
                    (
                        SELECT id
                        FROM event
                        WHERE
                            id > ? AND id < ?
                        ORDER BY id ASC
                        LIMIT 1
                    ) as first
                JOIN
                    (
                        SELECT id
                        FROM event
                        WHERE
                            id > ? AND id < ?
                        ORDER BY id DESC
                        LIMIT 1
                    ) as last;",
            lfp,
            rfp,
            lfp,
            rfp,
        )
        .fetch_optional(self.pool.reader())
        .await?;
        if let Some(row) = row {
            let first = EventId::try_from(row.first)?;
            let last = EventId::try_from(row.last)?;
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
        let start = range.start.as_bytes();
        let end = range.end.as_bytes();
        let row = sqlx::query!(
            "
            SELECT id
            FROM event
            WHERE
                id > ?
                AND id < ?
                AND value_retrieved = false
            ;",
            start,
            end
        )
        .fetch_all(self.pool.reader())
        .await?;
        Ok(row
            .into_iter()
            .map(|row| EventId::try_from(row.id))
            .collect::<Result<Vec<Self::Key>, EventIdError>>()?)
    }
}

#[async_trait]
impl iroh_bitswap::Store for ModelStore<Sha256a> {
    async fn get_size(&self, cid: &Cid) -> Result<usize> {
        Ok(
            sqlx::query("SELECT length(bytes) FROM block WHERE cid = ?;")
                .bind(cid.to_bytes())
                .fetch_one(self.pool.reader())
                .await?
                .get::<'_, i64, _>(0) as usize,
        )
    }

    async fn get(&self, cid: &Cid) -> Result<Block> {
        Ok(Block::new(
            sqlx::query("SELECT bytes FROM block WHERE cid = ?;")
                .bind(cid.to_bytes())
                .fetch_one(self.pool.reader())
                .await?
                .get::<'_, Vec<u8>, _>(0)
                .into(),
            cid.to_owned(),
        ))
    }

    async fn has(&self, cid: &Cid) -> Result<bool> {
        Ok(sqlx::query("SELECT count(1) FROM block WHERE cid = ?;")
            .bind(cid.to_bytes())
            .fetch_one(self.pool.reader())
            .await?
            .get::<'_, i64, _>(0)
            > 0)
    }
}

/// We intentionally expose the store to the API, separately from the recon::Store trait.
/// This allows us better control over the API functionality, particularly CRUD, that are related
/// to recon, but not explicitly part of the recon protocol. Eventually, it might be nice to reduce the
/// scope of the recon::Store trait (or remove the &mut self requirement), but for now we have both.
/// Anything that implements `ceramic_api::AccessModelStore` should also implement `recon::Store`.
/// This guarantees that regardless of entry point (api or recon), the data is stored and retrieved in the same way.
#[async_trait::async_trait]
impl ceramic_api::AccessModelStore for ModelStore<Sha256a> {
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

    use std::str::FromStr;

    use anyhow::Error;
    use bytes::Bytes;
    use cid::{Cid, CidGeneric};
    use expect_test::expect;
    use iroh_bitswap::Store;
    use recon::{Key, ReconItem};
    use test_log::test;

    use super::*;
    use crate::tests::*;

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
                    bytes: "ce010502b51217a029eb540d4f16d8429ae87f86ead3ca3c01010012204739d813c902e3011034902f4ca39146c88163cde9d94fe73d3332f2d03f3dd7",
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
                    bytes: "ce010502b51217a029eb540d4f16d8429ae87f86ead3ca3c02010012202c22bf5e2d32a77fd71cc93baa3b9c56f3fce454c1ef4f95100febddf31f309e",
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

        // the second insert of same key with value reports it already exists.
        // Do not override values
        expect![[r#"
            Ok(
                false,
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
                        bytes: "ce010502b51217a029eb540d4f16d8429ae87f86ead3ca3c0a010012209a089111fffeecffee792270263fea656ea7210c007fcde556b8fab9b96f8005",
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
                        bytes: "ce010502b51217a029eb540d4f16d8429ae87f86ead3ca3c0a010012209a089111fffeecffee792270263fea656ea7210c007fcde556b8fab9b96f8005",
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
                        bytes: "ce010502b51217a029eb540d4f16d8429ae87f86ead3ca3c0a010012209a089111fffeecffee792270263fea656ea7210c007fcde556b8fab9b96f8005",
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
                        bytes: "ce010502b51217a029eb540d4f16d8429ae87f86ead3ca3c0b010012200d2ceb194fa14839ad0dc3f50bcd3d9655917f45f13500db4af859693ef89ba1",
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
                    bytes: "ce010502b51217a029eb540d4f16d8429ae87f86ead3ca3c0401001220c2e9076a8b9fa2fc122df756d6618d0f8a3a7b78bbe7e5ea478f1e6c223e300d",
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
        assert_eq!(22, hw);
        assert_eq!([key_c], res.as_slice());

        let (hw, res) = store.new_keys_since_value_rowid(hw, 1).await.unwrap();
        assert_eq!(0, res.len());
        assert_eq!(22, hw); // previously returned 0
    }

    #[tokio::test]
    async fn test_store_block() {
        let blob: Bytes = hex::decode("0a050001020304").unwrap().into();
        let cid: CidGeneric<64> =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line

        let pool = SqlitePool::connect("sqlite::memory:", true).await.unwrap();
        let store = ModelStore::<Sha256a>::new(pool).await.unwrap();

        let result = store.put_block(&cid, &blob).await.unwrap();
        // assert the block is new
        assert!(result);

        let has: Result<bool, Error> = Store::has(&store, &cid).await;
        expect![["true"]].assert_eq(&has.unwrap().to_string());

        let size: Result<usize, Error> = Store::get_size(&store, &cid).await;
        expect![["7"]].assert_eq(&size.unwrap().to_string());

        let block = Store::get(&store, &cid).await.unwrap();
        expect!["bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom"] // cspell:disable-line
            .assert_eq(&block.cid().to_string());
        expect![["0A050001020304"]].assert_eq(&hex::encode_upper(block.data()));
    }

    #[tokio::test]
    async fn test_double_store_block() {
        let blob: Bytes = hex::decode("0a050001020304").unwrap().into();
        let cid: CidGeneric<64> =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line

        let pool = SqlitePool::connect("sqlite::memory:", true).await.unwrap();
        let store = ModelStore::<Sha256a>::new(pool).await.unwrap();

        let result = store.put_block(&cid, &blob).await;
        // Assert that the block is new
        assert!(result.unwrap());

        // Try to put the block again
        let result = store.put_block(&cid, &blob).await;
        // Assert that the block already existed
        assert!(!result.unwrap());

        let has: Result<bool, Error> = Store::has(&store, &cid).await;
        expect![["true"]].assert_eq(&has.unwrap().to_string());

        let size: Result<usize, Error> = Store::get_size(&store, &cid).await;
        expect![["7"]].assert_eq(&size.unwrap().to_string());

        let block = Store::get(&store, &cid).await.unwrap();
        expect!["bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom"] // cspell:disable-line
            .assert_eq(&block.cid().to_string());
        expect![["0A050001020304"]].assert_eq(&hex::encode_upper(block.data()));
    }

    #[tokio::test]
    async fn test_get_nonexistent_block() {
        let pool = SqlitePool::connect("sqlite::memory:", true).await.unwrap();
        let store = ModelStore::<Sha256a>::new(pool).await.unwrap();

        let cid =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line

        let err = store.get(&cid).await.unwrap_err().to_string();
        assert!(
            err.contains("no rows returned by a query that expected to return at least one row"),
            "{}",
            err
        );
    }
}
