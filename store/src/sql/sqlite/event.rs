use std::{
    collections::BTreeSet,
    marker::PhantomData,
    sync::{atomic::AtomicI64, Arc},
};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use ceramic_core::{EventId, RangeOpen};

use cid::Cid;
use iroh_bitswap::Block;
use iroh_car::CarReader;
use multihash::{Code, Multihash, MultihashDigest};
use recon::{AssociativeHash, HashCount, InsertResult, Key, ReconItem, Sha256a};

use tracing::instrument;

use crate::{
    sql::{
        rebuild_car, BlockBytes, BlockQuery, BlockRow, CountRow, DeliveredEvent, EventIdError,
        EventQuery, EventValueRaw, FirstAndLast, OrderKey, ReconHash, ReconQuery, ReconType,
        SqlBackend, GLOBAL_COUNTER,
    },
    DbTxSqlite, SqlitePool,
};

/// Unified implementation of [`recon::Store`] and [`iroh_bitswap::Store`] that can expose the
/// individual blocks from the CAR files directly.
#[derive(Clone, Debug)]
pub struct EventStoreSqlite<H>
where
    H: AssociativeHash,
{
    pub(crate) pool: SqlitePool,
    test_counter: Option<Arc<AtomicI64>>,
    hash: PhantomData<H>,
}

impl<H> EventStoreSqlite<H>
where
    H: AssociativeHash + std::convert::From<[u32; 8]>,
{
    /// Create an instance of the store initializing any neccessary tables.
    pub async fn new(pool: SqlitePool) -> Result<Self> {
        let store = EventStoreSqlite {
            pool,
            test_counter: None,
            hash: PhantomData,
        };
        store.init_delivered().await?;
        Ok(store)
    }

    /// Creates an instance that doesn't share the global event counter with other instances (only for tests to avoid running serially)
    #[allow(dead_code)]
    pub(crate) async fn new_local(pool: SqlitePool) -> Result<Self> {
        let store = EventStoreSqlite {
            pool,
            test_counter: Some(Arc::new(AtomicI64::new(0))),
            hash: PhantomData,
        };
        store.init_delivered().await?;
        Ok(store)
    }

    async fn init_delivered(&self) -> Result<()> {
        let max_delivered: CountRow = sqlx::query_as(EventQuery::max_delivered())
            .fetch_one(self.pool.reader())
            .await?;
        let max = max_delivered
            .res
            .checked_add(1)
            .context("More than i64::MAX delivered events!")?;
        if let Some(ref t) = self.test_counter {
            t.fetch_max(max, std::sync::atomic::Ordering::SeqCst);
        } else {
            GLOBAL_COUNTER.fetch_max(max, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(())
    }

    // could change this to rely on time similar to a snowflake ID
    fn get_delivered(&self) -> i64 {
        if let Some(ref t) = self.test_counter {
            t.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        } else {
            GLOBAL_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// Begin a database transaction.
    pub async fn begin_tx(&self) -> Result<DbTxSqlite<'_>> {
        self.pool.tx().await
    }

    /// Commit the database transaction.
    pub async fn commit_tx(&self, tx: DbTxSqlite<'_>) -> Result<()> {
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
        let offset = offset.try_into().unwrap_or(i64::MAX);
        let limit: i64 = limit.try_into().unwrap_or(i64::MAX);
        let all_blocks: Vec<EventValueRaw> = sqlx::query_as(EventQuery::value_blocks_many())
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .bind(limit)
            .bind(offset)
            .fetch_all(self.pool.reader())
            .await?;

        let values = EventValueRaw::into_carfiles(all_blocks).await?;
        Ok(Box::new(values.into_iter()))
    }

    async fn keys_with_missing_values_int(
        &self,
        range: RangeOpen<EventId>,
    ) -> Result<Vec<EventId>> {
        if range.start >= range.end {
            return Ok(vec![]);
        };
        let start = range.start.as_bytes();
        let end = range.end.as_bytes();
        let row: Vec<OrderKey> = sqlx::query_as(EventQuery::missing_values())
            .bind(start)
            .bind(end)
            .fetch_all(self.pool.reader())
            .await?;

        Ok(row
            .into_iter()
            .map(|row| EventId::try_from(row.order_key))
            .collect::<Result<Vec<EventId>, EventIdError>>()?)
    }

    async fn value_for_key_int(&self, key: &EventId) -> Result<Option<Vec<u8>>> {
        let blocks: Vec<BlockRow> = sqlx::query_as(EventQuery::value_blocks_one())
            .bind(key.as_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        rebuild_car(blocks).await
    }

    /// returns (new_key, new_val) tuple
    async fn insert_item_int(
        &self,
        item: &ReconItem<'_, EventId>,
        conn: &mut DbTxSqlite<'_>,
    ) -> Result<(bool, bool)> {
        // We make sure the key exists as we require it as an FK to add the event_block record.
        let new_key = self.insert_key_int(item.key, conn).await?;

        if let Some(val) = item.value {
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
            self.mark_ready_to_deliver(item.key, conn).await?;
        }
        Ok((new_key, item.value.is_some()))
    }

    /// Add a block, returns true if the block is new
    pub async fn put_block(&self, hash: &Multihash, blob: &Bytes) -> Result<bool> {
        let mut tx = self.pool.tx().await?;
        let res = self.put_block_tx(hash, blob, &mut tx).await?;
        tx.commit().await?;
        Ok(res)
    }

    /// Add a block, returns true if the block is new
    pub async fn put_block_tx(
        &self,
        hash: &Multihash,
        blob: &Bytes,
        conn: &mut DbTxSqlite<'_>,
    ) -> Result<bool> {
        let resp = sqlx::query("INSERT INTO ceramic_one_block (multihash, bytes) VALUES ($1, $2);")
            .bind(hash.to_bytes())
            .bind(blob.to_vec())
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
        conn: &mut DbTxSqlite<'_>,
    ) -> Result<()> {
        let hash = match cid.hash().code() {
            0x12 => Code::Sha2_256.digest(blob),
            0x1b => Code::Keccak256.digest(blob),
            0x11 => return Err(anyhow!("Sha1 not supported")),
            code => {
                return Err(anyhow!(
                    "multihash type {:#x} not Sha2_256, Keccak256",
                    code,
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

        let _new = self.put_block_tx(&hash, blob, conn).await?;

        let code: i64 = cid.codec().try_into().context(format!(
            "Invalid codec could not fit into an i64: {}",
            cid.codec()
        ))?;
        let id = key.cid().context("Event CID is required")?.to_bytes();
        let multihash = hash.to_bytes();
        sqlx::query(
            "INSERT INTO ceramic_one_event_block (event_cid, idx, root, block_multihash, codec) VALUES ($1, $2, $3, $4, $5) on conflict do nothing;")
            .bind(id)
            .bind(idx)
            .bind(root)
            .bind(multihash)
            .bind(code)
        .execute(&mut **conn)
        .await?;
        Ok(())
    }

    async fn mark_ready_to_deliver(&self, key: &EventId, conn: &mut DbTxSqlite<'_>) -> Result<()> {
        let id = key.as_bytes();
        let delivered = self.get_delivered();
        sqlx::query("UPDATE ceramic_one_event SET delivered = $1 WHERE order_key = $2;")
            .bind(delivered)
            .bind(id)
            .execute(&mut **conn)
            .await?;

        Ok(())
    }

    async fn insert_key_int(&self, key: &EventId, conn: &mut DbTxSqlite<'_>) -> Result<bool> {
        let id = key.as_bytes();
        let cid = key
            .cid()
            .map(|cid| cid.to_bytes())
            .context("Event CID is required")?;
        let hash = Sha256a::digest(key);

        let resp = sqlx::query(ReconQuery::insert_event())
            .bind(id)
            .bind(cid)
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

    /// Returns all the keys found after the given delivered value.
    pub async fn new_keys_since_value(
        &self,
        delivered: i64,
        limit: i64,
    ) -> Result<(i64, Vec<EventId>)> {
        let rows: Vec<DeliveredEvent> = sqlx::query_as(EventQuery::new_delivered_events())
            .bind(delivered)
            .bind(limit)
            .fetch_all(self.pool.reader())
            .await?;

        DeliveredEvent::parse_query_results(delivered, rows)
    }

    /// merge_from_sqlite takes the filepath to a sqlite file.
    /// If the file dose not exist the ATTACH DATABASE command will create it.
    /// This function assumes that the database contains a table named blocks with cid, bytes columns.
    pub async fn merge_from_sqlite(&self, input_ceramic_db_filename: &str) -> Result<()> {
        sqlx::query(
            "
                    ATTACH DATABASE $1 AS other;
                    INSERT OR IGNORE INTO ceramic_one_block SELECT multihash, bytes FROM other.ceramic_one_block;
                ",
        )
        .bind(input_ceramic_db_filename)
        .execute(self.pool.writer())
        .await?;
        Ok(())
    }

    /// Backup the database to a filepath output_ceramic_db_filename.
    pub async fn backup_to_sqlite(&self, output_ceramic_db_filename: &str) -> Result<()> {
        sqlx::query(".backup $1")
            .bind(output_ceramic_db_filename)
            .execute(self.pool.writer())
            .await?;
        Ok(())
    }
}

#[async_trait]
impl<H> recon::Store for EventStoreSqlite<H>
where
    H: AssociativeHash,
{
    type Key = EventId;
    type Hash = H;

    /// Returns true if the key was new. The value is always updated if included
    async fn insert(&self, item: ReconItem<'_, Self::Key>) -> Result<bool> {
        let (new, _new_val) = self.insert_item(&item).await?;
        Ok(new)
    }

    /// Insert new keys into the key space.
    /// Returns true if a key did not previously exist.
    async fn insert_many<'a, I>(&self, items: I) -> Result<InsertResult>
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
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<HashCount<Self::Hash>> {
        let row: ReconHash =
            sqlx::query_as(ReconQuery::hash_range(ReconType::Event, SqlBackend::Sqlite))
                .bind(left_fencepost.as_bytes())
                .bind(right_fencepost.as_bytes())
                .fetch_one(self.pool.reader())
                .await?;
        Ok(HashCount::new(H::from(row.hash()), row.count()))
    }

    #[instrument(skip(self))]
    async fn range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        let offset: i64 = offset
            .try_into()
            .context("Offset too large to fit into i64")?;
        let limit = limit.try_into().unwrap_or(100000); // 100k is still a huge limit
        let rows: Vec<OrderKey> = sqlx::query_as(ReconQuery::range(ReconType::Event))
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .bind(limit)
            .bind(offset)
            .fetch_all(self.pool.reader())
            .await?;
        let rows = rows
            .into_iter()
            .map(EventId::try_from)
            .collect::<Result<Vec<Self::Key>, EventIdError>>()?;
        Ok(Box::new(rows.into_iter()))
    }
    #[instrument(skip(self))]
    async fn range_with_values(
        &self,
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
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<usize> {
        let row: CountRow = sqlx::query_as(ReconQuery::count(ReconType::Event, SqlBackend::Sqlite))
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_one(self.pool.reader())
            .await?;
        Ok(row.res as usize)
    }

    /// Return the first key within the range.
    #[instrument(skip(self))]
    async fn first(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        let row: Option<OrderKey> = sqlx::query_as(ReconQuery::first_key(ReconType::Event))
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_optional(self.pool.reader())
            .await?;
        let res = row.map(EventId::try_from).transpose()?;
        Ok(res)
    }

    #[instrument(skip(self))]
    async fn last(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        let row: Option<OrderKey> = sqlx::query_as(ReconQuery::last_key(ReconType::Event))
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_optional(self.pool.reader())
            .await?;
        let res = row.map(EventId::try_from).transpose()?;
        Ok(res)
    }

    #[instrument(skip(self))]
    async fn first_and_last(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<(Self::Key, Self::Key)>> {
        let row: Option<FirstAndLast> = sqlx::query_as(ReconQuery::first_and_last(
            ReconType::Event,
            SqlBackend::Sqlite,
        ))
        .bind(left_fencepost.as_bytes())
        .bind(right_fencepost.as_bytes())
        .bind(left_fencepost.as_bytes())
        .bind(right_fencepost.as_bytes())
        .fetch_optional(self.pool.reader())
        .await?;

        if let Some(row) = row {
            let first = EventId::try_from(row.first_key)?;
            let last = EventId::try_from(row.last_key)?;
            Ok(Some((first, last)))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self))]
    async fn value_for_key(&self, key: &Self::Key) -> Result<Option<Vec<u8>>> {
        self.value_for_key_int(key).await
    }

    #[instrument(skip(self))]
    async fn keys_with_missing_values(
        &self,
        range: RangeOpen<Self::Key>,
    ) -> Result<Vec<Self::Key>> {
        self.keys_with_missing_values_int(range).await
    }
}

#[async_trait]
impl iroh_bitswap::Store for EventStoreSqlite<Sha256a> {
    async fn get_size(&self, cid: &Cid) -> Result<usize> {
        let len: CountRow = sqlx::query_as(BlockQuery::length())
            .bind(cid.hash().to_bytes())
            .fetch_one(self.pool.reader())
            .await?;
        Ok(len.res as usize)
    }

    async fn get(&self, cid: &Cid) -> Result<Block> {
        let block: BlockBytes = sqlx::query_as(BlockQuery::get())
            .bind(cid.hash().to_bytes())
            .fetch_one(self.pool.reader())
            .await?;
        Ok(Block::new(block.bytes.into(), cid.to_owned()))
    }

    async fn has(&self, cid: &Cid) -> Result<bool> {
        let len: CountRow = sqlx::query_as(BlockQuery::has())
            .bind(cid.hash().to_bytes())
            .fetch_one(self.pool.reader())
            .await?;
        Ok(len.res > 0)
    }

    async fn put(&self, block: &Block) -> Result<bool> {
        Ok(self.put_block(block.cid().hash(), block.data()).await?)
    }
}

/// We intentionally expose the store to the API, separately from the recon::Store trait.
/// This allows us better control over the API functionality, particularly CRUD, that are related
/// to recon, but not explicitly part of the recon protocol. Eventually, it might be nice to reduce the
/// scope of the recon::Store trait (or remove the &self requirement), but for now we have both.
/// Anything that implements `ceramic_api::AccessModelStore` should also implement `recon::Store`.
/// This guarantees that regardless of entry point (api or recon), the data is stored and retrieved in the same way.
#[async_trait::async_trait]
impl ceramic_api::AccessModelStore for EventStoreSqlite<Sha256a> {
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
        self.new_keys_since_value(highwater, limit).await
    }
}
