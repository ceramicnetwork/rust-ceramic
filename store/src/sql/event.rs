use std::num::TryFromIntError;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use ceramic_core::{event_id::InvalidEventId, EventId, RangeOpen};

use cid::Cid;
use iroh_bitswap::Block;
use multihash_codetable::Multihash;
use recon::{
    AssociativeHash, Error as ReconError, HashCount, InsertResult, Key, ReconItem,
    Result as ReconResult, Sha256a,
};

use tracing::instrument;

use crate::{
    sql::{
        rebuild_car, BlockBytes, BlockQuery, BlockRow, CountRow, DeliveredEvent, EventBlockQuery,
        EventBlockRaw, EventQuery, EventRaw, FirstAndLast, OrderKey, ReconHash, ReconQuery,
        ReconType, SqlBackend, GLOBAL_COUNTER,
    },
    DbTxSqlite, Error, Result, SqlitePool,
};

/// Unified implementation of [`recon::Store`] and [`iroh_bitswap::Store`] that can expose the
/// individual blocks from the CAR files directly.
#[derive(Clone, Debug)]
pub struct SqliteEventStore {
    pub(crate) pool: SqlitePool,
}

impl SqliteEventStore {
    /// Create an instance of the store initializing any neccessary tables.
    pub async fn new(pool: SqlitePool) -> Result<Self> {
        let store = SqliteEventStore { pool };
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
            .ok_or_else(|| Error::new_fatal(anyhow!("More than i64::MAX delivered events!")))?;
        GLOBAL_COUNTER.fetch_max(max, std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }

    // could change this to rely on time similar to a snowflake ID
    fn get_delivered(&self) -> i64 {
        GLOBAL_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Begin a database transaction.
    pub async fn begin_tx(&self) -> Result<DbTxSqlite<'_>> {
        Ok(self.pool.writer().begin().await?)
    }

    /// Commit the database transaction.
    pub async fn commit_tx(&self, tx: DbTxSqlite<'_>) -> Result<()> {
        Ok(tx.commit().await?)
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
        let all_blocks: Vec<EventBlockRaw> = sqlx::query_as(EventQuery::value_blocks_many())
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .bind(limit)
            .bind(offset)
            .fetch_all(self.pool.reader())
            .await?;

        let values = EventBlockRaw::into_carfiles(all_blocks).await?;
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

        let res = row
            .into_iter()
            .map(|row| EventId::try_from(row.order_key).map_err(|e| Error::new_app(anyhow!(e))))
            .collect::<Result<Vec<EventId>>>()?;

        Ok(res)
    }

    async fn value_for_key_int(&self, key: &EventId) -> Result<Option<Vec<u8>>> {
        let blocks: Vec<BlockRow> = sqlx::query_as(EventQuery::value_blocks_one())
            .bind(key.as_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        rebuild_car(blocks).await
    }

    async fn insert_item_int(&self, item: ReconItem<'_, EventId>) -> Result<(bool, bool)> {
        let new_val = item.value.is_some();
        let res = self.insert_items_int(&[item]).await?;
        let new_key = res.keys.first().cloned().unwrap_or(false);
        Ok((new_key, new_val))
    }

    /// Insert many items into the store (internal to the store)
    async fn insert_items_int(&self, items: &[ReconItem<'_, EventId>]) -> Result<InsertResult> {
        if items.is_empty() {
            return Ok(InsertResult::new(vec![], 0));
        }
        let mut to_add = vec![];
        for item in items {
            if let Some(val) = item.value {
                match EventRaw::try_build(item.key.to_owned(), val).await {
                    Ok(parsed) => to_add.push(parsed),
                    Err(error) => {
                        tracing::warn!(%error, order_key=%item.key, "Error parsing event into carfile");
                        continue;
                    }
                }
            } else {
                to_add.push(EventRaw::new(item.key.clone(), vec![]));
            }
        }
        if to_add.is_empty() {
            return Ok(InsertResult::new(vec![], 0));
        }
        let mut new_keys = vec![false; to_add.len()];
        let mut new_val_cnt = 0;
        let mut tx = self.pool.writer().begin().await.map_err(Error::from)?;

        for (idx, item) in to_add.into_iter().enumerate() {
            let new_key = self.insert_key_int(&item.order_key, &mut tx).await?;
            for block in item.blocks.iter() {
                self.insert_event_block_int(block, &mut tx).await?;
                self.mark_ready_to_deliver(&item.order_key, &mut tx).await?;
            }
            new_keys[idx] = new_key;
            if !item.blocks.is_empty() {
                new_val_cnt += 1;
            }
        }
        tx.commit().await.map_err(Error::from)?;
        let res = InsertResult::new(new_keys, new_val_cnt);

        Ok(res)
    }

    /// Add a block, returns true if the block is new
    pub async fn put_block(&self, hash: &Multihash, blob: &Bytes) -> Result<bool> {
        let mut tx = self.pool.writer().begin().await?;
        let res = self.put_block_tx(hash, blob, &mut tx).await?;
        tx.commit().await?;
        Ok(res)
    }

    /// Add a block, returns true if the block is new
    pub async fn put_block_tx(
        &self,
        hash: &Multihash,
        blob: &[u8],
        conn: &mut DbTxSqlite<'_>,
    ) -> Result<bool> {
        let resp = sqlx::query(BlockQuery::put())
            .bind(hash.to_bytes())
            .bind(blob)
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
        ev_block: &EventBlockRaw,
        conn: &mut DbTxSqlite<'_>,
    ) -> Result<()> {
        let _new = self
            .put_block_tx(ev_block.multihash.inner(), &ev_block.bytes, conn)
            .await?;

        sqlx::query(EventBlockQuery::upsert())
            .bind(&ev_block.order_key)
            .bind(ev_block.idx)
            .bind(ev_block.root)
            .bind(ev_block.multihash.to_bytes())
            .bind(ev_block.codec)
            .execute(&mut **conn)
            .await?;
        Ok(())
    }

    async fn mark_ready_to_deliver(&self, key: &EventId, conn: &mut DbTxSqlite<'_>) -> Result<()> {
        let id = key.as_bytes();
        let delivered = self.get_delivered();
        sqlx::query(EventQuery::mark_ready_to_deliver())
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
            .ok_or_else(|| Error::new_app(anyhow!("Event CID is required")))?;
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
impl recon::Store for SqliteEventStore {
    type Key = EventId;
    type Hash = Sha256a;

    /// Returns true if the key was new. The value is always updated if included
    async fn insert(&self, item: &ReconItem<'_, Self::Key>) -> ReconResult<bool> {
        let (res, _new_val) = self.insert_item_int(item.to_owned()).await?;
        Ok(res)
    }

    /// Insert new keys into the key space.
    /// Returns true if a key did not previously exist.
    async fn insert_many(&self, items: &[ReconItem<'_, EventId>]) -> ReconResult<InsertResult> {
        match items.len() {
            0 => Ok(InsertResult::new(vec![], 0)),
            _ => {
                let res = self.insert_items_int(items).await?;
                Ok(res)
            }
        }
    }

    /// return the hash and count for a range
    #[instrument(skip(self))]
    async fn hash_range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> ReconResult<HashCount<Self::Hash>> {
        let row: ReconHash =
            sqlx::query_as(ReconQuery::hash_range(ReconType::Event, SqlBackend::Sqlite))
                .bind(left_fencepost.as_bytes())
                .bind(right_fencepost.as_bytes())
                .fetch_one(self.pool.reader())
                .await
                .map_err(Error::from)?;
        Ok(HashCount::new(Self::Hash::from(row.hash()), row.count()))
    }

    #[instrument(skip(self))]
    async fn range(
        &self,
        left_fencepost: &EventId,
        right_fencepost: &EventId,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = EventId> + Send + 'static>> {
        let offset: i64 = offset.try_into().map_err(|_e: TryFromIntError| {
            ReconError::new_app(anyhow!("Offset too large to fit into i64"))
        })?;
        let limit = limit.try_into().unwrap_or(100000); // 100k is still a huge limit
        let rows: Vec<OrderKey> = sqlx::query_as(ReconQuery::range(ReconType::Event))
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .bind(limit)
            .bind(offset)
            .fetch_all(self.pool.reader())
            .await
            .map_err(Error::from)?;
        let rows = rows
            .into_iter()
            .map(|k| EventId::try_from(k).map_err(|e: InvalidEventId| Error::new_app(anyhow!(e))))
            .collect::<Result<Vec<EventId>>>()?;
        Ok(Box::new(rows.into_iter()))
    }
    #[instrument(skip(self))]
    async fn range_with_values(
        &self,
        left_fencepost: &EventId,
        right_fencepost: &EventId,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = (EventId, Vec<u8>)> + Send + 'static>> {
        Ok(self
            .range_with_values_int(left_fencepost, right_fencepost, offset, limit)
            .await?)
    }

    /// Return the number of keys within the range.
    #[instrument(skip(self))]
    async fn count(
        &self,
        left_fencepost: &EventId,
        right_fencepost: &EventId,
    ) -> ReconResult<usize> {
        let row: CountRow = sqlx::query_as(ReconQuery::count(ReconType::Event, SqlBackend::Sqlite))
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_one(self.pool.reader())
            .await
            .map_err(Error::from)?;
        Ok(row.res as usize)
    }

    /// Return the first key within the range.
    #[instrument(skip(self))]
    async fn first(
        &self,
        left_fencepost: &EventId,
        right_fencepost: &EventId,
    ) -> ReconResult<Option<EventId>> {
        let row: Option<OrderKey> = sqlx::query_as(ReconQuery::first_key(ReconType::Event))
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_optional(self.pool.reader())
            .await
            .map_err(Error::from)?;
        let res = row
            .map(|r| EventId::try_from(r).map_err(ReconError::new_app))
            .transpose()?;
        Ok(res)
    }

    #[instrument(skip(self))]
    async fn last(
        &self,
        left_fencepost: &EventId,
        right_fencepost: &EventId,
    ) -> ReconResult<Option<EventId>> {
        let row: Option<OrderKey> = sqlx::query_as(ReconQuery::last_key(ReconType::Event))
            .bind(left_fencepost.as_bytes())
            .bind(right_fencepost.as_bytes())
            .fetch_optional(self.pool.reader())
            .await
            .map_err(Error::from)?;
        let res = row
            .map(|r| EventId::try_from(r).map_err(ReconError::new_app))
            .transpose()?;
        Ok(res)
    }

    #[instrument(skip(self))]
    async fn first_and_last(
        &self,
        left_fencepost: &EventId,
        right_fencepost: &EventId,
    ) -> ReconResult<Option<(EventId, EventId)>> {
        let row: Option<FirstAndLast> = sqlx::query_as(ReconQuery::first_and_last(
            ReconType::Event,
            SqlBackend::Sqlite,
        ))
        .bind(left_fencepost.as_bytes())
        .bind(right_fencepost.as_bytes())
        .bind(left_fencepost.as_bytes())
        .bind(right_fencepost.as_bytes())
        .fetch_optional(self.pool.reader())
        .await
        .map_err(Error::from)?;

        if let Some(row) = row {
            let first = EventId::try_from(row.first_key).map_err(ReconError::new_app)?;

            let last = EventId::try_from(row.last_key).map_err(ReconError::new_app)?;
            Ok(Some((first, last)))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self))]
    async fn value_for_key(&self, key: &EventId) -> ReconResult<Option<Vec<u8>>> {
        Ok(self.value_for_key_int(key).await?)
    }

    #[instrument(skip(self))]
    async fn keys_with_missing_values(
        &self,
        range: RangeOpen<EventId>,
    ) -> ReconResult<Vec<EventId>> {
        Ok(self.keys_with_missing_values_int(range).await?)
    }
}

#[async_trait]
impl iroh_bitswap::Store for SqliteEventStore {
    async fn get_size(&self, cid: &Cid) -> anyhow::Result<usize> {
        let len: CountRow = sqlx::query_as(BlockQuery::length())
            .bind(cid.hash().to_bytes())
            .fetch_one(self.pool.reader())
            .await?;
        Ok(len.res as usize)
    }

    async fn get(&self, cid: &Cid) -> anyhow::Result<Block> {
        let block: BlockBytes = sqlx::query_as(BlockQuery::get())
            .bind(cid.hash().to_bytes())
            .fetch_one(self.pool.reader())
            .await?;
        Ok(Block::new(block.bytes.into(), cid.to_owned()))
    }

    async fn has(&self, cid: &Cid) -> anyhow::Result<bool> {
        let len: CountRow = sqlx::query_as(BlockQuery::has())
            .bind(cid.hash().to_bytes())
            .fetch_one(self.pool.reader())
            .await?;
        Ok(len.res > 0)
    }

    async fn put(&self, block: &Block) -> anyhow::Result<bool> {
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
impl ceramic_api::AccessModelStore for SqliteEventStore {
    async fn insert_many(
        &self,
        items: &[(EventId, Option<Vec<u8>>)],
    ) -> anyhow::Result<(Vec<bool>, usize)> {
        let items = items
            .iter()
            .map(|(key, value)| ReconItem::new(key, value.as_deref()))
            .collect::<Vec<ReconItem<'_, EventId>>>();
        let res = self.insert_items_int(&items).await?;
        Ok((res.keys, res.value_count))
    }

    async fn range_with_values(
        &self,
        start: &EventId,
        end: &EventId,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<(EventId, Vec<u8>)>> {
        let res = self
            .range_with_values_int(start, end, offset, limit)
            .await?;
        Ok(res.collect())
    }
    async fn value_for_key(&self, key: &EventId) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.value_for_key_int(key).await?)
    }

    async fn keys_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
    ) -> anyhow::Result<(i64, Vec<EventId>)> {
        Ok(self.new_keys_since_value(highwater, limit).await?)
    }
}
