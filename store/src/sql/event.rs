use std::{num::TryFromIntError, ops::Range};

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use ceramic_core::{event_id::InvalidEventId, EventId};

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
        EventBlockRaw, EventQuery, EventRaw, OrderKey, ReconHash, ReconQuery, ReconType,
        SqlBackend, GLOBAL_COUNTER,
    },
    DbTxSqlite, Error, Result, SqlitePool,
};

/// Unified implementation of [`recon::Store`] and [`iroh_bitswap::Store`] that can expose the
/// individual blocks from the CAR files directly.
#[derive(Clone, Debug)]
pub struct SqliteEventStore {
    /// The sqlite pool to use for database connections.
    pub pool: SqlitePool,
}

impl SqliteEventStore {
    /// Create an instance of the store initializing any neccessary tables.
    pub async fn new(pool: SqlitePool) -> Result<Self> {
        let store = SqliteEventStore { pool };
        store.init_delivered().await?;
        Ok(store)
    }

    /// Merge the blocks from one sqlite database into this one.
    pub async fn merge_blocks_from_sqlite(&self, input_ceramic_db_filename: &str) -> Result<()> {
        self.pool
            .merge_blocks_from_sqlite(input_ceramic_db_filename)
            .await
    }

    /// Backup the sqlite file to the given filename.
    pub async fn backup_to_sqlite(&self, output_ceramic_db_filename: &str) -> Result<()> {
        self.pool.backup_to_sqlite(output_ceramic_db_filename).await
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

    async fn range_with_values_int(
        &self,
        range: Range<&EventId>,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (EventId, Vec<u8>)> + Send + 'static>> {
        let offset = offset.try_into().unwrap_or(i64::MAX);
        let limit: i64 = limit.try_into().unwrap_or(i64::MAX);
        let all_blocks: Vec<EventBlockRaw> =
            sqlx::query_as(EventQuery::value_blocks_by_order_key_many())
                .bind(range.start.as_bytes())
                .bind(range.end.as_bytes())
                .bind(limit)
                .bind(offset)
                .fetch_all(self.pool.reader())
                .await?;

        let values = EventBlockRaw::into_carfiles(all_blocks).await?;
        Ok(Box::new(values.into_iter()))
    }

    async fn value_for_order_key_int(&self, key: &EventId) -> Result<Option<Vec<u8>>> {
        let blocks: Vec<BlockRow> = sqlx::query_as(EventQuery::value_blocks_by_order_key_one())
            .bind(key.to_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        rebuild_car(blocks).await
    }

    async fn value_for_cid_int(&self, key: &Cid) -> Result<Option<Vec<u8>>> {
        let blocks: Vec<BlockRow> = sqlx::query_as(EventQuery::value_blocks_by_cid_one())
            .bind(key.to_bytes())
            .fetch_all(self.pool.reader())
            .await?;
        rebuild_car(blocks).await
    }

    async fn insert_item_int(&self, item: ReconItem<'_, EventId>) -> Result<bool> {
        let res = self.insert_items_int(&[item]).await?;
        let new_key = res.keys.first().cloned().unwrap_or(false);
        Ok(new_key)
    }

    /// Insert many items into the store (internal to the store)
    async fn insert_items_int(&self, items: &[ReconItem<'_, EventId>]) -> Result<InsertResult> {
        if items.is_empty() {
            return Ok(InsertResult::new(vec![]));
        }
        let mut to_add = vec![];
        for item in items {
            match EventRaw::try_build(item.key.to_owned(), item.value).await {
                Ok(parsed) => to_add.push(parsed),
                Err(error) => {
                    tracing::warn!(%error, order_key=%item.key, "Error parsing event into carfile");
                    continue;
                }
            }
        }
        if to_add.is_empty() {
            return Ok(InsertResult::new(vec![]));
        }
        let mut new_keys = vec![false; to_add.len()];
        let mut tx = self.pool.writer().begin().await.map_err(Error::from)?;

        for (idx, item) in to_add.into_iter().enumerate() {
            let new_key = self.insert_key_int(&item.order_key, &mut tx).await?;
            if new_key {
                // Only add the blocks if this is a new key, otherwise ignore the value.
                // Will adjust with IOD changes but we may want to update the value if it's
                // missing in case we failed somehow on a previous attempt.
                for block in item.blocks.iter() {
                    self.insert_event_block_int(block, &mut tx).await?;
                }
                self.mark_ready_to_deliver(&item.order_key, &mut tx).await?;
            }
            new_keys[idx] = new_key;
        }
        tx.commit().await.map_err(Error::from)?;
        let res = InsertResult::new(new_keys);

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

    /// Returns the root CIDs of all the events found after the given delivered value.
    pub async fn new_events_since_value(
        &self,
        delivered: i64,
        limit: i64,
    ) -> Result<(i64, Vec<Cid>)> {
        let rows: Vec<DeliveredEvent> = sqlx::query_as(EventQuery::new_delivered_events())
            .bind(delivered)
            .bind(limit)
            .fetch_all(self.pool.reader())
            .await?;

        DeliveredEvent::parse_query_results(delivered, rows)
    }
    async fn get_block(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        let block: Option<BlockBytes> = sqlx::query_as(BlockQuery::get())
            .bind(cid.hash().to_bytes())
            .fetch_optional(self.pool.reader())
            .await?;
        Ok(block.map(|b| b.bytes))
    }
}

#[async_trait]
impl recon::Store for SqliteEventStore {
    type Key = EventId;
    type Hash = Sha256a;

    /// Returns true if the key was new. The value is always updated if included
    async fn insert(&self, item: &ReconItem<'_, Self::Key>) -> ReconResult<bool> {
        Ok(self.insert_item_int(item.to_owned()).await?)
    }

    /// Insert new keys into the key space.
    /// Returns true if a key did not previously exist.
    async fn insert_many(&self, items: &[ReconItem<'_, EventId>]) -> ReconResult<InsertResult> {
        match items.len() {
            0 => Ok(InsertResult::new(vec![])),
            _ => {
                let res = self.insert_items_int(items).await?;
                Ok(res)
            }
        }
    }

    /// return the hash and count for a range
    #[instrument(skip(self))]
    async fn hash_range(&self, range: Range<&Self::Key>) -> ReconResult<HashCount<Self::Hash>> {
        let row: ReconHash =
            sqlx::query_as(ReconQuery::hash_range(ReconType::Event, SqlBackend::Sqlite))
                .bind(range.start.as_bytes())
                .bind(range.end.as_bytes())
                .fetch_one(self.pool.reader())
                .await
                .map_err(Error::from)?;
        Ok(HashCount::new(Self::Hash::from(row.hash()), row.count()))
    }

    #[instrument(skip(self))]
    async fn range(
        &self,
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = EventId> + Send + 'static>> {
        let offset: i64 = offset.try_into().map_err(|_e: TryFromIntError| {
            ReconError::new_app(anyhow!("Offset too large to fit into i64"))
        })?;
        let limit = limit.try_into().unwrap_or(100000); // 100k is still a huge limit
        let rows: Vec<OrderKey> = sqlx::query_as(ReconQuery::range(ReconType::Event))
            .bind(range.start.as_bytes())
            .bind(range.end.as_bytes())
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
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = (EventId, Vec<u8>)> + Send + 'static>> {
        Ok(self.range_with_values_int(range, offset, limit).await?)
    }

    /// Return the number of keys within the range.
    #[instrument(skip(self))]
    async fn count(&self, range: Range<&Self::Key>) -> ReconResult<usize> {
        let row: CountRow = sqlx::query_as(ReconQuery::count(ReconType::Event, SqlBackend::Sqlite))
            .bind(range.start.as_bytes())
            .bind(range.end.as_bytes())
            .fetch_one(self.pool.reader())
            .await
            .map_err(Error::from)?;
        Ok(row.res as usize)
    }

    #[instrument(skip(self))]
    async fn value_for_key(&self, key: &EventId) -> ReconResult<Option<Vec<u8>>> {
        Ok(self.value_for_order_key_int(key).await?)
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
        let block = self.get_block(cid).await?;
        Ok(Block::new(
            block
                .ok_or_else(|| anyhow!("block {cid} does not exist"))?
                .into(),
            cid.to_owned(),
        ))
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
    async fn insert_many(&self, items: &[(EventId, Vec<u8>)]) -> anyhow::Result<Vec<bool>> {
        let items = items
            .iter()
            .map(|(key, value)| ReconItem::new(key, value))
            .collect::<Vec<ReconItem<'_, EventId>>>();
        let res = self.insert_items_int(&items).await?;
        Ok(res.keys)
    }

    async fn range_with_values(
        &self,
        range: Range<EventId>,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<(Cid, Vec<u8>)>> {
        self.range_with_values_int(&range.start..&range.end, offset, limit)
            .await?
            .map(|(event_id, value)| {
                Ok((
                    event_id
                        .cid()
                        .ok_or_else(|| anyhow!("EventId does not have an event CID"))?,
                    value,
                ))
            })
            .collect()
    }
    async fn value_for_order_key(&self, key: &EventId) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.value_for_order_key_int(key).await?)
    }

    async fn value_for_cid(&self, key: &Cid) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.value_for_cid_int(key).await?)
    }

    async fn events_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
    ) -> anyhow::Result<(i64, Vec<Cid>)> {
        Ok(self.new_events_since_value(highwater, limit).await?)
    }
    async fn get_block(&self, cid: &Cid) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.get_block(cid).await?)
    }
}
