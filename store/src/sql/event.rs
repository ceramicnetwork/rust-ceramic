use std::{
    num::TryFromIntError,
    ops::Range,
    sync::atomic::{AtomicI64, Ordering},
};

use anyhow::anyhow;
use ceramic_core::{event_id::InvalidEventId, EventId};
use cid::Cid;
use recon::{AssociativeHash, HashCount, InsertResult, Key, Result as ReconResult, Sha256a};

use crate::{
    sql::{
        entities::{
            rebuild_car, BlockRow, CountRow, DeliveredEvent, EventBlockRaw, EventRaw, OrderKey,
            ReconHash,
        },
        query::{EventQuery, ReconQuery, ReconType, SqlBackend},
    },
    CeramicOneBlock, CeramicOneEventBlock, DbTxSqlite, Error, Result, SqlitePool,
};

static GLOBAL_COUNTER: AtomicI64 = AtomicI64::new(0);

/// Access to the ceramic event table and related logic
pub struct CeramicOneEvent {}

impl CeramicOneEvent {
    async fn insert_key(tx: &mut DbTxSqlite<'_>, key: &EventId) -> Result<bool> {
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
            .execute(&mut **tx)
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

impl CeramicOneEvent {
    /// Initialize the delivered event counter. Should be called on startup.
    pub async fn init_delivered_order(pool: &SqlitePool) -> Result<()> {
        let max_delivered: CountRow = sqlx::query_as(EventQuery::max_delivered())
            .fetch_one(pool.reader())
            .await?;
        let max = max_delivered
            .res
            .checked_add(1)
            .ok_or_else(|| Error::new_fatal(anyhow!("More than i64::MAX delivered events!")))?;
        GLOBAL_COUNTER.fetch_max(max, std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }

    /// Mark an event ready to deliver to js-ceramic or other clients. This implies it's valid and it's previous events are known.
    pub async fn mark_ready_to_deliver(conn: &mut DbTxSqlite<'_>, key: &EventId) -> Result<()> {
        let id = key.as_bytes();
        // Fetch add happens with an open transaction (on one writer for the db) so we're guaranteed to get a unique value
        sqlx::query(EventQuery::mark_ready_to_deliver())
            .bind(GLOBAL_COUNTER.fetch_add(1, Ordering::SeqCst))
            .bind(id)
            .execute(&mut **conn)
            .await?;

        Ok(())
    }

    /// Parse an event from carfiles and store in the database. This is temporary and the parsing logic will be moved up to the service layer,
    /// and we will only rely on `insert_many` to store the events.
    pub async fn insert_raw_carfiles(
        pool: &SqlitePool,
        items: &[(EventId, &[u8])],
    ) -> Result<InsertResult> {
        if items.is_empty() {
            return Ok(InsertResult::new(vec![]));
        }
        let mut to_add = Vec::with_capacity(items.len());
        for (key, val) in items {
            match EventRaw::try_build(key.to_owned(), val).await {
                Ok(parsed) => to_add.push(parsed),
                Err(error) => {
                    tracing::warn!(%error, order_key=%key, "Error parsing event into carfile");
                    continue;
                }
            }
        }
        if to_add.is_empty() {
            return Ok(InsertResult::new(vec![]));
        }
        Self::insert_many(pool, &to_add).await
    }

    /// Insert many events into the database. This is the main function to use when storing events.
    pub async fn insert_many(pool: &SqlitePool, to_add: &[EventRaw]) -> Result<InsertResult> {
        let mut new_keys = vec![false; to_add.len()];

        let mut tx = pool.writer().begin().await.map_err(Error::from)?;

        for (idx, item) in to_add.iter().enumerate() {
            let new_key = Self::insert_key(&mut tx, &item.order_key).await?;
            // TODO: should we skip ever? all keys should be new, right?
            for block in item.blocks.iter() {
                CeramicOneBlock::insert(&mut tx, block.multihash.inner(), &block.bytes).await?;
                CeramicOneEventBlock::insert(&mut tx, block).await?;
            }

            Self::mark_ready_to_deliver(&mut tx, &item.order_key).await?;
            new_keys[idx] = new_key;
        }
        tx.commit().await.map_err(Error::from)?;
        let res = InsertResult::new(new_keys);

        Ok(res)
    }

    /// Calculate the hash of a range of events
    pub async fn hash_range(
        pool: &SqlitePool,
        range: Range<&EventId>,
    ) -> ReconResult<HashCount<Sha256a>> {
        let row: ReconHash =
            sqlx::query_as(ReconQuery::hash_range(ReconType::Event, SqlBackend::Sqlite))
                .bind(range.start.as_bytes())
                .bind(range.end.as_bytes())
                .fetch_one(pool.reader())
                .await
                .map_err(Error::from)?;
        Ok(HashCount::new(Sha256a::from(row.hash()), row.count()))
    }

    /// Find a range of event IDs
    pub async fn range(
        pool: &SqlitePool,
        range: Range<&EventId>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<EventId>> {
        let offset: i64 = offset.try_into().map_err(|_e: TryFromIntError| {
            Error::new_app(anyhow!("Offset too large to fit into i64"))
        })?;
        let limit = limit.try_into().unwrap_or(100000); // 100k is still a huge limit
        let rows: Vec<OrderKey> = sqlx::query_as(ReconQuery::range(ReconType::Event))
            .bind(range.start.as_bytes())
            .bind(range.end.as_bytes())
            .bind(limit)
            .bind(offset)
            .fetch_all(pool.reader())
            .await
            .map_err(Error::from)?;
        let rows = rows
            .into_iter()
            .map(|k| EventId::try_from(k).map_err(|e: InvalidEventId| Error::new_app(anyhow!(e))))
            .collect::<Result<Vec<EventId>>>()?;
        Ok(rows)
    }

    /// Find a range of event IDs with their values. Should replace `range` when we move to discovering values and keys simultaneously.
    pub async fn range_with_values(
        pool: &SqlitePool,
        range: Range<&EventId>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(EventId, Vec<u8>)>> {
        let offset = offset.try_into().unwrap_or(i64::MAX);
        let limit: i64 = limit.try_into().unwrap_or(i64::MAX);
        let all_blocks: Vec<EventBlockRaw> =
            sqlx::query_as(EventQuery::value_blocks_by_order_key_many())
                .bind(range.start.as_bytes())
                .bind(range.end.as_bytes())
                .bind(limit)
                .bind(offset)
                .fetch_all(pool.reader())
                .await?;

        let values = EventBlockRaw::into_carfiles(all_blocks).await?;
        Ok(values)
    }

    /// Count the number of events in a range
    pub async fn count(pool: &SqlitePool, range: Range<&EventId>) -> ReconResult<usize> {
        let row: CountRow = sqlx::query_as(ReconQuery::count(ReconType::Event, SqlBackend::Sqlite))
            .bind(range.start.as_bytes())
            .bind(range.end.as_bytes())
            .fetch_one(pool.reader())
            .await
            .map_err(Error::from)?;
        Ok(row.res as usize)
    }

    /// Returns the root CIDs of all the events found after the given delivered value.
    pub async fn new_events_since_value(
        pool: &SqlitePool,
        delivered: i64,
        limit: i64,
    ) -> Result<(i64, Vec<Cid>)> {
        let rows: Vec<DeliveredEvent> = sqlx::query_as(EventQuery::new_delivered_events())
            .bind(delivered)
            .bind(limit)
            .fetch_all(pool.reader())
            .await?;

        DeliveredEvent::parse_query_results(delivered, rows)
    }

    /// Finds the event data by a given EventId i.e. "order key".
    pub async fn value_by_order_key(pool: &SqlitePool, key: &EventId) -> Result<Option<Vec<u8>>> {
        let blocks: Vec<BlockRow> = sqlx::query_as(EventQuery::value_blocks_by_order_key_one())
            .bind(key.as_bytes())
            .fetch_all(pool.reader())
            .await?;
        rebuild_car(blocks).await
    }

    /// Finds the event data by a given CID i.e. the root CID in the carfile of the event.
    pub async fn value_by_cid(pool: &SqlitePool, key: &Cid) -> Result<Option<Vec<u8>>> {
        let blocks: Vec<BlockRow> = sqlx::query_as(EventQuery::value_blocks_by_cid_one())
            .bind(key.to_bytes())
            .fetch_all(pool.reader())
            .await?;
        rebuild_car(blocks).await
    }
}
