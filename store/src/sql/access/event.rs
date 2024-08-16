use std::{
    num::TryFromIntError,
    ops::Range,
    sync::atomic::{AtomicI64, Ordering},
};

use anyhow::anyhow;
use ceramic_core::{event_id::InvalidEventId, Cid, EventId};
use ceramic_event::unvalidated;
use ipld_core::ipld::Ipld;
use recon::{AssociativeHash, HashCount, Key, Result as ReconResult, Sha256a};

use crate::{
    sql::{
        entities::{
            rebuild_car, BlockRow, CountRow, EventInsertable, OrderKey, ReconEventBlockRaw,
            ReconHash,
        },
        query::{EventQuery, ReconQuery, ReconType, SqlBackend},
        sqlite::SqliteTransaction,
    },
    CeramicOneBlock, CeramicOneEventBlock, Error, Result, SqlitePool,
};

static GLOBAL_COUNTER: AtomicI64 = AtomicI64::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
/// An event that was inserted into the database
pub struct InsertedEvent {
    /// The event order key that was inserted
    pub order_key: EventId,
    /// Whether the event was marked as deliverable
    pub deliverable: bool,
    /// Whether the event was a new key
    pub new_key: bool,
}

impl InsertedEvent {
    /// Create a new delivered event
    fn new(order_key: EventId, new_key: bool, deliverable: bool) -> Self {
        Self {
            order_key,
            deliverable,
            new_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
/// The result of inserting events into the database
pub struct InsertResult {
    /// The events that were marked as delivered in this batch
    pub inserted: Vec<InsertedEvent>,
}

impl InsertResult {
    /// The count of new keys added in this batch
    pub fn count_new_keys(&self) -> usize {
        self.inserted.iter().filter(|e| e.new_key).count()
    }
}

impl InsertResult {
    fn new(inserted: Vec<InsertedEvent>) -> Self {
        Self { inserted }
    }
}

/// Access to the ceramic event table and related logic
pub struct CeramicOneEvent {}

impl CeramicOneEvent {
    fn next_deliverable() -> i64 {
        GLOBAL_COUNTER.fetch_add(1, Ordering::SeqCst)
    }

    /// Insert the event and its hash into the ceramic_one_event table
    async fn insert_event(
        tx: &mut SqliteTransaction<'_>,
        key: &EventId,
        deliverable: bool,
    ) -> Result<bool> {
        let id = key.as_bytes();
        let cid = key
            .cid()
            .map(|cid| cid.to_bytes())
            .ok_or_else(|| Error::new_app(anyhow!("Event CID is required")))?;
        let hash = Sha256a::digest(key);
        let delivered: Option<i64> = if deliverable {
            Some(Self::next_deliverable())
        } else {
            None
        };

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
            .bind(delivered)
            .execute(&mut **tx.inner())
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

    /// Get the current highwater mark for delivered events.
    pub async fn get_highwater_mark(_pool: &SqlitePool) -> Result<i64> {
        Ok(GLOBAL_COUNTER.load(Ordering::Relaxed))
    }

    /// Mark an event ready to deliver to js-ceramic or other clients. This implies it's valid and it's previous events are known.
    pub async fn mark_ready_to_deliver(conn: &mut SqliteTransaction<'_>, key: &Cid) -> Result<()> {
        // Fetch add happens with an open transaction (on one writer for the db) so we're guaranteed to get a unique value
        sqlx::query(EventQuery::mark_ready_to_deliver())
            .bind(Self::next_deliverable())
            .bind(key.to_bytes())
            .execute(&mut **conn.inner())
            .await?;

        Ok(())
    }

    /// Insert many events into the database. The events and their blocks and metadata are inserted in a single
    /// transaction and either all successful or rolled back.
    ///
    /// IMPORTANT:
    ///     It is the caller's responsibility to order events marked deliverable correctly.
    ///     That is, events will be processed in the order they are given so earlier events are given a lower global ordering
    ///     and will be returned earlier in the feed. Events can be intereaved with different streams, but if two events
    ///     depend on each other, the `prev` must come first in the list to ensure the correct order for indexers and consumers.
    pub async fn insert_many<'a, I>(pool: &SqlitePool, to_add: I) -> Result<InsertResult>
    where
        I: Iterator<Item = &'a EventInsertable>,
    {
        let mut inserted = Vec::new();
        let mut tx = pool.begin_tx().await.map_err(Error::from)?;

        for item in to_add {
            let new_key = Self::insert_event(&mut tx, item.order_key(), item.deliverable()).await?;
            inserted.push(InsertedEvent::new(
                item.order_key().clone(),
                new_key,
                item.deliverable(),
            ));
            if new_key {
                for block in item.blocks().iter() {
                    CeramicOneBlock::insert(&mut tx, block.multihash.inner(), &block.bytes).await?;
                    CeramicOneEventBlock::insert(&mut tx, block).await?;
                }
            }
            // the item already existed so we didn't mark it as deliverable on insert
            if !new_key && item.deliverable() {
                Self::mark_ready_to_deliver(&mut tx, &item.cid()).await?;
            }
        }
        tx.commit().await.map_err(Error::from)?;
        let res = InsertResult::new(inserted);

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

        let all_blocks: Vec<ReconEventBlockRaw> =
            sqlx::query_as(EventQuery::value_blocks_by_order_key_many())
                .bind(range.start.as_bytes())
                .bind(range.end.as_bytes())
                .bind(limit)
                .bind(offset)
                .fetch_all(pool.reader())
                .await?;

        let values = ReconEventBlockRaw::into_carfiles(all_blocks).await?;
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
        #[derive(sqlx::FromRow)]
        struct DeliveredEventRow {
            cid: Vec<u8>,
            new_highwater_mark: i64,
        }

        let rows: Vec<DeliveredEventRow> =
            sqlx::query_as(EventQuery::new_delivered_events_id_only())
                .bind(delivered)
                .bind(limit)
                .fetch_all(pool.reader())
                .await?;

        let max: i64 = rows.last().map_or(delivered, |r| r.new_highwater_mark + 1);
        let rows = rows
            .into_iter()
            .map(|row| Cid::try_from(row.cid).map_err(Error::new_app))
            .collect::<Result<Vec<Cid>>>()?;

        Ok((max, rows))
    }

    /// Returns the root CIDs and the data of all the events found after the given delivered value.
    pub async fn new_events_since_value_with_data(
        pool: &SqlitePool,
        delivered: i64,
        limit: i64,
    ) -> Result<(i64, Vec<(Cid, unvalidated::Event<Ipld>)>)> {
        struct DeliveredEventBlockRow {
            block: ReconEventBlockRaw,
            new_highwater_mark: i64,
        }

        use sqlx::Row as _;

        impl sqlx::FromRow<'_, sqlx::sqlite::SqliteRow> for DeliveredEventBlockRow {
            fn from_row(row: &sqlx::sqlite::SqliteRow) -> std::result::Result<Self, sqlx::Error> {
                let new_highwater_mark = row.try_get("new_highwater_mark")?;

                let block = ReconEventBlockRaw::from_row(row)?;
                Ok(Self {
                    block,
                    new_highwater_mark,
                })
            }
        }

        let mut all_blocks: Vec<DeliveredEventBlockRow> =
            sqlx::query_as(EventQuery::new_delivered_events_with_data())
                .bind(delivered)
                .bind(limit)
                .fetch_all(pool.reader())
                .await?;

        // default to the passed in value if there are no new events to avoid the client going back to 0
        let max_highwater = all_blocks
            .iter()
            .map(|row| row.new_highwater_mark + 1)
            .max()
            .unwrap_or(delivered);
        all_blocks.sort_by(|a, b| a.new_highwater_mark.cmp(&b.new_highwater_mark));
        let blocks = all_blocks.into_iter().map(|b| b.block).collect();
        let values = ReconEventBlockRaw::into_events(blocks).await?;
        Ok((max_highwater, values))
    }

    /// Find events that haven't been delivered to the client and may be ready.
    /// Returns the events and their values, and the highwater mark of the last event.
    /// The highwater mark can be used on the next call to get the next batch of events and will be 0 when done.
    pub async fn undelivered_with_values(
        pool: &SqlitePool,
        highwater_mark: i64,
        limit: i64,
    ) -> Result<(Vec<(Cid, unvalidated::Event<Ipld>)>, i64)> {
        struct UndeliveredEventBlockRow {
            block: ReconEventBlockRaw,
            row_id: i64,
        }

        use sqlx::Row as _;

        impl sqlx::FromRow<'_, sqlx::sqlite::SqliteRow> for UndeliveredEventBlockRow {
            fn from_row(row: &sqlx::sqlite::SqliteRow) -> std::result::Result<Self, sqlx::Error> {
                let row_id = row.try_get("rowid")?;
                let block = ReconEventBlockRaw::from_row(row)?;
                Ok(Self { block, row_id })
            }
        }

        let all_blocks: Vec<UndeliveredEventBlockRow> =
            sqlx::query_as(EventQuery::undelivered_with_values())
                .bind(highwater_mark)
                .bind(limit)
                .fetch_all(pool.reader())
                .await?;

        let max_highwater = all_blocks.iter().map(|row| row.row_id).max().unwrap_or(0); // if there's nothing in the list we just return 0
        let blocks = all_blocks.into_iter().map(|b| b.block).collect();
        let values = ReconEventBlockRaw::into_events(blocks).await?;
        Ok((values, max_highwater))
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

    /// Finds if an event exists and has been previously delivered, meaning anything that depends on it can be delivered.
    ///     returns (bool, bool) = (exists, deliverable)
    /// We don't guarantee that a client has seen the event, just that it's been marked as deliverable and they could.
    pub async fn deliverable_by_cid(pool: &SqlitePool, key: &Cid) -> Result<(bool, bool)> {
        #[derive(sqlx::FromRow)]
        struct CidExists {
            exists: bool,
            delivered: bool,
        }
        let exist: Option<CidExists> = sqlx::query_as(EventQuery::value_delivered_by_cid())
            .bind(key.to_bytes())
            .fetch_optional(pool.reader())
            .await?;
        Ok(exist.map_or((false, false), |row| (row.exists, row.delivered)))
    }
}
