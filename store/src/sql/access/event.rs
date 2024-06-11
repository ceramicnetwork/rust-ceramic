use std::{
    num::TryFromIntError,
    ops::Range,
    sync::atomic::{AtomicI64, Ordering},
};

use anyhow::anyhow;
use ceramic_core::{event_id::InvalidEventId, EventId};
use cid::Cid;

use recon::{AssociativeHash, HashCount, Key, Result as ReconResult, Sha256a};

use crate::{
    sql::{
        entities::{
            rebuild_car, BlockRow, CountRow, DeliveredEventRow, EventCid, EventHeader,
            EventInsertable, EventType, OrderKey, ReconEventBlockRaw, ReconHash, StreamCid,
        },
        query::{EventQuery, ReconQuery, ReconType, SqlBackend},
        sqlite::SqliteTransaction,
    },
    CeramicOneBlock, CeramicOneEventBlock, CeramicOneStream, Error, Result, SqlitePool,
};

static GLOBAL_COUNTER: AtomicI64 = AtomicI64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// An event that has been delivered to the client. Generally returned from a batch of inserts.
pub struct CandidateEvent {
    /// The Event CID
    pub cid: EventCid,
    /// The Stream CID
    pub stream_cid: StreamCid,
}

impl CandidateEvent {
    /// Create a new delivered event
    fn new(cid: Cid, stream_cid: StreamCid) -> Self {
        Self { cid, stream_cid }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// The result of inserting events into the database
pub struct InsertResult {
    /// True for new keys, false for existing keys. In same order as the input. BAH THIS ISN'T TRUE
    pub keys: Vec<bool>,
    /// The events that were delivered because they were init events or their previous events were known
    pub delivered: Vec<CandidateEvent>,
    /// Undelivered events in this batch
    pub undelivered: Vec<CandidateEvent>,
}

impl InsertResult {
    fn new(
        keys: Vec<bool>,
        delivered: Vec<CandidateEvent>,
        undelivered: Vec<CandidateEvent>,
    ) -> Self {
        Self {
            keys,
            delivered,
            undelivered,
        }
    }
}

impl From<InsertResult> for recon::InsertResult {
    fn from(res: InsertResult) -> Self {
        Self { keys: res.keys }
    }
}

/// Access to the ceramic event table and related logic
pub struct CeramicOneEvent {}

impl CeramicOneEvent {
    fn next_deliverable() -> i64 {
        GLOBAL_COUNTER.fetch_add(1, Ordering::SeqCst)
    }

    async fn insert_key(
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

    /// Splits the evtents into deliverable and non-deliverable events. Non-deliverable events are checked against the database to see if they can be delivered.
    /// If `require_history` is true, an error is returned if any event is missing history, oherwise they will not be marked as deliverable.
    async fn with_deliverable_marker<'a>(
        events: &'a [EventInsertable],
        pool: &SqlitePool,
        require_history: bool,
    ) -> Result<Vec<(bool, &'a EventInsertable)>> {
        // move all the init events to the front so we make sure to add them first and get the deliverable order correct
        let mut insert = Vec::with_capacity(events.len());
        let mut insert_check_history = Vec::with_capacity(events.len());
        for event in events {
            if event.body.event_type() == EventType::Init {
                insert.push((true, event));
            } else {
                insert_check_history.push(event);
            }
        }

        for event in insert_check_history {
            match &event.body.header {
                EventHeader::Init { .. } => {
                    unreachable!("Init events should have been filtered out")
                }
                EventHeader::Data { cid, prev, .. } | EventHeader::Time { cid, prev, .. } => {
                    // check for prev in this set and fallback to database
                    if insert.iter().any(|(_, e)| e.body.cid == *prev) {
                        insert.push((true, event));
                    } else {
                        let (_, delivered) = Self::delivered_by_cid(pool, prev).await?;
                        if !delivered {
                            if require_history {
                                return Err(Error::new_invalid_arg(anyhow!(
                                    "Missing history for event '{}' (prev: {})",
                                    cid,
                                    prev
                                )));
                            } else {
                                insert.push((false, event));
                            }
                        } else {
                            insert.push((true, event));
                        }
                    }
                }
            }
        }

        Ok(insert)
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
            .bind(&key.to_bytes())
            .execute(&mut **conn.inner())
            .await?;

        Ok(())
    }

    /// Insert many events into the database. If require_history is false, events can be stored without their previous events being present.
    /// For local writes (i.e. over the API), `require_history` should be true. For events discovered over recon, it should be false.
    pub async fn insert_many(
        pool: &SqlitePool,
        to_add: &[EventInsertable],
        require_history: bool,
    ) -> Result<InsertResult> {
        let mut new_keys = vec![false; to_add.len()];
        let mut delivered = Vec::with_capacity(to_add.len());
        let mut undelivered = Vec::with_capacity(to_add.len());
        // TODO: this changes the order of the keys so the response is not in the same order as the input as claimed
        // we currently throw this result away in recon but the recon::Store trait claims that it will be met. would be nice to remove that claim
        let to_add = Self::with_deliverable_marker(to_add, pool, require_history).await?;
        let mut tx = pool.begin_tx().await.map_err(Error::from)?;

        for (idx, (deliverable, item)) in to_add.iter().enumerate() {
            let new_key = Self::insert_key(&mut tx, &item.order_key, *deliverable).await?;
            let candidate = CandidateEvent::new(item.cid(), item.stream_cid());
            if *deliverable {
                delivered.push(candidate);
                // the insert failed so we didn't mark it as deliverable.. is this possible?
                if !new_key {
                    Self::mark_ready_to_deliver(&mut tx, &item.cid()).await?;
                }
            } else {
                undelivered.push(candidate);
            }
            if new_key {
                for block in item.body.blocks.iter() {
                    CeramicOneBlock::insert(&mut tx, block.multihash.inner(), &block.bytes).await?;
                    CeramicOneEventBlock::insert(&mut tx, block).await?;
                }
                if let EventHeader::Init { header, .. } = &item.body.header {
                    CeramicOneStream::insert_tx(&mut tx, item.stream_cid(), header).await?;
                }

                CeramicOneStream::insert_event_header_tx(&mut tx, &item.body.header).await?;
            }

            // see above: this order has changed
            new_keys[idx] = new_key;
        }
        tx.commit().await.map_err(Error::from)?;
        let res = InsertResult::new(new_keys, delivered, undelivered);

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
        let rows: Vec<DeliveredEventRow> = sqlx::query_as(EventQuery::new_delivered_events())
            .bind(delivered)
            .bind(limit)
            .fetch_all(pool.reader())
            .await?;

        DeliveredEventRow::parse_query_results(delivered, rows)
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
    /// (bool, bool) = (exists, delivered)
    pub async fn delivered_by_cid(pool: &SqlitePool, key: &Cid) -> Result<(bool, bool)> {
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
