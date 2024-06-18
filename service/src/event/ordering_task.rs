use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::anyhow;
use ceramic_store::{CeramicOneEvent, SqlitePool};
use cid::Cid;
use tracing::{debug, error, info, trace, warn};

use crate::{CeramicEventService, Error, Result};

/// How many events to select at once to see if they've become deliverable when we have downtime
/// Used at startup and occassionally in case we ever dropped something
/// We keep the number small for now as we may need to traverse many prevs for each one of these and load them into memory.
const DELIVERABLE_EVENTS_BATCH_SIZE: usize = 1000;
/// How many batches of undelivered events are we willing to process on start up?
/// To avoid an infinite loop. It's going to take a long time to process `DELIVERABLE_EVENTS_BATCH_SIZE * MAX_ITERATIONS` events
const MAX_ITERATIONS: usize = 100_000_000;

/// How often should we try to process all undelivered events in case we missed something
const CHECK_ALL_INTERVAL_SECONDS: u64 = 60 * 10; // 10 minutes

type InitCid = cid::Cid;
type PrevCid = cid::Cid;
type EventCid = cid::Cid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeliveredEvent {
    pub(crate) cid: Cid,
    pub(crate) init_cid: InitCid,
}

impl DeliveredEvent {
    pub fn new(cid: Cid, init_cid: InitCid) -> Self {
        Self { cid, init_cid }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct DeliverableMetadata {
    pub(crate) init_cid: InitCid,
    pub(crate) prev: PrevCid,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeliverableEvent {
    pub(crate) cid: EventCid,
    pub(crate) meta: DeliverableMetadata,
    attempts: usize,
    last_attempt: std::time::Instant,
    started: std::time::Instant,
    expires: Option<std::time::Instant>,
}

impl DeliverableEvent {
    pub fn new(cid: Cid, meta: DeliverableMetadata, expires: Option<std::time::Instant>) -> Self {
        Self {
            cid,
            meta,
            attempts: 0,
            last_attempt: std::time::Instant::now(),
            started: std::time::Instant::now(),
            expires,
        }
    }
}

#[derive(Debug)]
pub struct DeliverableTask {
    pub(crate) _handle: tokio::task::JoinHandle<()>,
    pub(crate) tx: tokio::sync::mpsc::Sender<DeliverableEvent>,
    pub(crate) tx_new: tokio::sync::mpsc::Sender<DeliveredEvent>,
}

#[derive(Debug)]
pub struct OrderingTask {}

impl OrderingTask {
    pub async fn run(pool: SqlitePool, q_depth: usize, load_delivered: bool) -> DeliverableTask {
        let (tx, rx) = tokio::sync::mpsc::channel::<DeliverableEvent>(q_depth);
        let (tx_new, rx_new) = tokio::sync::mpsc::channel::<DeliveredEvent>(q_depth);

        let handle =
            tokio::spawn(async move { Self::run_loop(pool, load_delivered, rx, rx_new).await });

        DeliverableTask {
            _handle: handle,
            tx,
            tx_new,
        }
    }

    async fn run_loop(
        pool: SqlitePool,
        load_undelivered: bool,
        mut rx: tokio::sync::mpsc::Receiver<DeliverableEvent>,
        mut rx_new: tokio::sync::mpsc::Receiver<DeliveredEvent>,
    ) {
        // before starting, make sure we've updated any events in the database we missed
        let mut state = OrderingState::new();
        if load_undelivered
            && state
                .process_all_undelivered_events(&pool, MAX_ITERATIONS)
                .await
                .map_err(Self::log_error)
                .is_err()
        {
            return;
        }

        let mut last_processed = std::time::Instant::now();
        loop {
            let mut modified: Option<HashSet<InitCid>> = None;
            let mut need_prev_buf = Vec::with_capacity(100);
            let mut newly_added_buf = Vec::with_capacity(100);

            tokio::select! {
                incoming = rx.recv_many(&mut need_prev_buf, 100) => {
                    if incoming > 0 {
                        modified = Some(state.add_incoming_batch(need_prev_buf));
                    }
                }
                new = rx_new.recv_many(&mut newly_added_buf, 100) => {
                    if new > 0 {
                        modified = Some(newly_added_buf.into_iter().map(|ev| ev.init_cid).collect::<HashSet<InitCid>>());
                    }
                }
                else => {
                    info!(stream_count=%state.pending_by_stream.len(), "Server dropped the ordering task. Processing once more before exiting...");
                    let _ = state
                        .process_events(&pool, None)
                        .await
                        .map_err(Self::log_error);
                    return;
                }
            };
            // Given the math on OrderingState and the generally low number of updates to streams, we are going
            // to ignore pruning until there's  more of an indication that it's necessary. Just log some stats.
            if last_processed.elapsed().as_secs() > CHECK_ALL_INTERVAL_SECONDS {
                let stream_count = state.pending_by_stream.len();
                if stream_count > 1000 {
                    info!(%stream_count, "Over 1000 pending streams without recent updates.");
                } else {
                    debug!(%stream_count, "Fewer than 1000 streams pending without recent updates.");
                }
            }

            if modified.is_some()
                && state
                    .process_events(&pool, modified)
                    .await
                    .map_err(Self::log_error)
                    .is_err()
            {
                return;
            }
            last_processed = std::time::Instant::now();
        }
    }

    /// Log an error and return a result that can be used to stop the task if it was fatal
    fn log_error(err: Error) -> std::result::Result<(), ()> {
        match err {
            Error::Application { error } => {
                warn!("Encountered application error: {:?}", error);
                Ok(())
            }
            Error::Fatal { error } => {
                error!("Encountered fatal error: {:?}", error);
                Err(())
            }
            Error::Transient { error } | Error::InvalidArgument { error } => {
                info!("Encountered error: {:?}", error);
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
/// Rough size estimate:
///     pending_by_stream: 96 * stream_cnt + 540 * event_cnt
///     ready_events: 96 * ready_event_cnt
/// so for stream_cnt = 1000, event_cnt = 2, ready_event_cnt = 1000
/// we get about 1 MB of memory used.
pub struct OrderingState {
    /// Map of undelivered events by init CID (i.e. the stream CID).
    pending_by_stream: HashMap<InitCid, StreamEvents>,
    /// Queue of events that can be marked ready to deliver.
    /// Can be added as long as their prev is stored or in this list ahead of them.
    ready_events: VecDeque<EventCid>,
}

#[derive(Debug, Clone, Default)]
/// ~540 bytes per event in this struct
pub(crate) struct StreamEvents {
    prev_map: HashMap<PrevCid, EventCid>,
    cid_map: HashMap<EventCid, DeliverableEvent>,
}

impl FromIterator<DeliverableEvent> for StreamEvents {
    fn from_iter<T: IntoIterator<Item = DeliverableEvent>>(iter: T) -> Self {
        let mut stream = Self::new();
        for item in iter {
            stream.add_event(item);
        }
        stream
    }
}

impl StreamEvents {
    pub fn new() -> Self {
        Self::default()
    }

    /// returns Some(Stream Init CID) if this is a new event, else None.
    pub fn add_event(&mut self, event: DeliverableEvent) -> Option<InitCid> {
        let res = if self.prev_map.insert(event.meta.prev, event.cid).is_none() {
            Some(event.meta.init_cid)
        } else {
            None
        };
        self.cid_map.insert(event.cid, event);
        res
    }

    pub fn is_empty(&self) -> bool {
        // these should always match
        self.prev_map.is_empty() && self.cid_map.is_empty()
    }

    fn remove_by_event_cid(&mut self, cid: &Cid) -> Option<DeliverableEvent> {
        if let Some(cid) = self.cid_map.remove(cid) {
            self.prev_map.remove(&cid.meta.prev);
            Some(cid)
        } else {
            None
        }
    }

    fn remove_by_prev_cid(&mut self, cid: &Cid) -> Option<EventCid> {
        if let Some(cid) = self.prev_map.remove(cid) {
            self.cid_map.remove(&cid);
            Some(cid)
        } else {
            None
        }
    }
}

impl OrderingState {
    pub fn new() -> Self {
        Self {
            pending_by_stream: HashMap::new(),
            ready_events: VecDeque::new(),
        }
    }

    /// This will review all the events for any streams known to have undelivered events and see if any of them are now deliverable.
    /// If `streams_to_process` is None, all streams will be processed, otherwise only the streams in the set will be processed.
    /// Processing all streams could take a long time and not necessarily do anything productive (if we're missing a key event, we're still blocked).
    /// However, passing a value for `streams_to_process` when we know something has changed is likely to have positive results and be much faster.
    pub(crate) async fn process_events(
        &mut self,
        pool: &SqlitePool,
        streams_to_process: Option<HashSet<InitCid>>,
    ) -> Result<()> {
        self.persist_ready_events(pool).await?;
        for (cid, stream_events) in self.pending_by_stream.iter_mut() {
            if streams_to_process
                .as_ref()
                .map_or(false, |to_do| !to_do.contains(cid))
            {
                continue;
            }
            let deliverable = Self::discover_deliverable_events(pool, stream_events).await?;
            if !deliverable.is_empty() {
                self.ready_events.extend(deliverable)
            }
        }
        if !self.ready_events.is_empty() {
            self.persist_ready_events(pool).await?;
        }

        Ok(())
    }

    /// Removes deliverable events from the `prev_map` and returns them. This means prev is already delivered or in the
    /// list to be marked as delivered. The input is expected to be a list of CIDs for a given stream that are waiting
    /// to be processed. It will still work if it's intermixed for multiple streams, but it's not the most efficient way to use it.
    /// The returned CIDs in the VeqDeque are for events that are expected to be updated FIFO i.e. vec.pop_front()
    ///
    /// This breaks with multi-prev as we expect a single prev for each event. The input map is expected to contain the
    /// (prev <- event) relationship (that is, the value is the event that depends on the key).
    pub(crate) async fn discover_deliverable_events(
        pool: &SqlitePool,
        stream_map: &mut StreamEvents,
    ) -> Result<VecDeque<EventCid>> {
        if stream_map.is_empty() {
            return Ok(VecDeque::new());
        }

        let mut deliverable = VecDeque::new();
        let prev_map_cln = stream_map.prev_map.clone();
        for (prev, ev_cid) in prev_map_cln {
            if stream_map.cid_map.contains_key(&prev) {
                trace!(
                    ?prev,
                    cid=?ev_cid,
                    "Found event that depends on another event in memory"
                );
                // we have it in memory so we need to order it related to others to insert correctly
                // although it may not be possible if the chain just goes back to some unknown event
                // once we find the first event that's deliverable, we can go back through and find the rest
                continue;
            } else {
                let (exists, delivered) = CeramicOneEvent::delivered_by_cid(pool, &prev).await?;
                if delivered {
                    trace!(deliverable=?ev_cid, "Found delivered prev in database. Adding to ready list");
                    deliverable.push_back(ev_cid);
                    stream_map.remove_by_event_cid(&ev_cid);
                } else if exists {
                    trace!("Found undelivered prev in database. Building data to check for deliverable.");
                    // if it's not in memory, we need to read it from the db and parse it for the prev value to add it to our set
                    let data = CeramicOneEvent::value_by_cid(pool, &prev)
                        .await?
                        .ok_or_else(|| {
                            Error::new_app(anyhow!(
                                "Missing data for event that exists should be impossible"
                            ))
                        })?;
                    let (insertable_body, maybe_prev) =
                        CeramicEventService::parse_event_carfile(prev, &data).await?;

                    if let Some(prev) = maybe_prev {
                        let event = DeliverableEvent::new(insertable_body.cid, prev, None);
                        trace!(cid=%event.cid, "Adding event discovered in database to stream pending list");
                        stream_map.add_event(event);
                    } else {
                        warn!(event_cid=%insertable_body.cid,"Found undelivered event with no prev while processing pending. Should not happen.");
                        deliverable.push_back(insertable_body.cid);
                        stream_map.remove_by_event_cid(&ev_cid);
                    }
                } else {
                    trace!(
                        ?ev_cid,
                        "Found event that depends on unknown event. Will check later."
                    );
                }
            }
        }
        let mut newly_ready = deliverable.clone();
        while let Some(cid) = newly_ready.pop_front() {
            if let Some(now_ready_ev) = stream_map.remove_by_prev_cid(&cid) {
                deliverable.push_back(now_ready_ev);
                newly_ready.push_back(now_ready_ev);
            }
        }
        debug!(?deliverable, "deliverable events discovered");

        Ok(deliverable)
    }

    /// Process all undelivered events in the database. This is a blocking operation that could take a long time.
    /// It is intended to be run at startup but could be used on an interval or after some errors to recover.
    pub(crate) async fn process_all_undelivered_events(
        &mut self,
        pool: &SqlitePool,
        max_iterations: usize,
    ) -> Result<()> {
        let mut cnt = 0;
        let mut offset: usize = 0;
        while cnt < max_iterations {
            cnt += 1;
            let (new, found) = self
                .add_undelivered_batch(pool, offset, DELIVERABLE_EVENTS_BATCH_SIZE)
                .await?;
            if new == 0 {
                break;
            } else {
                // We can start processing and we'll follow the stream history if we have it. In that case, we either arrive
                // at the beginning and mark them all delivered, or we find a gap and stop processing and leave them in memory.
                // In this case, we won't discover them until we start running recon with a peer, so maybe we should drop them
                // or otherwise mark them ignored somehow.
                self.process_events(pool, None).await?;
                if new < DELIVERABLE_EVENTS_BATCH_SIZE {
                    break;
                }
                offset = offset.saturating_add(found);
            }
            if cnt >= max_iterations {
                warn!(batch_size=DELIVERABLE_EVENTS_BATCH_SIZE, iterations=%max_iterations, "Exceeded max iterations for finding undelivered events!");
                break;
            }
        }
        if self.ready_events.is_empty() {
            Ok(())
        } else {
            self.persist_ready_events(pool).await?;
            Ok(())
        }
    }

    /// Add a batch of events from the database to the pending list to be processed.
    /// Returns the (#events new events found , #events returned by query)
    async fn add_undelivered_batch(
        &mut self,
        pool: &SqlitePool,
        offset: usize,
        limit: usize,
    ) -> Result<(usize, usize)> {
        let undelivered = CeramicOneEvent::undelivered_with_values(pool, offset, limit).await?;
        trace!(count=%undelivered.len(), "Found undelivered events to process");
        if undelivered.is_empty() {
            return Ok((0, 0));
        }
        let found = undelivered.len();
        let mut new = 0;
        for (key, data) in undelivered {
            let event_cid = key.cid().ok_or_else(|| {
                Error::new_invalid_arg(anyhow::anyhow!("EventID is missing a CID: {}", key))
            })?;
            let (insertable_body, maybe_prev) =
                CeramicEventService::parse_event_carfile(event_cid, &data).await?;
            if let Some(prev) = maybe_prev {
                let event = DeliverableEvent::new(insertable_body.cid, prev, None);
                if self.track_pending(event).is_some() {
                    new += 1;
                }
            } else {
                // safe to ignore in tests, shows up because when we mark init events as undelivered even though they don't have a prev
                info!(event_cid=%insertable_body.cid, "Found undelivered event with no prev while processing undelivered. Should not happen. Likely means events were dropped before.");
                self.ready_events.push_back(insertable_body.cid);
                new += 1; // we treat this as new since it might unlock something else but it's not actually going in our queue is it's a bit odd
            }
        }
        trace!(%new, %found, "Adding undelivered events to pending set");
        Ok((new, found))
    }

    fn add_incoming_batch(&mut self, events: Vec<DeliverableEvent>) -> HashSet<InitCid> {
        let mut updated_streams = HashSet::with_capacity(events.len());
        for event in events {
            if let Some(updated_stream) = self.track_pending(event) {
                updated_streams.insert(updated_stream);
            }
        }
        updated_streams
    }

    /// returns the init event CID (stream CID) if this is a new event
    fn track_pending(&mut self, event: DeliverableEvent) -> Option<InitCid> {
        self.pending_by_stream
            .entry(event.meta.init_cid)
            .or_default()
            .add_event(event)
    }

    /// Modify all the events that are ready to be marked as delivered.

    /// We should improve the error handling and likely add some batching if the number of ready events is very high.
    /// We copy the events up front to avoid losing any events if the task is cancelled.
    async fn persist_ready_events(&mut self, pool: &SqlitePool) -> Result<()> {
        if !self.ready_events.is_empty() {
            let mut to_process = self.ready_events.clone(); // to avoid cancel loss
            tracing::debug!(count=%self.ready_events.len(), "Marking events as ready to deliver");
            let mut tx = pool.begin_tx().await?;

            // We process the ready events as a FIFO queue so they are marked delivered before events
            // that were added after and depend on them.
            while let Some(cid) = to_process.pop_front() {
                CeramicOneEvent::mark_ready_to_deliver(&mut tx, &cid).await?;
            }
            tx.commit().await?;
            self.ready_events.clear(); // safe to clear since we are past any await points and hold exclusive access
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use ceramic_store::EventInsertable;
    use multihash_codetable::{Code, MultihashDigest};
    use recon::ReconItem;
    use test_log::test;

    use crate::tests::{build_event, check_deliverable, random_block, TestEventInfo};

    use super::*;

    /// these events are init events so they should have been delivered
    /// need to build with data events that have the prev stored already
    async fn build_insertable_undelivered() -> EventInsertable {
        let TestEventInfo {
            event_id: id, car, ..
        } = build_event().await;
        let cid = id.cid().unwrap();

        let (body, _meta) = CeramicEventService::parse_event_carfile(cid, &car)
            .await
            .unwrap();
        assert!(!body.deliverable);
        EventInsertable::try_new(id, body).unwrap()
    }

    fn assert_stream_map_elems(map: &StreamEvents, size: usize) {
        assert_eq!(size, map.cid_map.len(), "{:?}", map);
        assert_eq!(size, map.prev_map.len(), "{:?}", map);
    }

    fn build_linked_events(
        number: usize,
        stream_cid: Cid,
        first_prev: Cid,
    ) -> Vec<DeliverableEvent> {
        let mut events = Vec::with_capacity(number);

        let first_cid = random_block().cid;
        events.push(DeliverableEvent::new(
            first_cid,
            DeliverableMetadata {
                init_cid: stream_cid,
                prev: first_prev,
            },
            None,
        ));

        for i in 1..number {
            let random = random_block();
            let ev = DeliverableEvent::new(
                random.cid,
                DeliverableMetadata {
                    init_cid: stream_cid,
                    prev: events[i - 1].cid,
                },
                None,
            );
            events.push(ev);
        }

        events
    }

    #[test(tokio::test)]
    async fn test_none_deliverable_without_first() {
        // they events all point to the one before but A has never been delivered so we can't do anything
        let stream_cid = Cid::new_v1(0x71, Code::Sha2_256.digest(b"arbitrary"));
        let missing = Cid::new_v1(0x71, Code::Sha2_256.digest(b"missing"));
        let events = build_linked_events(4, stream_cid, missing);
        let mut prev_map = StreamEvents::from_iter(events);

        let pool = SqlitePool::connect_in_memory().await.unwrap();

        let deliverable = super::OrderingState::discover_deliverable_events(&pool, &mut prev_map)
            .await
            .unwrap();

        assert_eq!(0, deliverable.len());
    }

    #[test(tokio::test)]
    async fn test_all_deliverable_one_stream() {
        let TestEventInfo {
            event_id: one_id,
            car: one_car,
            ..
        } = build_event().await;
        let one_cid = one_id.cid().unwrap();
        let store = CeramicEventService::new(SqlitePool::connect_in_memory().await.unwrap())
            .await
            .unwrap();
        recon::Store::insert(&store, &ReconItem::new(&one_id, &one_car))
            .await
            .unwrap();

        check_deliverable(&store.pool, &one_cid, true).await;

        let stream_cid = Cid::new_v1(0x71, Code::Sha2_256.digest(b"arbitrary"));

        let events = build_linked_events(4, stream_cid, one_cid);
        let expected = VecDeque::from_iter(events.iter().map(|ev| ev.cid));
        let mut prev_map = StreamEvents::from_iter(events);

        assert_stream_map_elems(&prev_map, 4);
        let deliverable =
            super::OrderingState::discover_deliverable_events(&store.pool, &mut prev_map)
                .await
                .unwrap();

        assert_eq!(4, deliverable.len());
        assert_eq!(expected, deliverable);
        assert_stream_map_elems(&prev_map, 0);
    }

    #[test(tokio::test)]
    async fn test_some_deliverable_one_stream() {
        let TestEventInfo {
            event_id: one_id,
            car: one_car,
            ..
        } = build_event().await;
        let one_cid = one_id.cid().unwrap();
        let store = CeramicEventService::new(SqlitePool::connect_in_memory().await.unwrap())
            .await
            .unwrap();
        recon::Store::insert(&store, &ReconItem::new(&one_id, &one_car))
            .await
            .unwrap();

        check_deliverable(&store.pool, &one_cid, true).await;

        let stream_cid = Cid::new_v1(0x71, Code::Sha2_256.digest(b"arbitrary"));
        let missing = Cid::new_v1(0x71, Code::Sha2_256.digest(b"missing"));

        let mut deliverable_events = build_linked_events(6, stream_cid, one_cid);
        let stuck_events = build_linked_events(8, stream_cid, missing);
        let expected = VecDeque::from_iter(deliverable_events.iter().map(|ev| ev.cid));
        deliverable_events.extend(stuck_events);
        let mut prev_map = StreamEvents::from_iter(deliverable_events);

        assert_stream_map_elems(&prev_map, 14);
        let deliverable =
            super::OrderingState::discover_deliverable_events(&store.pool, &mut prev_map)
                .await
                .unwrap();

        assert_eq!(6, deliverable.len());
        assert_eq!(expected, deliverable);
        assert_stream_map_elems(&prev_map, 8);
    }

    #[test(tokio::test)]
    // expected to be per stream but all events are combined for the history required version currently so
    // this needs to work as well
    async fn test_all_deliverable_multiple_streams() {
        let TestEventInfo {
            event_id: one_id,
            car: one_car,
            ..
        } = build_event().await;
        let TestEventInfo {
            event_id: two_id,
            car: two_car,
            ..
        } = build_event().await;
        let one_cid = one_id.cid().unwrap();
        let two_cid = two_id.cid().unwrap();
        let store = CeramicEventService::new(SqlitePool::connect_in_memory().await.unwrap())
            .await
            .unwrap();
        recon::Store::insert_many(
            &store,
            &[
                ReconItem::new(&one_id, &one_car),
                ReconItem::new(&two_id, &two_car),
            ],
        )
        .await
        .unwrap();

        check_deliverable(&store.pool, &one_cid, true).await;
        check_deliverable(&store.pool, &two_cid, true).await;

        let stream_cid = Cid::new_v1(0x71, Code::Sha2_256.digest(b"arbitrary-one"));
        let stream_cid_2 = Cid::new_v1(0x71, Code::Sha2_256.digest(b"arbitrary-two"));

        let mut events_a = build_linked_events(4, stream_cid, one_cid);
        let mut events_b = build_linked_events(10, stream_cid_2, two_cid);
        let expected_a = VecDeque::from_iter(events_a.iter().map(|ev| ev.cid));
        let expected_b = VecDeque::from_iter(events_b.iter().map(|ev| ev.cid));
        // we expect the events to be in the prev chain order, but they can be intervleaved across streams
        // we reverse the items in the input to proov this (it's a hashmap internally so there is no order, but still)
        events_a.reverse();
        events_b.reverse();
        events_a.extend(events_b);
        assert_eq!(14, events_a.len());
        let mut prev_map = StreamEvents::from_iter(events_a);

        assert_stream_map_elems(&prev_map, 14);
        let deliverable =
            super::OrderingState::discover_deliverable_events(&store.pool, &mut prev_map)
                .await
                .unwrap();

        assert_eq!(14, deliverable.len());
        assert_eq!(0, prev_map.cid_map.len(), "{:?}", prev_map);
        assert_eq!(0, prev_map.prev_map.len(), "{:?}", prev_map);

        let mut split_a = VecDeque::new();
        let mut split_b = VecDeque::new();
        for cid in deliverable {
            if expected_a.contains(&cid) {
                split_a.push_back(cid);
            } else if expected_b.contains(&cid) {
                split_b.push_back(cid);
            } else {
                panic!("Unexpected CID in deliverable list: {:?}", cid);
            }
        }

        assert_eq!(expected_a, split_a);
        assert_eq!(expected_b, split_b);
    }

    #[test(tokio::test)]
    async fn test_undelivered_batch_empty() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let (new, found) = OrderingState::new()
            .add_undelivered_batch(&pool, 0, 10)
            .await
            .unwrap();
        assert_eq!(0, new);
        assert_eq!(0, found);
    }

    #[test(tokio::test)]
    async fn test_undelivered_batch_offset() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let insertable = build_insertable_undelivered().await;

        let _new = CeramicOneEvent::insert_many(&pool, &[insertable])
            .await
            .unwrap();

        let mut state = OrderingState::new();
        let (new, found) = state.add_undelivered_batch(&pool, 0, 10).await.unwrap();
        assert_eq!(1, found);
        assert_eq!(1, new);
        let (new, found) = state.add_undelivered_batch(&pool, 10, 10).await.unwrap();
        assert_eq!(0, new);
        assert_eq!(0, found);
        state.persist_ready_events(&pool).await.unwrap();
        let (new, found) = state.add_undelivered_batch(&pool, 0, 10).await.unwrap();
        assert_eq!(0, new);
        assert_eq!(0, found);
    }

    #[test(tokio::test)]
    async fn test_undelivered_batch_all() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let mut undelivered = Vec::with_capacity(10);
        for _ in 0..10 {
            let insertable = build_insertable_undelivered().await;
            undelivered.push(insertable);
        }

        let (hw, event) = CeramicOneEvent::new_events_since_value(&pool, 0, 1000)
            .await
            .unwrap();
        assert_eq!(0, hw);
        assert!(event.is_empty());

        let _new = CeramicOneEvent::insert_many(&pool, &undelivered[..])
            .await
            .unwrap();

        let mut state = OrderingState::new();
        state
            .process_all_undelivered_events(&pool, 1)
            .await
            .unwrap();

        let (_hw, event) = CeramicOneEvent::new_events_since_value(&pool, 0, 1000)
            .await
            .unwrap();
        assert_eq!(event.len(), 10);
    }
}
