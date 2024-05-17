use std::collections::{HashMap, VecDeque};

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

        loop {
            let mut need_prev_buf = Vec::with_capacity(100);
            let mut newly_added_buf = Vec::with_capacity(100);

            tokio::select! {
                incoming = rx.recv_many(&mut need_prev_buf, 100) => {
                    if incoming > 0 {
                        state.add_incoming_batch(need_prev_buf);
                    }
                }
                new = rx_new.recv_many(&mut newly_added_buf, 100) => {
                    if new > 0 {
                        let mut newly_ready: HashMap<cid::CidGeneric<64>, VecDeque<_>> = HashMap::with_capacity(new); // worst case
                        for item in newly_added_buf {
                            if let Some(waiting) = state.pending_by_stream.get_mut(&item.init_cid) {
                                if let Some(good_to_go) = waiting.remove_by_prev_cid(&item.cid) {
                                    newly_ready.entry(item.init_cid).or_default().push_back(good_to_go);
                                    tracing::trace!(%good_to_go, "Found event unblocked by incoming delivered list.");
                                }
                            }
                        }
                        if !newly_ready.is_empty() {
                            for (updated_stream, mut unblocked) in newly_ready {
                                if let Some(waiting) = state.pending_by_stream.get_mut(&updated_stream) {
                                    while let Some(cid) = unblocked.pop_front() {
                                        // we're we unblocked by the message on the channel and need to be updated
                                        state.ready_events.push_back(cid);
                                        // now find anyone we just unblocked
                                        if let Some(now_ready_ev) = waiting.remove_by_prev_cid(&cid) {
                                            state.ready_events.push_back(now_ready_ev);
                                            unblocked.push_back(now_ready_ev);
                                        }
                                    }
                                }
                            }
                            if state.persist_ready_events(&pool).await.map_err(Self::log_error).is_err() {
                                return;
                            }
                        }
                    }
                }
                else => {
                    info!("Server dropped the ordering task. Processing once more before exiting...");
                    let _ = state
                        .process_events(&pool)
                        .await
                        .map_err(Self::log_error);
                    return;
                }
            };
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
pub struct OrderingState {
    /// Map of undelivered events by init CID.
    ///  { Init CID: { prevCid: eventData } }
    pending_by_stream: HashMap<InitCid, StreamEvents>,
    /// Queue of events that can be marked ready to deliver.
    /// Can be added as long as their prev is stored or in this list ahead of them.
    ready_events: VecDeque<EventCid>,
}

#[derive(Debug, Clone, Default)]
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

    /// returns true if we didn't know about this event previously
    pub fn add_event(&mut self, event: DeliverableEvent) -> bool {
        let res = self.prev_map.insert(event.meta.prev, event.cid).is_some();
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

    /// This will process all the events in the queues to see if they can be marked as delivered.
    pub async fn process_events(&mut self, pool: &SqlitePool) -> Result<()> {
        self.persist_ready_events(pool).await?;
        self.process_pending_events(pool).await?;

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
                    // if it's not in memory, we need to read it from the db and parse it for the prev value
                    // to add it to our set. this is most likely when processing a batch of events that were undelivered

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

    /// Review all pending items to see if they can be delivered now. Requires reviewing the database and the events we have in memory.
    /// Will order the events as defined by the linked list of prevs and add them to the queue to be marked as delivered.
    /// Returns true if new events were discovered and added to the ready list.
    async fn process_pending_events(&mut self, pool: &SqlitePool) -> Result<()> {
        let mut new_to_persist = false;
        for stream_events in self.pending_by_stream.values_mut() {
            let deliverable = Self::discover_deliverable_events(pool, stream_events).await?;
            if !deliverable.is_empty() {
                new_to_persist = true;
                self.ready_events.extend(deliverable)
            }
        }
        if new_to_persist {
            self.persist_ready_events(pool).await?;
        }

        Ok(())
    }

    /// Process all undelivered events in the database. This is a blocking operation that could take a long time
    /// and is intended to be run at startup.
    pub async fn process_all_undelivered_events(
        &mut self,
        pool: &SqlitePool,
        max_iterations: usize,
    ) -> Result<()> {
        let mut cnt = 0;
        let mut offset = 0;
        while cnt < max_iterations {
            cnt += 1;
            let (new, found) = self
                .add_undelivered_batch(pool, offset, DELIVERABLE_EVENTS_BATCH_SIZE)
                .await?;
            if new == 0 {
                break;
            } else {
                // process the pending events to clear out the queue
                // we have a batch of events in our pending queue by stream ID. We can pick one and process it.
                self.process_events(pool).await?;
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
                if self.track_pending(event) {
                    new += 1;
                }
            } else {
                // safe to ignore in tests, shows up because when we mark init events as undelivered even though they don't have a prev
                info!(event_cid=%insertable_body.cid,"Found undelivered event with no prev while processing undelivered. Should not happen. Likely means events were dropped before.");
                self.ready_events.push_back(insertable_body.cid);
                new += 1; // we treat this as new since it might unlock something else but it's not actually going in our queue is it's a bit odd
            }
        }
        trace!(%new, %found, "Adding undelivered events to pending set");
        Ok((new, found))
    }

    fn add_incoming_batch(&mut self, events: Vec<DeliverableEvent>) {
        for event in events {
            self.track_pending(event);
        }
    }

    /// returns true if this is a new event
    fn track_pending(&mut self, event: DeliverableEvent) -> bool {
        self.pending_by_stream
            .entry(event.meta.init_cid)
            .or_default()
            .add_event(event)
    }

    /// Process all the events that are ready to be marked as delivered
    async fn persist_ready_events(&mut self, pool: &SqlitePool) -> Result<()> {
        if !self.ready_events.is_empty() {
            tracing::debug!(count=%self.ready_events.len(), "Marking events as ready to deliver");
            let mut tx = pool.begin_tx().await?;

            // We process the ready events as a FIFO queue so they are marked delivered before events
            // that were added after and depend on them.
            while let Some(cid) = self.ready_events.pop_front() {
                CeramicOneEvent::mark_ready_to_deliver(&mut tx, &cid).await?;
            }
            tx.commit().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use ceramic_store::EventInsertable;
    use multihash_codetable::{Code, MultihashDigest};
    use recon::ReconItem;

    use crate::tests::{build_event, check_deliverable, random_block};

    use super::*;

    /// these events are init events so they should have been delivered
    /// need to build with data events that have the prev stored already
    async fn build_insertable_undelivered() -> EventInsertable {
        let (id, _, car) = build_event().await;
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

    #[tokio::test]
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

    #[tokio::test]
    async fn test_all_deliverable_one_stream() {
        let _ = ceramic_metrics::init_local_tracing();
        let (one_id, _, one_car) = build_event().await;
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

    #[tokio::test]
    async fn test_some_deliverable_one_stream() {
        let _ = ceramic_metrics::init_local_tracing();
        let (one_id, _, one_car) = build_event().await;
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

    #[tokio::test]
    // expected to be per stream but all events are combined for the history required version currently so
    // this needs to work as well
    async fn test_all_deliverable_multiple_streams() {
        let _ = ceramic_metrics::init_local_tracing();
        let (one_id, _, one_car) = build_event().await;
        let (two_id, _, two_car) = build_event().await;
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

    #[tokio::test]
    async fn test_undelivered_batch_empty() {
        let _ = ceramic_metrics::init_local_tracing();
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let (new, found) = OrderingState::new()
            .add_undelivered_batch(&pool, 0, 10)
            .await
            .unwrap();
        assert_eq!(0, new);
        assert_eq!(0, found);
    }

    #[tokio::test]
    async fn test_undelivered_batch_offset() {
        let _ = ceramic_metrics::init_local_tracing();
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

    #[tokio::test]
    async fn test_undelivered_batch_all() {
        let _ = ceramic_metrics::init_local_tracing();

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
