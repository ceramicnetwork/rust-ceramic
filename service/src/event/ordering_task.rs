use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::anyhow;
use ceramic_store::{CeramicOneEvent, SqlitePool};
use cid::Cid;
use itertools::Itertools;
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

/// Also the `InitCid` as this is the CID of the first event in the stream
type StreamCid = cid::Cid;
type PrevCid = cid::Cid;
type EventCid = cid::Cid;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeliveredEvent {
    pub(crate) cid: Cid,
    pub(crate) stream_cid: StreamCid,
}

impl DeliveredEvent {
    pub fn new(cid: Cid, stream_cid: StreamCid) -> Self {
        Self { cid, stream_cid }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct DeliverableMetadata {
    pub(crate) stream_cid: StreamCid,
    pub(crate) prev: PrevCid,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiscoveredEvent {
    pub(crate) stream_cid: StreamCid,
    pub(crate) cid: EventCid,
    pub(crate) prev: PrevCid,
}

impl DiscoveredEvent {
    pub fn new(cid: EventCid, meta: DeliverableMetadata) -> Self {
        Self {
            cid,
            stream_cid: meta.stream_cid,
            prev: meta.prev,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CandidateEvent {
    cid: EventCid,
    prev: PrevCid,
}

impl From<DiscoveredEvent> for CandidateEvent {
    fn from(value: DiscoveredEvent) -> Self {
        Self {
            cid: value.cid,
            prev: value.prev,
        }
    }
}

#[derive(Debug)]
pub struct DeliverableTask {
    pub(crate) _handle: tokio::task::JoinHandle<()>,
    pub(crate) tx: tokio::sync::mpsc::Sender<DiscoveredEvent>,
    pub(crate) tx_new: tokio::sync::mpsc::Sender<DeliveredEvent>,
}

#[derive(Debug)]
pub struct OrderingTask {}

impl OrderingTask {
    pub async fn run(pool: SqlitePool, q_depth: usize, load_delivered: bool) -> DeliverableTask {
        let (tx, rx) = tokio::sync::mpsc::channel::<DiscoveredEvent>(q_depth);
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
        mut rx: tokio::sync::mpsc::Receiver<DiscoveredEvent>,
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

        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(CHECK_ALL_INTERVAL_SECONDS));
        loop {
            let mut modified: HashSet<StreamCid> = HashSet::new();
            let mut need_prev_buf = Vec::with_capacity(100);
            let mut newly_added_buf = Vec::with_capacity(100);

            tokio::select! {
                _incoming = rx.recv_many(&mut need_prev_buf, 100) => {
                    for event in need_prev_buf {
                        modified.insert(event.stream_cid);
                        state.track_pending(event);
                    }
                }
                _new = rx_new.recv_many(&mut newly_added_buf, 100) => {
                    for ev in newly_added_buf {
                        modified.insert(ev.stream_cid);
                    }
                }
                _ = interval.tick() => {
                    // Given the math on OrderingState and the generally low number of updates to streams, we are going
                    // to ignore pruning undelivered values until there's more of an indication that it's necessary.
                    state.prune_empty_streams();
                    let stream_count = state.pending_by_stream.len();
                    if stream_count > 1000 {
                        debug!(%stream_count, "Over 1000 pending streams without recent updates.");
                    } else {
                        trace!(%stream_count, "Fewer than 1000 streams pending without recent updates.");
                    }
                }
                else => {
                    debug!(stream_count=%state.pending_by_stream.len(), "Server dropped the ordering task. Processing once more before exiting...");
                    if state.process_deliverable_events(&pool, None).await.is_err() {
                        return;
                    }
                }
            };

            if !modified.is_empty() && state.process_deliverable_events(&pool, None).await.is_err()
            {
                return;
            }
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

#[derive(Debug, Clone, Default)]
/// ~540 bytes per event in this struct
pub(crate) struct StreamEvents {
    cid: StreamCid,
    prev_map: HashMap<PrevCid, EventCid>,
    cid_map: HashMap<EventCid, CandidateEvent>,
}

impl StreamEvents {
    fn new_empty(cid: StreamCid) -> Self {
        Self {
            cid,
            prev_map: HashMap::default(),
            cid_map: HashMap::default(),
        }
    }

    /// returns true if this is a new event.
    fn add_event(&mut self, event: DiscoveredEvent) -> bool {
        let event = CandidateEvent::from(event);
        self.add_candidate(event)
    }

    fn add_candidate(&mut self, event: CandidateEvent) -> bool {
        let res = self.prev_map.insert(event.prev, event.cid).is_none();
        self.cid_map.insert(event.cid, event);
        res
    }

    /// Checks if the stream has any outstanding events in memory to deliver
    fn is_empty(&self) -> bool {
        debug_assert_eq!(self.cid_map.len(), self.prev_map.len());
        self.prev_map.is_empty() && self.cid_map.is_empty()
    }

    fn undelivered_cids(&self) -> Vec<EventCid> {
        debug_assert_eq!(self.cid_map.len(), self.prev_map.len());
        self.cid_map.keys().copied().collect()
    }

    fn remove_by_event_cid(&mut self, cid: &Cid) -> Option<CandidateEvent> {
        debug_assert_eq!(self.cid_map.len(), self.prev_map.len());
        if let Some(ev) = self.cid_map.remove(cid) {
            self.prev_map.remove(&ev.prev);
            Some(ev)
        } else {
            None
        }
    }

    fn remove_by_prev_cid(&mut self, prev: &Cid) -> Option<EventCid> {
        debug_assert_eq!(self.cid_map.len(), self.prev_map.len());
        if let Some(cid) = self.prev_map.remove(prev) {
            self.cid_map.remove(&cid);
            Some(cid)
        } else {
            None
        }
    }

    /// Find the next event that is known to be deliverable (tip).
    /// Could be simplified if we could query "current tip for a stream" and then "known events for a stream".
    /// With that info, we could just arrange them rather than have to try to find them one by one.
    async fn find_next_tip(
        &self,
        pool: &SqlitePool,
    ) -> Result<(Option<EventCid>, Vec<CandidateEvent>)> {
        let mut new_tip: Option<EventCid> = None;
        let mut discovered_candidates = Vec::new();
        let prev_cln = self.prev_map.clone();
        for (prev, ev_cid) in prev_cln {
            if self.cid_map.contains_key(&prev) {
                trace!(%prev, %ev_cid, "Discovered prev event in memory");
                continue;
            } else {
                let (exists, delivered) = CeramicOneEvent::delivered_by_cid(pool, &prev).await?;
                if delivered {
                    trace!(new_tip=?ev_cid, %prev, "Found delivered prev in database. Adding to ready list");
                    new_tip = Some(ev_cid);
                    break;
                } else if exists {
                    trace!(%prev, cid=%ev_cid, "Found undelivered prev in database. Building data to check for deliverable.");
                    // if it's not in memory, we need to read it from the db and parse it for the prev value to add it to our set
                    let data = CeramicOneEvent::value_by_cid(pool, &prev)
                        .await?
                        .ok_or_else(|| {
                            Error::new_app(anyhow::anyhow!(
                                "Missing data for event that exists should be impossible"
                            ))
                        })?;
                    let (insertable_body, maybe_prev) =
                        CeramicEventService::parse_event_carfile(prev, &data).await?;

                    if let Some(new_prev) = maybe_prev {
                        let event = CandidateEvent {
                            cid: insertable_body.cid(),
                            prev: new_prev.prev,
                        };
                        debug_assert_eq!(new_prev.stream_cid, self.cid);
                        trace!(
                            ?event,
                            "Adding event discovered in database to stream pending list"
                        );
                        // this might be a tip, or it might have a prev we need to inspect, so we just add it to the set and try again
                        discovered_candidates.push(event);
                    } else {
                        warn!(cid=%insertable_body.cid(), "Found undelivered init event while processing. Should not happen.");
                        new_tip = Some(insertable_body.cid());
                        break;
                    }
                } else {
                    trace!(%prev, cid=%ev_cid, "Unable to find prev in database");
                }
            }
        }
        Ok((new_tip, discovered_candidates))
    }

    /// Builds the chain of deliverable events for the stream. Returns None if no events are deliverable.
    /// Will modify the internal state to remove the events that are deliverable, and will extend the state
    /// with events discovered from the database. In the end, every event that is deliverable will be removed
    /// and returned, while anything that requires discovering an event from a peer will be left in memory.
    async fn process_new_deliverable(
        &mut self,
        pool: &SqlitePool,
    ) -> crate::Result<Option<VecDeque<EventCid>>> {
        if self.is_empty() {
            return Ok(None);
        }

        let mut done = false;
        let mut deliverable = VecDeque::with_capacity(self.prev_map.len());

        let mut cnt = 0;
        while !done && cnt < MAX_ITERATIONS {
            trace!(?self, iteration=%cnt, "looping for new tip: process_new_deliverable");
            cnt += 1;
            let (new_tip, candidates) = self.find_next_tip(pool).await?;
            // Previously, we added the candidates to the set, but didn't retry the loop to see if the chain could be extended. This meant we
            // could end up quitting early, even though it might be possible to find and connect everything if we kept looking. Now, we make
            // sure we discover all the partial chains on the last event received so we can deliver all the events in the stream. This basically
            // comes down to not being able to query "all events for a stream" and "current tip of stream" so we end up building the chain one
            // by one. As this is inefficient, especially for long streams, we should probably add stream meta/header info to the database.
            done = new_tip.is_none() && candidates.is_empty();
            if !candidates.is_empty() {
                trace!(?new_tip, ?candidates, stream=?self, "need to recurse to find more deliverable events");
            }
            for candidate in candidates {
                self.add_candidate(candidate);
            }

            if let Some(mut tip) = new_tip {
                deliverable.push_back(tip);
                self.remove_by_event_cid(&tip);
                while let Some(next_event) = self.remove_by_prev_cid(&tip) {
                    deliverable.push_back(next_event);
                    tip = next_event;
                }
            }
        }

        trace!(stream=?self, ?deliverable, "found deliverable events?");
        if deliverable.is_empty() {
            Ok(None)
        } else {
            Ok(Some(deliverable))
        }
    }
}

#[cfg(test)]
impl StreamEvents {
    pub fn new<I>(cid: StreamCid, events: I) -> Self
    where
        I: IntoIterator<Item = CandidateEvent>,
    {
        let mut new = Self::new_empty(cid);

        for event in events {
            new.add_candidate(event);
        }
        new
    }

    fn pending_event_count(&self) -> usize {
        debug_assert_eq!(self.cid_map.len(), self.prev_map.len());
        self.cid_map.len()
    }
}

#[derive(Debug, Clone)]
pub struct UndeliveredBatch {
    new: usize,
    found: usize,
}

#[derive(Debug)]
/// Rough size estimate:
///     pending_by_stream: 96 * stream_cnt + 540 * event_cnt
///     ready_events: 96 * ready_event_cnt
/// so for stream_cnt = 1000, event_cnt = 2, ready_event_cnt = 1000
/// we get about 1 MB of memory used.
pub struct OrderingState {
    /// Map of undelivered events by init CID (i.e. the stream CID).
    pending_by_stream: HashMap<StreamCid, StreamEvents>,
    /// Queue of events that can be marked ready to deliver.
    /// Can be added as long as their prev is stored or in this list ahead of them.
    ready_events: VecDeque<EventCid>,
}

impl FromIterator<DiscoveredEvent> for OrderingState {
    fn from_iter<I: IntoIterator<Item = DiscoveredEvent>>(iter: I) -> Self {
        let mut ordering_state = OrderingState::new();
        for event in iter {
            ordering_state
                .pending_by_stream
                .entry(event.stream_cid)
                .or_insert_with(|| StreamEvents::new_empty(event.stream_cid))
                .add_event(event);
        }
        ordering_state
    }
}

impl OrderingState {
    pub fn new() -> Self {
        Self {
            pending_by_stream: HashMap::new(),
            ready_events: VecDeque::new(),
        }
    }

    #[cfg(test)]
    fn pending_event_count(&self) -> usize {
        self.pending_by_stream
            .values()
            .map(|s| s.pending_event_count())
            .sum()
    }

    fn prune_empty_streams(&mut self) {
        self.pending_by_stream
            .retain(|_, stream_events| !stream_events.is_empty());
    }

    /// This will review all the events streams in the `streams` set, or all streams known to have undelivered events and see if any of them are now deliverable.
    /// This is currently not cancel safe and could lose events from memory if an error occurs while writing. These events should be rediscovered and processed
    /// on future updates of the stream or on restart but they may not be delivered in the meantime.
    ///
    /// Processing all streams could take a long time and not necessarily do anything productive (if we're missing a key event, we're still blocked).
    /// However, passing a value for `streams_to_process` when we know something has changed is likely to have positive results and be much faster.
    async fn process_deliverable_events(
        &mut self,
        pool: &SqlitePool,
        streams_to_process: Option<HashSet<StreamCid>>,
    ) -> Result<()> {
        trace!(?streams_to_process, "discover_deliverable_events");
        self.prune_empty_streams();
        for (cid, stream_events) in self.pending_by_stream.iter_mut() {
            if streams_to_process
                .as_ref()
                .map_or(false, |to_do| !to_do.contains(cid))
            {
                continue;
            }
            if let Some(deliverable) = stream_events.process_new_deliverable(pool).await? {
                if !deliverable.is_empty() {
                    self.ready_events.extend(deliverable);
                }
            }
        }

        self.persist_ready_events(pool).await?;

        Ok(())
    }

    /// It will return events that can be delivered based on the discovered input (i.e. prev is known and delivered).
    /// The returned CIDs in the VeqDeque are for events that are expected to be updated FIFO i.e. for _ in or vec.pop_front()
    /// It will error if any of the events are not deliverable when ordered in relation to the set or events in the database.
    /// REQUIRES: That the stream init event is already stored in the database otherwise will return an error.
    ///
    /// This breaks with multi-prev as we expect a single prev for each event.
    pub(crate) async fn verify_all_deliverable(
        pool: &SqlitePool,
        events: Vec<DiscoveredEvent>,
    ) -> Result<VecDeque<EventCid>> {
        if events.is_empty() {
            return Ok(VecDeque::new());
        }

        let required = events.len();
        let mut deliverable = VecDeque::with_capacity(required);
        let mut state = Self::from_iter(events);

        for stream_map in state.pending_by_stream.values_mut() {
            if let Some(good_to_go) = stream_map.process_new_deliverable(pool).await? {
                if !good_to_go.is_empty() {
                    deliverable.extend(good_to_go);
                }
            }
        }
        let all_delivered = state.pending_by_stream.values().all(|s| s.is_empty());
        if !all_delivered {
            let undelivered = state
                .pending_by_stream
                .values()
                .flat_map(|s| s.undelivered_cids())
                .collect::<Vec<_>>();
            return Err(Error::new_invalid_arg(anyhow!(
                "Failed to discover history for all events: {}",
                undelivered.iter().join(","),
            )));
        }
        debug_assert_eq!(
            deliverable.len(),
            required,
            "All events must be deliverable"
        );
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
            let UndeliveredBatch { new, found } = self
                .add_undelivered_batch(pool, offset, DELIVERABLE_EVENTS_BATCH_SIZE)
                .await?;
            if new == 0 {
                break;
            } else {
                // We can start processing and we'll follow the stream history if we have it. In that case, we either arrive
                // at the beginning and mark them all delivered, or we find a gap and stop processing and leave them in memory.
                // In this case, we won't discover them until we start running recon with a peer, so maybe we should drop them
                // or otherwise mark them ignored somehow.
                self.process_deliverable_events(pool, None).await?;
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
    ) -> Result<UndeliveredBatch> {
        let undelivered = CeramicOneEvent::undelivered_with_values(pool, offset, limit).await?;
        trace!(count=%undelivered.len(), "Found undelivered events to process");
        if undelivered.is_empty() {
            return Ok(UndeliveredBatch { new: 0, found: 0 });
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
                tracing::trace!(cid=%insertable_body.cid(), ?prev, "Parsed event from database in batch");
                let event = DiscoveredEvent::new(insertable_body.cid(), prev);
                if self.track_pending(event) {
                    new += 1;
                }
            } else {
                // safe to ignore in tests, shows up because when we mark init events as undelivered even though they don't have a prev
                info!(event_cid=%insertable_body.cid(), "Found undelivered event with no prev while processing undelivered. Should not happen. Likely means events were dropped before.");
                self.ready_events.push_back(insertable_body.cid());
                new += 1; // we treat this as new since it might unlock something else but it's not actually going in our queue is it's a bit odd
            }
        }
        debug!(%new, %found, "Adding undelivered events to pending set");
        Ok(UndeliveredBatch { new, found })
    }

    /// returns true if this is a new event
    fn track_pending(&mut self, event: DiscoveredEvent) -> bool {
        self.pending_by_stream
            .entry(event.stream_cid)
            .or_insert_with(|| StreamEvents::new_empty(event.stream_cid))
            .add_event(event)
    }

    /// Modify all the events that are ready to be marked as delivered.
    /// We should improve the error handling and likely add some batching if the number of ready events is very high.
    async fn persist_ready_events(&mut self, pool: &SqlitePool) -> Result<()> {
        if !self.ready_events.is_empty() {
            tracing::debug!(count=%self.ready_events.len(), "Marking events as ready to deliver");
            let mut tx = pool.begin_tx().await?;

            // We process the ready events as a FIFO queue so they are marked delivered before events
            // that were added after and depend on them. Could use `pop_front` but we don't want to modify the queue until committed.
            for cid in &self.ready_events {
                CeramicOneEvent::mark_ready_to_deliver(&mut tx, cid).await?;
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
        assert!(!body.deliverable());
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
    ) -> Vec<DiscoveredEvent> {
        let mut events = Vec::with_capacity(number);

        let first_cid = random_block().cid;
        events.push(DiscoveredEvent::new(
            first_cid,
            DeliverableMetadata {
                stream_cid,
                prev: first_prev,
            },
        ));

        for i in 1..number {
            let random = random_block();
            let ev = DiscoveredEvent::new(
                random.cid,
                DeliverableMetadata {
                    stream_cid,
                    prev: events[i - 1].cid,
                },
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
        let mut prev_map = StreamEvents::new(
            stream_cid,
            events.into_iter().map(|e: DiscoveredEvent| e.into()),
        );

        let pool = SqlitePool::connect_in_memory().await.unwrap();

        let deliverable = prev_map.process_new_deliverable(&pool).await.unwrap();

        assert_eq!(None, deliverable);
    }

    #[tokio::test]
    async fn test_all_deliverable_one_stream() {
        let _ = ceramic_metrics::init_local_tracing();
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
        let mut prev_map = StreamEvents::new(
            stream_cid,
            events.into_iter().map(|e: DiscoveredEvent| e.into()),
        );

        assert_stream_map_elems(&prev_map, 4);

        let deliverable = prev_map
            .process_new_deliverable(&store.pool)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(4, deliverable.len());
        assert_eq!(expected, deliverable);
        assert_stream_map_elems(&prev_map, 0);
    }

    #[tokio::test]
    async fn test_some_deliverable_one_stream() {
        let _ = ceramic_metrics::init_local_tracing();
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

        let mut prev_map = StreamEvents::new(
            stream_cid,
            deliverable_events
                .into_iter()
                .map(|e: DiscoveredEvent| e.into()),
        );

        assert_stream_map_elems(&prev_map, 14);
        let deliverable = prev_map
            .process_new_deliverable(&store.pool)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(6, deliverable.len());
        assert_eq!(expected, deliverable);
        assert_stream_map_elems(&prev_map, 8);
    }

    #[tokio::test]
    async fn test_all_deliverable_multiple_streams() {
        let _ = ceramic_metrics::init_local_tracing();
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
        events_a.reverse();
        events_b.reverse();
        let events = events_a
            .iter()
            .chain(events_b.iter())
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(14, events.len());

        let mut prev_map = OrderingState::from_iter(events);

        prev_map
            .process_deliverable_events(&store.pool, None)
            .await
            .unwrap();
        let ready = prev_map.ready_events.clone();

        assert_eq!(0, ready.len());
        assert_eq!(0, prev_map.pending_event_count(), "{:?}", prev_map);
    }

    #[tokio::test]
    async fn test_undelivered_batch_empty() {
        let _ = ceramic_metrics::init_local_tracing();
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let UndeliveredBatch { new, found } = OrderingState::new()
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
        let UndeliveredBatch { new, found } =
            state.add_undelivered_batch(&pool, 0, 10).await.unwrap();
        assert_eq!(1, found);
        assert_eq!(1, new);
        let UndeliveredBatch { new, found } =
            state.add_undelivered_batch(&pool, 10, 10).await.unwrap();
        assert_eq!(0, new);
        assert_eq!(0, found);
        state.persist_ready_events(&pool).await.unwrap();
        let UndeliveredBatch { new, found } =
            state.add_undelivered_batch(&pool, 0, 10).await.unwrap();
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
