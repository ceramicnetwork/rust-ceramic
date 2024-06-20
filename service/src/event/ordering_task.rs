use std::collections::{HashMap, VecDeque};

use ceramic_core::EventId;
use ceramic_store::{CeramicOneEvent, SqlitePool};
use cid::Cid;
use tracing::{debug, error, info, trace, warn};

use crate::{CeramicEventService, Error, Result};

use super::service::{EventMetadata, InsertableBodyWithMeta};

type StreamCid = Cid;
type EventCid = Cid;
type PrevCid = Cid;

#[derive(Debug)]
pub struct DeliverableTask {
    pub(crate) _handle: tokio::task::JoinHandle<()>,
    /// Currently only receives events discovered over recon that are out of order and need to be marked ready (deliverable)
    /// when their prev chain is discovered and complete (i.e. my prev is deliverable then I am deliverable).
    pub(crate) tx_inserted: tokio::sync::mpsc::Sender<InsertableBodyWithMeta>,
}

#[derive(Debug)]
pub struct OrderingTask {}

impl OrderingTask {
    /// Discover all undelivered events in the database and mark them deliverable if possible.
    /// Returns the number of events marked deliverable.
    pub async fn process_all_undelivered_events(
        pool: &SqlitePool,
        max_iterations: usize,
        batch_size: u32,
    ) -> Result<usize> {
        OrderingState::process_all_undelivered_events(pool, max_iterations, batch_size).await
    }

    /// Spawn a task to run the ordering task background process in a loop
    pub async fn run(pool: SqlitePool, q_depth: usize) -> DeliverableTask {
        let (tx_inserted, rx_inserted) =
            tokio::sync::mpsc::channel::<InsertableBodyWithMeta>(q_depth);

        let handle = tokio::spawn(async move { Self::run_loop(pool, rx_inserted).await });

        DeliverableTask {
            _handle: handle,
            tx_inserted,
        }
    }

    async fn run_loop(
        pool: SqlitePool,
        mut rx_inserted: tokio::sync::mpsc::Receiver<InsertableBodyWithMeta>,
    ) {
        let mut state = OrderingState::new();

        while !rx_inserted.is_closed() {
            let mut recon_events = Vec::with_capacity(100);
            // consider trying to recv in a loop until X or 10ms whatever comes first and then process
            // the more events we get in memory, the fewer queries we need to run.
            if rx_inserted.recv_many(&mut recon_events, 100).await > 0 {
                trace!(?recon_events, "new events discovered!");
                state.add_inserted_events(recon_events);

                if state
                    .process_streams(&pool)
                    .await
                    .map_err(Self::log_error)
                    .is_err()
                {
                    return;
                }
            }
        }

        let _ = state.process_streams(&pool).await.map_err(Self::log_error);
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum StreamEvent {
    InitEvent(EventCid),
    /// An event that is known to be deliverable from the database
    KnownDeliverable(StreamEventMetadata),
    /// An event that needs more history to be deliverable
    Undelivered(StreamEventMetadata),
}

impl StreamEvent {
    fn is_deliverable(&self) -> bool {
        match self {
            StreamEvent::InitEvent(_) | StreamEvent::KnownDeliverable(_) => true,
            StreamEvent::Undelivered(_) => false,
        }
    }

    /// Builds a stream event from the database if it exists.
    async fn load_by_cid(pool: &SqlitePool, cid: EventCid) -> Result<Option<Self>> {
        // TODO: Condense the multiple DB queries happening here into a single query
        let (exists, deliverable) = CeramicOneEvent::deliverable_by_cid(pool, &cid).await?;
        if exists {
            let parsed_body =
                if let Some(body) = CeramicEventService::load_by_cid(pool, cid).await? {
                    body
                } else {
                    warn!(%cid, "No event body found for event that should exist");
                    return Ok(None);
                };

            let known_prev = match &parsed_body.metadata {
                EventMetadata::Init { cid, .. } => {
                    assert!(
                        deliverable,
                        "Init event must always be deliverable. Found undelivered CID: {}",
                        cid
                    );
                    StreamEvent::InitEvent(*cid)
                }
                EventMetadata::Data { prev, .. } | EventMetadata::Time { prev, .. } => {
                    if deliverable {
                        trace!(%cid, "Found deliverable event in database");
                        StreamEvent::KnownDeliverable(StreamEventMetadata::new(cid, *prev))
                    } else {
                        trace!(%cid, "Found undelivered event in database");
                        StreamEvent::Undelivered(StreamEventMetadata::new(cid, *prev))
                    }
                }
            };
            Ok(Some(known_prev))
        } else {
            trace!(%cid, "Missing event in database");
            Ok(None)
        }
    }
}

impl From<InsertableBodyWithMeta> for StreamEvent {
    fn from(ev: InsertableBodyWithMeta) -> Self {
        match ev.metadata {
            EventMetadata::Init { cid, .. } => StreamEvent::InitEvent(cid),
            EventMetadata::Data { cid, prev, .. } | EventMetadata::Time { cid, prev, .. } => {
                let meta = StreamEventMetadata::new(cid, prev);
                if ev.body.deliverable() {
                    StreamEvent::KnownDeliverable(meta)
                } else {
                    StreamEvent::Undelivered(meta)
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StreamEventMetadata {
    cid: EventCid,
    prev: PrevCid,
}

impl StreamEventMetadata {
    fn new(cid: EventCid, prev: PrevCid) -> Self {
        Self { cid, prev }
    }
}

#[derive(Debug, Clone)]
/// ~500 bytes per event in this struct
pub(crate) struct StreamEvents {
    /// Map of `event.prev` to `event.cid` to determine which event depended on a newly
    /// discovered deliverable event.
    prev_map: HashMap<PrevCid, EventCid>,
    /// Map of `event.cid` to `metadata` for quick lookup of the event metadata.
    cid_map: HashMap<EventCid, StreamEvent>,
    /// whether we should process this stream because new events have been added
    should_process: bool,
    /// The newly discovered events that are deliverable and should be processed.
    new_deliverable: VecDeque<EventCid>,
}

impl Default for StreamEvents {
    fn default() -> Self {
        Self {
            prev_map: HashMap::default(),
            cid_map: HashMap::default(),
            // default to true so we try to follow the event history on the the first batch loading
            // will also avoid any possible race conditions if we somehow get things out of order on the channel
            should_process: true,
            new_deliverable: VecDeque::new(),
        }
    }
}

impl StreamEvents {
    fn new(event: StreamEvent) -> Self {
        let mut new = Self::default();
        new.add_event(event);
        new
    }

    /// returns true if this is a new event.
    fn add_event(&mut self, event: StreamEvent) -> bool {
        let cid = match &event {
            StreamEvent::InitEvent(cid) => {
                self.update_should_process_for_new_delivered(cid);
                *cid
            }
            StreamEvent::KnownDeliverable(meta) => {
                self.prev_map.insert(meta.prev, meta.cid);
                self.update_should_process_for_new_delivered(&meta.cid);
                meta.cid
            }
            StreamEvent::Undelivered(meta) => {
                self.prev_map.insert(meta.prev, meta.cid);
                if !self.should_process {
                    // we depend on something in memory
                    self.should_process = self.prev_map.contains_key(&meta.prev);
                }
                meta.cid
            }
        };

        self.cid_map.insert(cid, event).is_none()
    }

    /// we'll be processed if something in memory depends on this event
    fn update_should_process_for_new_delivered(&mut self, new_cid: &EventCid) {
        // don't reset the flag if we're already supposed to be processed
        if !self.should_process {
            self.should_process = self.prev_map.contains_key(new_cid);
        }
    }

    /// Called when we've persisted the deliverable events to the database and can clean up our state.
    /// Returns `true` if we're finished processing and can be dropped from memory.
    /// Returns `false` if we have more work to do and should be retained for future processing
    fn processing_completed(&mut self) -> bool {
        self.should_process = false;
        self.new_deliverable.clear();
        !self
            .cid_map
            .iter()
            .any(|(_, ev)| matches!(ev, StreamEvent::Undelivered(_)))
    }

    /// When we discover the prev event is deliverable, we can mark ourselves as deliverable.
    /// This adds us to the queue to insert in the database and updates our state to deliverable.
    fn discovered_deliverable_prev(&mut self, cid: EventCid) {
        self.new_deliverable.push_back(cid);
        let event = self
            .cid_map
            .get_mut(&cid)
            .expect("Must have event in cid_map");
        match event {
            StreamEvent::InitEvent(cid)
            | StreamEvent::KnownDeliverable(StreamEventMetadata { cid, .. }) => {
                unreachable!(
                    "should not have found a deliverable event in our undelivered queue: {}",
                    cid,
                )
            }
            StreamEvent::Undelivered(meta) => {
                // we're deliverable now
                *self.cid_map.get_mut(&cid).unwrap() = StreamEvent::KnownDeliverable(meta.clone());
            }
        }
    }

    async fn order_events(&mut self, pool: &SqlitePool) -> Result<()> {
        // We collect everything we can into memory and then order things.
        // If our prev is deliverable then we can mark ourselves as deliverable. If our prev wasn't deliverable yet,
        // we track it and repeat (i.e. add it to our state and the set we're iterating to attempt to load its prev).
        // We mutate out state as we go adding things to the queue and changing their known deliverability so that
        // if we get canceled while querying the database, we can pick up where we left off. Our queue will still
        // have all the events in the order they need to be inserted, and the cid_map state will reflect their deliverability.
        let mut undelivered_q =
            VecDeque::from_iter(self.cid_map.iter().filter_map(|(cid, ev)| match ev {
                StreamEvent::Undelivered(meta) => {
                    debug_assert_eq!(meta.cid, *cid);
                    Some(meta.clone())
                }
                _ => None,
            }));

        debug!(count=%undelivered_q.len(), "undelivered events to process");

        while let Some(StreamEventMetadata {
            cid: undelivered_cid,
            prev: desired_prev,
        }) = undelivered_q.pop_front()
        {
            if let Some(known_prev) = self.cid_map.get(&desired_prev) {
                if known_prev.is_deliverable() {
                    trace!(
                        %undelivered_cid,
                        %desired_prev,
                        "Found event whose prev is already in memory and IS deliverable!"
                    );
                    self.discovered_deliverable_prev(undelivered_cid);
                } else {
                    trace!(
                        %undelivered_cid,
                        %desired_prev,
                        "Found event whose prev is already in memory but NOT deliverable."
                    );
                    // nothing to do until it arrives on the channel
                }
            } else if let Some(discovered_prev) =
                StreamEvent::load_by_cid(pool, desired_prev).await?
            {
                match &discovered_prev {
                    // we found our prev in the database and it's deliverable, so we're deliverable now
                    StreamEvent::InitEvent(_) | StreamEvent::KnownDeliverable(_) => {
                        self.discovered_deliverable_prev(undelivered_cid);
                    }
                    // it's not deliverable yet so we add track it and append it to the queue we're iterating to search for its prev.
                    // if we follow this chain to something deliverable in this loop, the values we have in memory will be updated in the final loop at the end.
                    StreamEvent::Undelivered(prev_meta) => {
                        undelivered_q.push_back(StreamEventMetadata {
                            cid: prev_meta.cid,
                            prev: prev_meta.prev,
                        });
                        self.add_event(discovered_prev);
                    }
                }
            } else {
                trace!("Found event that depends on another event we haven't discovered yet");
            }
        }

        let mut newly_ready = self.new_deliverable.clone();
        while let Some(cid) = newly_ready.pop_front() {
            if let Some(now_ready) = self.prev_map.get(&cid) {
                let ev = self
                    .cid_map
                    .get(now_ready)
                    .expect("must have value in cid_map if it's in prev_map")
                    .to_owned();
                match ev {
                    StreamEvent::InitEvent(cid) => {
                        unreachable!("should not have found an undelivered init event and added it to our delivery queue {}", cid);
                    }
                    StreamEvent::KnownDeliverable(_) => {
                        // This is fine as we could have already discovered and added ourself to the queue above.
                        // We get marked as KnownDeliverable in that case and we don't have anything more to do.
                    }
                    StreamEvent::Undelivered(meta) => {
                        // Discovering this event's prev (and therefore this event) as deliverable might have unlocked
                        // something else, so we add it to the back of the queue to check.
                        newly_ready.push_back(meta.cid);
                        self.discovered_deliverable_prev(meta.cid);
                    }
                }
            }
        }
        debug!(count=%self.new_deliverable.len(), "deliverable events discovered");
        Ok(())
    }
}

#[derive(Debug)]
pub struct OrderingState {
    pending_by_stream: HashMap<StreamCid, StreamEvents>,
    deliverable: VecDeque<EventCid>,
}

impl OrderingState {
    fn new() -> Self {
        Self {
            pending_by_stream: HashMap::new(),
            deliverable: VecDeque::new(),
        }
    }

    /// Add a stream to the list of streams to process.
    fn add_inserted_events(&mut self, events: Vec<InsertableBodyWithMeta>) {
        for ev in events {
            let stream_cid = ev.metadata.stream_cid();
            let event = ev.into();
            self.add_stream_event(stream_cid, event);
        }
    }

    /// Add an event to the list of events to process. Only creates a new stream to track if it's an undelivered event.
    /// We ignore delivered events for streams we're not tracking as we can look them up later if we need them.
    /// As we get lots of init events, we don't want them to be stuck in memory unless we have a reason to track them.
    fn add_stream_event(&mut self, stream_cid: StreamCid, event: StreamEvent) {
        if let Some(stream) = self.pending_by_stream.get_mut(&stream_cid) {
            stream.add_event(event);
        } else if matches!(event, StreamEvent::Undelivered(_)) {
            let stream = StreamEvents::new(event);
            self.pending_by_stream.insert(stream_cid, stream);
        }
    }

    /// Process every stream we know about that has undelivered events that should be "unlocked" now. This could be adjusted to commit things in batches,
    /// but for now it assumes it can process all the streams and events in one go. It should be idempotent, so if it fails, it can be retried.
    async fn process_streams(&mut self, pool: &SqlitePool) -> Result<()> {
        for (_stream_cid, stream_events) in self.pending_by_stream.iter_mut() {
            if stream_events.should_process {
                stream_events.order_events(pool).await?;
                self.deliverable
                    .extend(stream_events.new_deliverable.iter());
            }
        }

        match self.persist_ready_events(pool).await {
            Ok(_) => {}
            Err(err) => {
                // Clear the queue as we'll rediscover it on the next run, rather than try to double update everything.
                // We will no-op the updates so it doesn't really hurt but it's unnecessary.
                // The StreamEvents in our pending_by_stream map all have their state updated in memory so we can pick up where we left off.
                self.deliverable.clear();
                return Err(err);
            }
        }
        // keep things that still have missing history but don't process them again until we get something new
        self.pending_by_stream
            .retain(|_, stream_events| !stream_events.processing_completed());

        debug!(remaining_streams=%self.pending_by_stream.len(), "Finished processing streams");
        trace!(stream_state=?self, "Finished processing streams");

        Ok(())
    }

    /// Process all undelivered events in the database. This is a blocking operation that could take a long time.
    /// It is intended to be run at startup but could be used on an interval or after some errors to recover.
    pub(crate) async fn process_all_undelivered_events(
        pool: &SqlitePool,
        max_iterations: usize,
        batch_size: u32,
    ) -> Result<usize> {
        let mut state = Self::new();
        let mut iter_cnt = 0;
        let mut event_cnt = 0;
        let mut highwater = 0;
        while iter_cnt < max_iterations {
            iter_cnt += 1;
            let (undelivered, new_hw) =
                CeramicOneEvent::undelivered_with_values(pool, batch_size.into(), highwater)
                    .await?;
            highwater = new_hw;
            let found_something = !undelivered.is_empty();
            let found_everything = undelivered.len() < batch_size as usize;
            if found_something {
                // We can start processing and we'll follow the stream history if we have it. In that case, we either arrive
                // at the beginning and mark them all delivered, or we find a gap and stop processing and leave them in memory.
                // In this case, we won't discover them until we start running recon with a peer, so maybe we should drop them
                // or otherwise mark them ignored somehow. When this function ends, we do drop everything so for now it's probably okay.
                event_cnt += state
                    .process_undelivered_events_batch(pool, undelivered)
                    .await?;
            }
            if !found_something || found_everything {
                break;
            }
        }
        if iter_cnt > max_iterations {
            info!(%batch_size, iterations=%iter_cnt, "Exceeded max iterations for finding undelivered events!");
        }

        Ok(event_cnt)
    }

    async fn process_undelivered_events_batch(
        &mut self,
        pool: &SqlitePool,
        event_data: Vec<(EventId, Vec<u8>)>,
    ) -> Result<usize> {
        trace!(cnt=%event_data.len(), "Processing undelivered events batch");
        let mut event_cnt = 0;
        for (event_id, carfile) in event_data {
            let event_cid = event_id.cid().ok_or_else(|| {
                Error::new_invalid_arg(anyhow::anyhow!("EventID is missing a CID: {}", event_id))
            })?;

            let loaded = CeramicEventService::parse_event_carfile_cid(event_cid, &carfile).await?;

            let event = match &loaded.metadata {
                EventMetadata::Init { cid, .. } => {
                    unreachable!("Init events should not be undelivered. CID={}", cid);
                }
                EventMetadata::Data { cid, prev, .. } | EventMetadata::Time { cid, prev, .. } => {
                    StreamEvent::Undelivered(StreamEventMetadata::new(*cid, *prev))
                }
            };

            event_cnt += 1;
            self.add_stream_event(loaded.metadata.stream_cid(), event);
        }
        self.process_streams(pool).await?;

        Ok(event_cnt)
    }

    /// We should improve the error handling and likely add some batching if the number of ready events is very high.
    /// We copy the events up front to avoid losing any events if the task is cancelled.
    async fn persist_ready_events(&mut self, pool: &SqlitePool) -> Result<()> {
        if !self.deliverable.is_empty() {
            tracing::debug!(count=%self.deliverable.len(), "Marking events as ready to deliver");
            let mut tx = pool.begin_tx().await?;
            // We process the ready events as a FIFO queue so they are marked delivered before events that were added after and depend on them.
            // Could use `pop_front` but we want to make sure we commit and then clear everything at once.
            for cid in &self.deliverable {
                CeramicOneEvent::mark_ready_to_deliver(&mut tx, cid).await?;
            }
            tx.commit().await?;
            self.deliverable.clear();
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use ceramic_store::EventInsertable;
    use test_log::test;

    use crate::tests::get_n_events;

    use super::*;

    async fn get_n_insertable_events(n: usize) -> Vec<EventInsertable> {
        let mut res = Vec::with_capacity(n);
        let events = get_n_events(n).await;
        for event in events {
            let (event, _) =
                CeramicEventService::parse_event_carfile_order_key(event.0.to_owned(), &event.1)
                    .await
                    .unwrap();
            res.push(event);
        }
        res
    }

    #[test(tokio::test)]
    async fn test_undelivered_batch_empty() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let processed = OrderingState::process_all_undelivered_events(&pool, 10, 100)
            .await
            .unwrap();
        assert_eq!(0, processed);
    }

    async fn insert_10_with_9_undelivered(pool: &SqlitePool) {
        let insertable = get_n_insertable_events(10).await;
        let init = insertable.first().unwrap().to_owned();
        let undelivered = insertable.into_iter().skip(1).collect::<Vec<_>>();

        let new = CeramicOneEvent::insert_many(pool, &undelivered[..])
            .await
            .unwrap();

        assert_eq!(9, new.inserted.len());
        assert_eq!(0, new.inserted.iter().filter(|e| e.deliverable).count());

        let new = CeramicOneEvent::insert_many(pool, &[init]).await.unwrap();
        assert_eq!(1, new.inserted.len());
        assert_eq!(1, new.inserted.iter().filter(|e| e.deliverable).count());
    }

    #[test(tokio::test)]
    async fn test_undelivered_batch_offset() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();

        insert_10_with_9_undelivered(&pool).await;
        let (_, events) = CeramicOneEvent::new_events_since_value(&pool, 0, 100)
            .await
            .unwrap();
        assert_eq!(1, events.len());

        let processed = OrderingState::process_all_undelivered_events(&pool, 1, 5)
            .await
            .unwrap();
        assert_eq!(5, processed);
        let (_, events) = CeramicOneEvent::new_events_since_value(&pool, 0, 100)
            .await
            .unwrap();
        assert_eq!(6, events.len());
        // the last 5 are processed and we have 10 delivered
        let processed = OrderingState::process_all_undelivered_events(&pool, 1, 5)
            .await
            .unwrap();
        assert_eq!(4, processed);
        let (_, events) = CeramicOneEvent::new_events_since_value(&pool, 0, 100)
            .await
            .unwrap();
        assert_eq!(10, events.len());

        // nothing left
        let processed = OrderingState::process_all_undelivered_events(&pool, 1, 100)
            .await
            .unwrap();

        assert_eq!(0, processed);
    }

    #[test(tokio::test)]
    async fn test_undelivered_batch_iterations_ends_early() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        // create 5 streams with 9 undelivered events each
        insert_10_with_9_undelivered(&pool).await;
        insert_10_with_9_undelivered(&pool).await;
        insert_10_with_9_undelivered(&pool).await;
        insert_10_with_9_undelivered(&pool).await;
        insert_10_with_9_undelivered(&pool).await;

        let (_hw, event) = CeramicOneEvent::new_events_since_value(&pool, 0, 1000)
            .await
            .unwrap();
        assert_eq!(5, event.len());
        let _res = OrderingState::process_all_undelivered_events(&pool, 4, 10)
            .await
            .unwrap();

        let (_hw, event) = CeramicOneEvent::new_events_since_value(&pool, 0, 1000)
            .await
            .unwrap();
        assert_eq!(45, event.len());
    }

    #[test(tokio::test)]
    async fn test_undelivered_batch_iterations_ends_when_all_found() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        // create 5 streams with 9 undelivered events each
        insert_10_with_9_undelivered(&pool).await;
        insert_10_with_9_undelivered(&pool).await;
        insert_10_with_9_undelivered(&pool).await;
        insert_10_with_9_undelivered(&pool).await;
        insert_10_with_9_undelivered(&pool).await;

        let (_hw, event) = CeramicOneEvent::new_events_since_value(&pool, 0, 1000)
            .await
            .unwrap();
        assert_eq!(5, event.len());
        let _res = OrderingState::process_all_undelivered_events(&pool, 100_000_000, 5)
            .await
            .unwrap();

        let (_hw, event) = CeramicOneEvent::new_events_since_value(&pool, 0, 1000)
            .await
            .unwrap();
        assert_eq!(50, event.len());
    }

    #[test(tokio::test)]
    async fn test_process_all_undelivered_one_batch() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        // create 5 streams with 9 undelivered events each
        insert_10_with_9_undelivered(&pool).await;
        insert_10_with_9_undelivered(&pool).await;
        insert_10_with_9_undelivered(&pool).await;
        insert_10_with_9_undelivered(&pool).await;
        insert_10_with_9_undelivered(&pool).await;

        let (_hw, event) = CeramicOneEvent::new_events_since_value(&pool, 0, 1000)
            .await
            .unwrap();
        assert_eq!(5, event.len());
        let _res = OrderingState::process_all_undelivered_events(&pool, 1, 100)
            .await
            .unwrap();

        let (_hw, event) = CeramicOneEvent::new_events_since_value(&pool, 0, 1000)
            .await
            .unwrap();
        assert_eq!(50, event.len());
    }
}
