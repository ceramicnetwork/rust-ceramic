use std::collections::{HashMap, VecDeque};

use ceramic_core::EventId;
use ceramic_store::{CeramicOneEvent, SqlitePool};
use cid::Cid;
use tracing::{debug, error, info, trace, warn};

use crate::{CeramicEventService, Error, Result};

use super::service::{EventHeader, InsertableBodyWithHeader};

/// How many events to select at once to see if they've become deliverable when we have downtime
/// Used at startup and occassionally in case we ever dropped something
/// We keep the number small for now as we may need to traverse many prevs for each one of these and load them into memory.
const DELIVERABLE_EVENTS_BATCH_SIZE: u32 = 1000;

/// How many batches of undelivered events are we willing to process on start up?
/// To avoid an infinite loop. It's going to take a long time to process `DELIVERABLE_EVENTS_BATCH_SIZE * MAX_ITERATIONS` events
const MAX_ITERATIONS: usize = 100_000_000;

type StreamCid = Cid;
type EventCid = Cid;
type PrevCid = Cid;

#[derive(Debug)]
pub struct DeliverableTask {
    pub(crate) _handle: tokio::task::JoinHandle<()>,
    /// Currently events discovered over recon that are out of order and need to be marked ready (deliverable)
    /// when their prev chain is discovered and complete (i.e. my prev is deliverable then I am deliverable).
    pub(crate) tx_inserted: tokio::sync::mpsc::Sender<InsertableBodyWithHeader>,
}

#[derive(Debug)]
pub struct OrderingTask {}

impl OrderingTask {
    pub async fn run(pool: SqlitePool, q_depth: usize, load_delivered: bool) -> DeliverableTask {
        let (tx_inserted, rx_inserted) =
            tokio::sync::mpsc::channel::<InsertableBodyWithHeader>(q_depth);

        let handle =
            tokio::spawn(async move { Self::run_loop(pool, load_delivered, rx_inserted).await });

        DeliverableTask {
            _handle: handle,
            tx_inserted,
        }
    }

    async fn run_loop(
        pool: SqlitePool,
        load_undelivered: bool,
        mut rx_inserted: tokio::sync::mpsc::Receiver<InsertableBodyWithHeader>,
    ) {
        // before starting, make sure we've updated any events in the database we missed
        // this could take a long time. possibly we want to put it in another task so we can start processing events immediately
        let mut state = OrderingState::new();
        if load_undelivered
            && state
                .process_all_undelivered_events(
                    &pool,
                    MAX_ITERATIONS,
                    DELIVERABLE_EVENTS_BATCH_SIZE,
                )
                .await
                .map_err(Self::log_error)
                .is_err()
        {
            return;
        }

        loop {
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
            } else if rx_inserted.is_closed() {
                debug!(
                "Server dropped the delivered events channel. Attempting to processing streams in memory once more before exiting."
                );

                if state
                    .process_streams(&pool)
                    .await
                    .map_err(Self::log_error)
                    .is_err()
                {
                    return;
                }
                break;
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum StreamEvent {
    InitEvent(EventCid),
    /// An event that is known to be deliverable from the database
    KnownDeliverable(StreamEventMetadata),
    /// An event that needs more history to be deliverable
    Undelivered(StreamEventMetadata),
}

impl StreamEvent {
    /// Builds a stream event from the database if it exists.
    async fn load_by_cid(pool: &SqlitePool, cid: EventCid) -> Result<Option<Self>> {
        // TODO: one query
        let (exists, deliverable) = CeramicOneEvent::deliverable_by_cid(pool, &cid).await?;
        if exists {
            let parsed_body =
                if let Some(body) = CeramicEventService::load_by_cid(pool, cid).await? {
                    body
                } else {
                    warn!(%cid, "No event body found for event that should exist");
                    return Ok(None);
                };

            let known_prev = match &parsed_body.header {
                EventHeader::Init { cid, .. } => {
                    if !deliverable {
                        warn!(%cid,"Found init event in database that wasn't previously marked as deliverable. Updating now...");
                        let mut tx = pool.begin_tx().await?;
                        CeramicOneEvent::mark_ready_to_deliver(&mut tx, cid).await?;
                        tx.commit().await?;
                    }
                    StreamEvent::InitEvent(*cid)
                }
                EventHeader::Data { prev, .. } | EventHeader::Time { prev, .. } => {
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

impl From<InsertableBodyWithHeader> for StreamEvent {
    fn from(ev: InsertableBodyWithHeader) -> Self {
        match ev.header {
            EventHeader::Init { cid, .. } => StreamEvent::InitEvent(cid),
            EventHeader::Data { cid, prev, .. } | EventHeader::Time { cid, prev, .. } => {
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

#[derive(Debug, Clone, Default)]
/// ~540 bytes per event in this struct
pub(crate) struct StreamEvents {
    /// Map of `event.prev` to `event.cid` to find the previous event easily.
    prev_map: HashMap<PrevCid, EventCid>,
    /// Map of `event.cid` to `metadata` for quick lookup of the event metadata.
    cid_map: HashMap<EventCid, StreamEvent>,
    /// whether we should process this stream because new events have been added
    skip_processing: bool,
    /// The newly discovered events that are deliverable and should be processed.
    new_deliverable: VecDeque<EventCid>,
}

impl StreamEvents {
    fn new(event: StreamEvent) -> Self {
        let mut new = Self::default();
        new.add_event(event);
        new
    }

    // we'll be processed if something in memory depends on this event
    fn update_should_process_for_new_delivered(&mut self, new_cid: &EventCid) {
        // don't reset a true flag to false
        if self.skip_processing {
            self.skip_processing = !self.prev_map.contains_key(new_cid);
        }
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
                if self.skip_processing {
                    // we depend on something in memory
                    self.skip_processing = !self.prev_map.contains_key(&meta.prev)
                }
                meta.cid
            }
        };

        self.cid_map.insert(cid, event).is_none()
    }

    fn remove_by_prev_cid(&mut self, prev: &Cid) -> Option<EventCid> {
        if let Some(cid) = self.prev_map.remove(prev) {
            self.cid_map.remove(&cid);
            Some(cid)
        } else {
            None
        }
    }

    /// Called when we've persisted the deliverable events to the database and can clean up our state.
    /// Returns true if we should be retained for future processing (i.e we have more we need to discover)
    /// and false if we can be dropped from memory.
    fn completed_processing(&mut self) -> bool {
        self.skip_processing = true;

        for cid in self.new_deliverable.iter() {
            if let Some(ev) = self.cid_map.get_mut(cid) {
                match ev {
                    StreamEvent::InitEvent(_) => {}
                    StreamEvent::KnownDeliverable(_) => {
                        warn!(
                                ?ev,
                                "Found event in deliverable queue that was already marked as deliverable."
                            )
                    }
                    StreamEvent::Undelivered(meta) => {
                        // we're delivered now
                        *ev = StreamEvent::KnownDeliverable(meta.clone());
                    }
                }
            }
        }
        self.new_deliverable.clear();
        self.cid_map
            .iter()
            .any(|(_, ev)| matches!(ev, StreamEvent::Undelivered(_)))
    }

    async fn order_events(&mut self, pool: &SqlitePool) -> Result<()> {
        // We collect everything we can into memory and then order things.
        // If our prev is the init event or already been delivered, we can mark ourselves as deliverable.
        // If our prev wasn't deliverable yet, we track it and repeat (i.e. follow its prev if we don't have it)

        let mut deliverable_queue = VecDeque::new();
        let mut undelivered =
            VecDeque::from_iter(self.cid_map.iter().filter_map(|(cid, ev)| match ev {
                StreamEvent::Undelivered(meta) => {
                    assert_eq!(meta.cid, *cid);
                    Some((meta.cid, meta.prev))
                }
                _ => None,
            }));

        while let Some((cid, prev)) = undelivered.pop_front() {
            if let Some(prev_event) = self.cid_map.get(&prev) {
                match prev_event {
                    StreamEvent::InitEvent(_) | StreamEvent::KnownDeliverable(_) => {
                        trace!(
                            %prev,
                            %cid,
                            "Found event whose prev is already in memory and IS deliverable!"
                        );
                        deliverable_queue.push_back(cid)
                    }
                    StreamEvent::Undelivered(_) => {
                        trace!(
                            %prev,
                            %cid,
                            "Found event whose prev is already in memory but NOT deliverable."
                        );
                        // nothing to do until it arrives on the channel
                    }
                }

                continue;
            }

            let prev_event = StreamEvent::load_by_cid(pool, prev).await?;
            if let Some(known_prev) = prev_event {
                match &known_prev {
                    StreamEvent::InitEvent(_) | StreamEvent::KnownDeliverable(_) => {
                        deliverable_queue.push_back(cid);
                    }
                    StreamEvent::Undelivered(undelivered_ev) => {
                        // we'll try to follow this back to something deliverable
                        undelivered.push_back((undelivered_ev.cid, undelivered_ev.prev));
                    }
                }
                self.add_event(known_prev);
            } else {
                trace!("Found event that depends on another event we haven't discovered yet");
            }
        }

        let mut newly_ready = deliverable_queue.clone();
        while let Some(cid) = newly_ready.pop_front() {
            if let Some(now_ready_ev) = self.remove_by_prev_cid(&cid) {
                if let Some(ev) = self.cid_map.get(&now_ready_ev) {
                    match ev {
                        StreamEvent::InitEvent(_) | StreamEvent::KnownDeliverable(_) => {
                            warn!(?ev, "should not have found a deliverable event when we expected only undelivered events!");
                        }
                        StreamEvent::Undelivered(_) => {
                            newly_ready.push_back(now_ready_ev);
                        }
                    }
                }
                deliverable_queue.push_back(now_ready_ev);
                newly_ready.push_back(now_ready_ev);
            }
        }
        self.new_deliverable = deliverable_queue;
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
    /// We ignore delivered events for streams we're not tracking as we can look them up later if we need them.
    /// We will get lots of init events we can ignore unless we need them, otherwise they'll be stuck in memory for a long time.
    fn add_inserted_events(&mut self, events: Vec<InsertableBodyWithHeader>) {
        for ev in events {
            let stream_cid = ev.header.stream_cid();
            let event = ev.into();
            self.add_stream_event(stream_cid, event);
        }
    }

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
            if stream_events.skip_processing {
                continue;
            }
            stream_events.order_events(pool).await?;
            self.deliverable
                .extend(stream_events.new_deliverable.iter());
        }

        self.persist_ready_events(pool).await?;
        // keep things that still have missing history but don't process them again until we get something new
        self.pending_by_stream
            .retain(|_, stream_events| stream_events.completed_processing());

        debug!(remaining_streams=%self.pending_by_stream.len(), "Finished processing streams");
        trace!(stream_state=?self, "Finished processing streams");

        Ok(())
    }

    /// Process all undelivered events in the database. This is a blocking operation that could take a long time.
    /// It is intended to be run at startup but could be used on an interval or after some errors to recover.
    pub(crate) async fn process_all_undelivered_events(
        &mut self,
        pool: &SqlitePool,
        max_iterations: usize,
        batch_size: u32,
    ) -> Result<usize> {
        let mut iter_cnt = 0;
        let mut event_cnt = 0;
        let mut highwater = 0;
        while iter_cnt < max_iterations {
            iter_cnt += 1;
            let (undelivered, new_hw) =
                CeramicOneEvent::undelivered_with_values(pool, batch_size.into(), highwater)
                    .await?;
            highwater = new_hw;
            if undelivered.is_empty() {
                break;
            } else {
                // We can start processing and we'll follow the stream history if we have it. In that case, we either arrive
                // at the beginning and mark them all delivered, or we find a gap and stop processing and leave them in memory.
                // In this case, we won't discover them until we start running recon with a peer, so maybe we should drop them
                // or otherwise mark them ignored somehow.
                let found_all = undelivered.len() < batch_size as usize;
                event_cnt += self
                    .process_undelivered_events_batch(pool, undelivered)
                    .await?;
                if found_all {
                    break;
                }
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
        let mut to_store_asap = Vec::new();
        let mut event_cnt = 0;
        for (event_id, carfile) in event_data {
            let event_cid = event_id.cid().ok_or_else(|| {
                Error::new_invalid_arg(anyhow::anyhow!("EventID is missing a CID: {}", event_id))
            })?;

            let loaded = CeramicEventService::parse_event_carfile_cid(event_cid, &carfile).await?;

            let event = match &loaded.header {
                EventHeader::Init { cid, .. } => {
                    warn!(%cid,"Found init event in database that wasn't previously marked as deliverable. Updating now...");
                    to_store_asap.push(*cid);
                    StreamEvent::InitEvent(*cid)
                }
                EventHeader::Data { cid, prev, .. } | EventHeader::Time { cid, prev, .. } => {
                    StreamEvent::Undelivered(StreamEventMetadata::new(*cid, *prev))
                }
            };

            event_cnt += 1;
            self.add_stream_event(loaded.header.stream_cid(), event);
        }

        if !to_store_asap.is_empty() {
            info!("storing init events that were somehow missed previously");
            let mut tx = pool.begin_tx().await?;
            for cid in to_store_asap {
                CeramicOneEvent::mark_ready_to_deliver(&mut tx, &cid).await?;
            }
            tx.commit().await?;
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
        let processed = OrderingState::new()
            .process_all_undelivered_events(&pool, 10, 100)
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

        let processed = OrderingState::new()
            .process_all_undelivered_events(&pool, 1, 5)
            .await
            .unwrap();
        assert_eq!(5, processed);
        let (_, events) = CeramicOneEvent::new_events_since_value(&pool, 0, 100)
            .await
            .unwrap();
        assert_eq!(6, events.len());
        // the last 5 are processed and we have 10 delivered
        let processed = OrderingState::new()
            .process_all_undelivered_events(&pool, 1, 5)
            .await
            .unwrap();
        assert_eq!(4, processed);
        let (_, events) = CeramicOneEvent::new_events_since_value(&pool, 0, 100)
            .await
            .unwrap();
        assert_eq!(10, events.len());

        // nothing left
        let processed = OrderingState::new()
            .process_all_undelivered_events(&pool, 1, 100)
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
        let mut state = OrderingState::new();
        state
            .process_all_undelivered_events(&pool, 4, 10)
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
        let mut state = OrderingState::new();
        state
            .process_all_undelivered_events(&pool, 100_000_000, 5)
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
        let mut state = OrderingState::new();
        state
            .process_all_undelivered_events(&pool, 1, 100)
            .await
            .unwrap();

        let (_hw, event) = CeramicOneEvent::new_events_since_value(&pool, 0, 1000)
            .await
            .unwrap();
        assert_eq!(50, event.len());
    }
}
