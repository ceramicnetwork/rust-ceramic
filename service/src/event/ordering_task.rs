use std::collections::{HashMap, HashSet, VecDeque};

use ceramic_store::{
    CeramicOneEvent, CeramicOneStream, InsertedEvent, SqlitePool, StreamEventMetadata,
};
use cid::Cid;
use tracing::{debug, error, info, trace, warn};

use crate::{Error, Result};

type StreamCid = Cid;
type EventCid = Cid;
type PrevCid = Cid;

#[derive(Debug)]
pub struct DeliverableTask {
    pub(crate) _handle: tokio::task::JoinHandle<()>,
    pub(crate) tx_delivered: tokio::sync::mpsc::Sender<InsertedEvent>,
}

#[derive(Debug)]
pub struct OrderingTask {}

impl OrderingTask {
    pub async fn run(pool: SqlitePool, q_depth: usize, load_delivered: bool) -> DeliverableTask {
        let (tx_delivered, rx_delivered) = tokio::sync::mpsc::channel::<InsertedEvent>(q_depth);

        let handle =
            tokio::spawn(async move { Self::run_loop(pool, load_delivered, rx_delivered).await });

        DeliverableTask {
            _handle: handle,
            tx_delivered,
        }
    }

    async fn run_loop(
        pool: SqlitePool,
        load_undelivered: bool,
        mut rx_delivered: tokio::sync::mpsc::Receiver<InsertedEvent>,
    ) {
        // before starting, make sure we've updated any events in the database we missed
        let mut state = OrderingState::new();
        if load_undelivered
            && state
                .process_all_undelivered_events(&pool)
                .await
                .map_err(Self::log_error)
                .is_err()
        {
            return;
        }

        loop {
            let mut delivered_events = Vec::with_capacity(100);

            if rx_delivered.recv_many(&mut delivered_events, 100).await > 0 {
                debug!(?delivered_events, "new delivered events!");
                for event in delivered_events {
                    state.add_stream(event.stream_cid);
                }

                if state
                    .process_streams(&pool)
                    .await
                    .map_err(Self::log_error)
                    .is_err()
                {
                    return;
                }
            } else if rx_delivered.is_closed() {
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

#[derive(Debug, Clone, Default)]
/// ~540 bytes per event in this struct
pub(crate) struct StreamEvents {
    /// Map of `event.prev` to `event.cid` for quick lookup of the next event in the stream.
    prev_map: HashMap<PrevCid, EventCid>,
    /// Map of `event.cid` to `metadata` for quick lookup of the event metadata.
    cid_map: HashMap<EventCid, StreamEventMetadata>,
    /// Events that can be delivered FIFO order for the stream
    deliverable: VecDeque<EventCid>,
    /// The total number of events in the stream when we started
    total_events: usize,
}

impl StreamEvents {
    fn new<I>(_cid: StreamCid, events: I) -> Self
    where
        I: ExactSizeIterator<Item = StreamEventMetadata>,
    {
        let total_events = events.len();
        let mut new = Self {
            prev_map: HashMap::with_capacity(total_events),
            cid_map: HashMap::with_capacity(total_events),
            deliverable: VecDeque::with_capacity(total_events),
            total_events,
        };

        for event in events {
            new.add_event(event);
        }
        new
    }

    async fn new_from_db(stream: StreamCid, pool: &SqlitePool) -> Result<Self> {
        let stream_events = CeramicOneStream::load_stream_events(pool, stream).await?;
        trace!(?stream_events, "Loaded stream events for ordering");
        Ok(Self::new(stream, stream_events.into_iter()))
    }

    fn is_empty(&self) -> bool {
        // We only care if we have things that are pending to be delivered
        self.prev_map.is_empty()
    }

    /// returns true if this is a new event.
    fn add_event(&mut self, event: StreamEventMetadata) -> bool {
        if let Some(prev) = event.prev {
            self.prev_map.insert(prev, event.cid);
        }
        self.cid_map.insert(event.cid, event).is_none()
    }

    fn remove_by_event_cid(&mut self, cid: &Cid) -> Option<StreamEventMetadata> {
        if let Some(ev) = self.cid_map.remove(cid) {
            if let Some(prev) = ev.prev {
                self.prev_map.remove(&prev);
            }
            Some(ev)
        } else {
            None
        }
    }

    fn remove_by_prev_cid(&mut self, prev: &Cid) -> Option<EventCid> {
        if let Some(cid) = self.prev_map.remove(prev) {
            self.cid_map.remove(&cid);
            Some(cid)
        } else {
            None
        }
    }

    fn delivered_events(&self) -> impl Iterator<Item = &EventCid> {
        self.cid_map
            .iter()
            .filter_map(|(cid, event)| if event.deliverable { Some(cid) } else { None })
    }

    async fn order_events(pool: &SqlitePool, stream: StreamCid) -> Result<Self> {
        let mut to_process = Self::new_from_db(stream, pool).await?;
        if to_process.delivered_events().count() == 0 {
            return Ok(to_process);
        }

        let stream_event_count = to_process.cid_map.len();
        let delivered_cids = to_process.delivered_events().cloned().collect::<Vec<_>>();
        let mut start_with = VecDeque::with_capacity(stream_event_count - delivered_cids.len());

        for cid in delivered_cids {
            if let Some(next_event) = to_process.remove_by_prev_cid(&cid) {
                to_process.remove_by_event_cid(&next_event);
                start_with.push_back(next_event);
            }
        }

        while let Some(new_tip) = start_with.pop_front() {
            to_process.deliverable.push_back(new_tip);
            let mut tip = new_tip;
            while let Some(next_event) = to_process.remove_by_prev_cid(&tip) {
                to_process.deliverable.push_back(next_event);
                tip = next_event;
            }
        }
        Ok(to_process)
    }
}

#[derive(Debug)]
pub struct OrderingState {
    streams: HashSet<StreamCid>,
    deliverable: VecDeque<EventCid>,
}

impl OrderingState {
    fn new() -> Self {
        Self {
            streams: HashSet::new(),
            deliverable: VecDeque::new(),
        }
    }

    /// Add a stream to the list of streams to process. This implies it has undelivered events and is worthwhile to attempt.
    fn add_stream(&mut self, stream: StreamCid) -> bool {
        self.streams.insert(stream)
    }

    /// Process every stream we know about that has undelivered events that should be "unlocked" now. This could be adjusted to process commit things in batches,
    /// but for now it assumes it can process all the streams and events in one go. It should be idempotent, so if it fails, it can be retried. Events that are
    /// delivered multiple times will not change the original delivered state.
    async fn process_streams(&mut self, pool: &SqlitePool) -> Result<()> {
        let mut stream_cnt = HashMap::new();
        // we need to handle the fact that new writes can come in without knowing they're deliverable because we're still in the process of updating them.
        // so when we finish the loop and we had streams we couldn't complete, we try again to see if they had new writes. if nothing changed we exit.
        // this could certainly be optimized. we could only query the count of events, or we could load undelivered events and keep track of our
        // total state, as anything forking that was deliverable would arrive on the incoming channel. for now, streams are short and this is probably sufficient.
        loop {
            let mut processed_streams = Vec::with_capacity(self.streams.len());
            for stream in &self.streams {
                let ordered_events = StreamEvents::order_events(pool, *stream).await?;
                stream_cnt.insert(*stream, ordered_events.total_events);
                if ordered_events.is_empty() {
                    processed_streams.push(*stream);
                }
                self.deliverable.extend(ordered_events.deliverable);
            }

            let found_events = !self.deliverable.is_empty();
            if found_events {
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
            self.streams
                .retain(|stream| !processed_streams.contains(stream));
            // not strictly necessary as the next loop will not do anything but we can avoid allocating
            if self.streams.is_empty() || !found_events {
                break;
            }

            debug!(stream_state=?self, ?processed_streams, "Finished processing streams loop with more to do");
        }
        debug!(stream_state=?self, "Finished processing streams");

        Ok(())
    }

    /// Processes all streams with undelivered events returning the total number of streams identified. This is a recursive function that will
    /// continue to process streams until it finds no more streams with undelivered events. This is useful for bootstrapping the ordering task
    /// in case we missed/dropped something in the past. Anything we can't process now requires discovering from a peer, so there isn't really
    /// any advantage to keeping it in memory and trying again later.
    async fn process_all_undelivered_events(&mut self, pool: &SqlitePool) -> Result<usize> {
        tracing::trace!("Processing all undelivered events for ordering");

        let mut streams_discovered = 0;
        let mut resume_at = Some(0);
        while let Some(highwater_mark) = resume_at {
            let (cids, hw_mark) =
                CeramicOneStream::load_stream_cids_with_undelivered_events(pool, highwater_mark)
                    .await?;
            resume_at = hw_mark;
            trace!(count=cids.len(), stream_cids=?cids, "Discovered streams with undelivered events");
            for cid in cids {
                if self.add_stream(cid) {
                    streams_discovered += 1;
                }
            }
            self.process_streams(pool).await?;
        }

        Ok(streams_discovered)
    }
}

#[cfg(test)]
mod test {
    use ceramic_store::EventInsertable;

    use crate::tests::get_n_events;

    use super::*;

    #[tokio::test]
    async fn test_undelivered_batch_empty() {
        let _ = ceramic_metrics::init_local_tracing();
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let total = OrderingState::new()
            .process_all_undelivered_events(&pool)
            .await
            .unwrap();
        assert_eq!(0, total);
    }

    #[tokio::test]
    async fn test_undelivered_streams_all() {
        let _ = ceramic_metrics::init_local_tracing();
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let s1_events = get_n_events(3).await;
        let s2_events = get_n_events(5).await;
        let mut all_insertable = Vec::with_capacity(8);

        for event in s1_events.iter() {
            let insertable = EventInsertable::try_new(event.0.to_owned(), &event.1)
                .await
                .unwrap();
            let expected_deliverable = insertable.deliverable();
            let res = CeramicOneEvent::insert_many(&pool, &[insertable.clone()])
                .await
                .unwrap();
            assert_eq!(expected_deliverable, res.inserted[0].deliverable);

            all_insertable.push(insertable);
        }
        for event in s2_events.iter() {
            let insertable = EventInsertable::try_new(event.0.to_owned(), &event.1)
                .await
                .unwrap();
            let expected_deliverable = insertable.deliverable();
            let res = CeramicOneEvent::insert_many(&pool, &[insertable.clone()])
                .await
                .unwrap();
            assert_eq!(expected_deliverable, res.inserted[0].deliverable);

            all_insertable.push(insertable);
        }

        for event in &all_insertable {
            let (_exists, delivered) = CeramicOneEvent::deliverable_by_cid(&pool, &event.cid())
                .await
                .unwrap();
            // init events are always delivered and the others should have been skipped
            if event.cid() == event.stream_cid()
                || event.order_key == s1_events[0].0
                || event.order_key == s2_events[0].0
            {
                assert!(
                    delivered,
                    "Event {:?} was not delivered. init={:?}, s1={:?}, s2={:?}",
                    event.cid(),
                    event.stream_cid(),
                    s1_events
                        .iter()
                        .map(|(e, _)| e.cid().unwrap())
                        .collect::<Vec<_>>(),
                    s2_events
                        .iter()
                        .map(|(e, _)| e.cid().unwrap())
                        .collect::<Vec<_>>(),
                );
            } else {
                assert!(!delivered);
            }
        }
        let total = OrderingState::new()
            .process_all_undelivered_events(&pool)
            .await
            .unwrap();

        assert_eq!(2, total);
        for event in &all_insertable {
            let (_exists, delivered) = CeramicOneEvent::deliverable_by_cid(&pool, &event.cid())
                .await
                .unwrap();
            assert!(delivered);
        }
    }
}
