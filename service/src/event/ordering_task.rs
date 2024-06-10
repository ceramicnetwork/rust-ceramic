use std::collections::{HashMap, HashSet, VecDeque};

use ceramic_store::{CandidateEvent, CeramicOneEvent, CeramicOneStream, SqlitePool, StreamCommit};
use cid::Cid;
use itertools::Itertools;
use tracing::{debug, error, info, trace, warn};

use crate::{Error, Result};

/// How often should we try to review our internal state to see if we missed something?
/// Should this query the database to try to discover any events? Probably add some "channel full" global flag so we
/// know to start querying the database for more events when we recover.
const CHECK_ALL_INTERVAL_SECONDS: u64 = 60 * 10; // 10 minutes

type StreamCid = Cid;
type EventCid = Cid;
type PrevCid = Cid;

#[derive(Debug)]
pub struct DeliverableTask {
    pub(crate) _handle: tokio::task::JoinHandle<()>,
    pub(crate) tx_delivered: tokio::sync::mpsc::Sender<CandidateEvent>,
    pub(crate) tx_stream_update: tokio::sync::mpsc::Sender<StreamCid>,
}

#[derive(Debug)]
pub struct OrderingTask {}

impl OrderingTask {
    pub async fn run(pool: SqlitePool, q_depth: usize, load_delivered: bool) -> DeliverableTask {
        let (tx_delivered, rx_delivered) = tokio::sync::mpsc::channel::<CandidateEvent>(q_depth);
        let (tx_stream_update, rx_stream_update) = tokio::sync::mpsc::channel::<StreamCid>(q_depth);

        let handle = tokio::spawn(async move {
            Self::run_loop(pool, load_delivered, rx_delivered, rx_stream_update).await
        });

        DeliverableTask {
            _handle: handle,
            tx_delivered,
            tx_stream_update,
        }
    }

    async fn run_loop(
        pool: SqlitePool,
        load_undelivered: bool,
        mut rx_delivered: tokio::sync::mpsc::Receiver<CandidateEvent>,
        mut rx_stream_update: tokio::sync::mpsc::Receiver<StreamCid>,
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

        let mut interval =
            tokio::time::interval(std::time::Duration::from_secs(CHECK_ALL_INTERVAL_SECONDS));
        loop {
            let mut delivered_events = Vec::with_capacity(100);
            let mut updated_streams = Vec::with_capacity(100);

            tokio::select! {
                _new = rx_delivered.recv_many(&mut delivered_events, 100) => {
                    debug!(?delivered_events, "incoming writes!");
                }
                _new = rx_stream_update.recv_many(&mut updated_streams, 100) => {
                    debug!(?updated_streams, "incoming updates!");
                    for stream in updated_streams {
                        state.add_stream(stream);
                    }
                }
                _ = interval.tick() => {
                    // For this and the else branch, we allow the loop to complete and if state has anything to process, it will be processed.
                    debug!(stream_count=%state.streams.len(),"Process undelivered stream events interval triggered.");
                    }
                else => {
                    debug!(stream_count=%state.streams.len(), "Server dropped the ordering task. Processing once more before exiting...");
                }
            };

            if !delivered_events.is_empty()
                && state
                    .process_delivered_events(&pool, delivered_events)
                    .await
                    .map_err(Self::log_error)
                    .is_err()
            {
                return;
            }

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
    _cid: StreamCid,
    prev_map: HashMap<PrevCid, EventCid>,
    cid_map: HashMap<EventCid, StreamCommit>,
}

impl StreamEvents {
    fn new<I>(cid: StreamCid, events: I) -> Self
    where
        I: IntoIterator<Item = StreamCommit>,
    {
        let mut new = Self::new_empty(cid);

        for event in events {
            new.add_event(event);
        }
        new
    }

    fn new_empty(cid: StreamCid) -> Self {
        Self {
            _cid: cid,
            prev_map: HashMap::default(),
            cid_map: HashMap::default(),
        }
    }

    /// returns true if this is a new event.
    fn add_event(&mut self, event: StreamCommit) -> bool {
        if let Some(prev) = event.prev {
            self.prev_map.insert(prev, event.cid);
        }
        self.cid_map.insert(event.cid, event).is_none()
    }

    fn remove_by_event_cid(&mut self, cid: &Cid) -> Option<StreamCommit> {
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
            .filter_map(|(cid, event)| if event.delivered { Some(cid) } else { None })
    }

    fn order_events(&mut self, start_with: EventCid) -> VecDeque<Cid> {
        let mut deliverable = VecDeque::with_capacity(self.prev_map.len());

        deliverable.push_back(start_with);
        self.remove_by_event_cid(&start_with);
        let mut tip = start_with;
        // technically, could be in deliverable set as well if the stream is forking?
        while let Some(next_event) = self.remove_by_prev_cid(&tip) {
            deliverable.push_back(next_event);
            tip = next_event;
        }
        deliverable
    }
}

pub struct OrderingState {
    streams: HashSet<StreamCid>,
    processed_streams: HashSet<StreamCid>,
    deliverable: VecDeque<EventCid>,
}

impl OrderingState {
    fn new() -> Self {
        Self {
            streams: HashSet::new(),
            processed_streams: HashSet::new(),
            deliverable: VecDeque::new(),
        }
    }

    /// Add a stream to the list of streams to process. This implies it has undelivered events and is worthwhile to attempt.
    fn add_stream(&mut self, stream: StreamCid) {
        self.processed_streams.remove(&stream);
        self.streams.insert(stream);
    }

    /// Process every stream we know about that has undelivered events that should be "unlocked" now. This could be adjusted to process commit things in batches,
    /// but for now it assumes it can process all the streams and events in one go. It should be idempotent, so if it fails, it can be retried. Events that are
    /// delivered multiple times will not change the original delivered state.
    async fn process_streams(&mut self, pool: &SqlitePool) -> Result<()> {
        for stream in &self.streams {
            let stream_events = CeramicOneStream::load_stream_commits(pool, *stream).await?;
            trace!(?stream_events, "Loaded stream events for ordering");
            let mut to_process = StreamEvents::new(*stream, stream_events.into_iter());
            if to_process.delivered_events().count() == 0 {
                return Ok(());
            }

            let mut start_with = VecDeque::with_capacity(to_process.cid_map.len());
            let delivered_cids = to_process.delivered_events().cloned().collect::<Vec<_>>();

            for cid in delivered_cids {
                if let Some(needs_me) = to_process.remove_by_prev_cid(&cid) {
                    start_with.push_back(needs_me);
                }
            }

            while let Some(new_tip) = start_with.pop_front() {
                self.deliverable.extend(to_process.order_events(new_tip));
            }
            self.processed_streams.insert(*stream);
        }

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
            self.streams
                .retain(|stream| !self.processed_streams.contains(stream));
            self.processed_streams.clear();
        }
        Ok(())
    }

    /// We group the events into their stream and see if something was waiting for one of these. If so, we process the stream.
    async fn process_delivered_events(
        &mut self,
        pool: &SqlitePool,
        events: Vec<CandidateEvent>,
    ) -> Result<()> {
        tracing::trace!("Processing {} delivered events for ordering", events.len());
        let stream_events: HashMap<StreamCid, Vec<EventCid>> = events
            .into_iter()
            .into_group_map_by(|event| event.stream_cid)
            .into_iter()
            .map(|(stream_cid, events)| {
                (
                    stream_cid,
                    events
                        .into_iter()
                        .map(|event| event.cid)
                        .collect::<Vec<_>>(),
                )
            })
            .collect();

        for (stream, event_cids) in stream_events {
            for cid in event_cids {
                let (_exists, delivered) = CeramicOneEvent::delivered_by_cid(pool, &cid).await?;
                if delivered {
                    self.add_stream(stream);
                    break;
                }
            }
        }
        Ok(())
    }

    /// Processes all streams with undelivered events returning the total number of streams identified. This is a recursive function that will
    /// continue to process streams until it finds no more streams with undelivered events. This is useful for bootstrapping the ordering task
    /// in case we missed/dropped something in the past. Anything we can't process now requires discovering from a peer, so there isn't really
    /// any advantage to keeping it in memory and trying again later.
    async fn process_all_undelivered_events(&mut self, pool: &SqlitePool) -> Result<usize> {
        tracing::trace!("Processing all undelivered events for ordering");

        let mut streams_discovered = 0;
        let mut last_cid = Some(StreamCid::default());
        while let Some(cid) = last_cid {
            let cids =
                CeramicOneStream::load_stream_cids_with_undelivered_events(pool, cid).await?;
            last_cid = cids.last().cloned();
            for cid in cids {
                self.add_stream(cid);
                streams_discovered += 1;
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
        let mut insert_later = Vec::with_capacity(2);
        let mut all_insertable = Vec::with_capacity(8);

        for (i, event) in s1_events.iter().enumerate() {
            let insertable = EventInsertable::try_new(event.0.to_owned(), &event.1)
                .await
                .unwrap();
            all_insertable.push(insertable.clone());
            if i == 1 {
                insert_later.push(insertable);
            } else {
                CeramicOneEvent::insert_many(&pool, &[insertable], false)
                    .await
                    .unwrap();
            }
        }
        for (i, event) in s2_events.iter().enumerate() {
            let insertable = EventInsertable::try_new(event.0.to_owned(), &event.1)
                .await
                .unwrap();
            all_insertable.push(insertable.clone());

            if i == 1 {
                insert_later.push(insertable);
            } else {
                CeramicOneEvent::insert_many(&pool, &[insertable], false)
                    .await
                    .unwrap();
            }
        }

        CeramicOneEvent::insert_many(&pool, &insert_later[..], false)
            .await
            .unwrap();

        for event in &all_insertable {
            let (_exists, delivered) = CeramicOneEvent::delivered_by_cid(&pool, &event.cid())
                .await
                .unwrap();
            // init events are always delivered and the last two would have been okay.. but the rest should have been skipped
            if event.cid() == event.stream_cid()
                || event.order_key == s1_events[1].0
                || event.order_key == s2_events[1].0
            {
                assert!(delivered);
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
            let (_exists, delivered) = CeramicOneEvent::delivered_by_cid(&pool, &event.cid())
                .await
                .unwrap();
            assert!(delivered);
        }
    }
}
