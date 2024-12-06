use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::sync::Arc;

use anyhow::anyhow;
use ceramic_event::unvalidated;
use cid::Cid;
use ipld_core::ipld::Ipld;
use itertools::Itertools;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinSet;
use tracing::{debug, error, info, trace, warn};

use crate::event::service::PENDING_EVENTS_CHANNEL_DEPTH;
use crate::store::EventAccess;
use crate::{Error, Result};

use super::service::DiscoveredEvent;

type StreamCid = Cid;
type EventCid = Cid;
type PrevCid = Cid;

const LOG_EVERY_N_ENTRIES: usize = 10_000;
/// The max number of tasks we'll spawn when ordering events.
/// Each stask is given a chunk of streams and wil order them
const MAX_STREAM_PROCESSING_TASKS: usize = 16;
/// The minimum number of streams each task will process to ensure we don't spawn tasks when we don't have much work.
/// If there are many streams, each batch will be the size of `(num_streams_to_process) / MAX_STREAM_PROCESSING_TASKS`.
const MIN_NUM_STREAMS_PER_BATCH: usize = 25;
/// The number of events we initially pull from the channel when doing startup undelivered batch processing.
/// Being larger was measured to go faster when processing millions of events, however there's no need to
/// allocate such a large array when normally processing events in the background as we tend to keep up.
const EVENTS_TO_RECV_STARTUP: usize = 1000;
const EVENTS_TO_RECV_BACKGROUND: usize = 100;
/// The number of tasks to spawn at startup to read undelivered events from the database.
const UNDELIVERED_EVENTS_STARTUP_TASKS: u32 = 16;

#[derive(Debug)]
pub struct DeliverableTask {
    pub(crate) _handle: tokio::task::JoinHandle<()>,
    /// Currently receives events discovered over recon.
    pub(crate) tx_inserted: tokio::sync::mpsc::Sender<DiscoveredEvent>,
}

#[derive(Debug)]
pub struct OrderingTask {}

impl OrderingTask {
    /// Discover all undelivered events in the database and mark them deliverable if possible.
    /// Returns the number of events marked deliverable. Will spawn `UNDELIVERED_EVENTS_STARTUP_TASKS`
    /// to read events from the database simultaneously.
    pub async fn process_all_undelivered_events(
        event_access: Arc<EventAccess>,
        max_iterations: usize,
        batch_size: u32,
        shutdown_signal: Box<dyn Future<Output = ()>>,
    ) -> Result<usize> {
        Self::process_all_undelivered_events_with_tasks(
            event_access,
            max_iterations,
            batch_size,
            UNDELIVERED_EVENTS_STARTUP_TASKS,
            shutdown_signal,
        )
        .await
    }

    /// Internal function to allow specifying the number of tasks for better control in tests
    async fn process_all_undelivered_events_with_tasks(
        event_access: Arc<EventAccess>,
        max_iterations: usize,
        batch_size: u32,
        num_tasks: u32,
        shutdown_signal: Box<dyn Future<Output = ()>>,
    ) -> Result<usize> {
        let (tx, rx_inserted) =
            tokio::sync::mpsc::channel::<DiscoveredEvent>(PENDING_EVENTS_CHANNEL_DEPTH);

        let event_access_cln = event_access.clone();
        let writer_handle = tokio::task::spawn(async move {
            Self::run_loop(event_access_cln, rx_inserted, EVENTS_TO_RECV_STARTUP).await
        });
        let cnt = match OrderingState::process_all_undelivered_events(
            event_access,
            max_iterations,
            batch_size,
            tx,
            num_tasks,
            Box::into_pin(shutdown_signal),
        )
        .await
        {
            Ok(cnt) => cnt,
            Err(e) => {
                error!("encountered error processing undelivered events: {}", e);
                writer_handle.abort();
                return Err(Error::new_fatal(anyhow!(
                    "failed to process undelivered events: {}",
                    e
                )));
            }
        };
        info!("Waiting for {cnt} undelivered events to finish ordering...");
        if let Err(e) = writer_handle.await {
            error!(error=?e, "event ordering task failed to complete");
        }
        Ok(cnt)
    }

    /// Spawn a task to run the ordering task background process in a loop
    pub(crate) async fn run(event_access: Arc<EventAccess>, q_depth: usize) -> DeliverableTask {
        let (tx_inserted, rx_inserted) = tokio::sync::mpsc::channel::<DiscoveredEvent>(q_depth);

        let handle = tokio::spawn(async move {
            Self::run_loop(event_access, rx_inserted, EVENTS_TO_RECV_BACKGROUND).await
        });

        DeliverableTask {
            _handle: handle,
            tx_inserted,
        }
    }

    async fn run_loop(
        event_access: Arc<EventAccess>,
        mut rx_inserted: tokio::sync::mpsc::Receiver<DiscoveredEvent>,
        events_to_recv: usize,
    ) {
        let mut state = OrderingState::new();

        while !rx_inserted.is_closed() {
            let mut recon_events = Vec::with_capacity(events_to_recv);
            if rx_inserted
                .recv_many(&mut recon_events, events_to_recv)
                .await
                > 0
            {
                trace!(?recon_events, "new events discovered!");
                state.add_inserted_events(recon_events);
                // the more events we get in memory, the fewer queries we need to run to find potential history.
                // so we read out all the events we can can. we do have to yield again, but getting a bigger batch
                // we can process in went faster than smaller batches when processing millions of events at startup
                if !rx_inserted.is_empty() {
                    let to_take = rx_inserted.len();
                    let mut remaining_events = Vec::with_capacity(to_take);
                    rx_inserted.recv_many(&mut remaining_events, to_take).await;
                    state.add_inserted_events(remaining_events);
                }

                state = match state.process_streams(Arc::clone(&event_access)).await {
                    Ok(s) => s,
                    Err(()) => {
                        error!("Ordering task exiting due to fatal error");
                        return;
                    }
                }
            }
        }
        // read and process everything that's still in the channel
        if !rx_inserted.is_empty() {
            let mut remaining = Vec::with_capacity(rx_inserted.len());
            rx_inserted
                .recv_many(&mut remaining, rx_inserted.len())
                .await;
            state.add_inserted_events(remaining);
        }

        let _ = state.process_streams(event_access).await;
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
    async fn load_by_cid(event_access: &EventAccess, cid: EventCid) -> Result<Option<Self>> {
        // TODO: Condense the multiple DB queries happening here into a single query
        let (exists, deliverable) = event_access.deliverable_by_cid(&cid).await?;
        if exists {
            let data = event_access.value_by_cid(&cid).await?.ok_or_else(|| {
                Error::new_app(anyhow!(
                    "Missing event data for event that must exist: CID={}",
                    cid
                ))
            })?;
            let (_cid, parsed) = unvalidated::Event::<Ipld>::decode_car(data.as_slice(), false)
                .map_err(Error::new_app)?;

            let known_prev = match parsed.prev() {
                None => {
                    assert!(
                        deliverable,
                        "Init event must always be deliverable. Found undelivered CID: {}",
                        cid
                    );
                    StreamEvent::InitEvent(cid)
                }
                Some(prev) => {
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

impl From<DiscoveredEvent> for StreamEvent {
    fn from(ev: DiscoveredEvent) -> Self {
        match ev.prev {
            None => StreamEvent::InitEvent(ev.cid),
            Some(prev) => {
                let meta = StreamEventMetadata::new(ev.cid, prev);
                if ev.known_deliverable {
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
    /// The last event in our deliverable queue that we kept in memory to try to
    /// avoid looking it back up as soon as we get the next event. Need to remove next time.
    last_deliverable: Option<EventCid>,
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
            last_deliverable: None,
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
                    // as we could sit in the channel for a while and streams can fork, we can't rely
                    // on our in memory state to know what happened between enqueue and dequeque
                    // and need to try to process. We could add an LRU cache in the future to
                    // avoid looking up events that were processed while were were enqueued.
                    self.should_process = true;
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
        // if we're done, we don't need to bother cleaning up since we get dropped
        if self
            .cid_map
            .iter()
            .all(|(_, ev)| !matches!(ev, StreamEvent::Undelivered(_)))
        {
            true
        } else {
            self.should_process = false;

            if let Some(prune_now) = self.last_deliverable.take() {
                self.new_deliverable.push_front(prune_now);
            }
            self.last_deliverable = self.new_deliverable.back().cloned();
            for cid in self
                .new_deliverable
                .iter()
                .take(self.new_deliverable.len().saturating_sub(1))
            {
                if let Some(ev) = self.cid_map.remove(cid) {
                    match ev {
                        StreamEvent::InitEvent(_) => {
                            warn!(%cid, "should not have init event in our delivery queue when processing_completed");
                            debug_assert!(false); // panic in debug mode
                        }
                        StreamEvent::KnownDeliverable(meta) => {
                            self.prev_map.remove(&meta.prev);
                        }
                        StreamEvent::Undelivered(_) => {
                            unreachable!("should not have undelivered event in our delivery queue")
                        }
                    }
                }
            }
            self.new_deliverable.clear();
            false
        }
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

    async fn order_events(&mut self, event_access: &EventAccess) -> Result<()> {
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

        trace!(count=%undelivered_q.len(), "undelivered events to process");

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
                StreamEvent::load_by_cid(event_access, desired_prev).await?
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
        trace!(count=%self.new_deliverable.len(), "deliverable events discovered");
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

    /// Log an error and return a true if it was fatal
    fn log_error(err: Error) -> bool {
        match err {
            Error::Application { error } => {
                warn!("Encountered application error: {:?}", error);
                false
            }
            Error::Fatal { error } => {
                error!("Encountered fatal error: {:?}", error);
                true
            }
            Error::Transient { error } | Error::InvalidArgument { error } => {
                warn!("Encountered error: {:?}", error);
                false
            }
        }
    }

    /// Update the list of streams to process with new events.
    /// Relies on `add_stream_event` to handle updating the internal state.
    fn add_inserted_events(&mut self, events: Vec<DiscoveredEvent>) {
        for ev in events {
            let stream_cid = ev.id;
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
    /// Requires taking ownership of self and returning it rather than &mut access to allow processing the streams in multiple tasks.
    /// Returns an error if processing was fatal and should not be retried.
    async fn process_streams(
        mut self,
        event_access: Arc<EventAccess>,
    ) -> std::result::Result<Self, ()> {
        let mut to_process = HashMap::new();
        self.pending_by_stream.retain(|k, s| {
            if s.should_process {
                let v = std::mem::take(s);
                to_process.insert(*k, v);
                false
            } else {
                true
            }
        });

        // we need to collect everything we get back from the tasks so we can put it back on our state
        // in case we fail to persist the updates and need to try again
        let mut processed_streams = Vec::with_capacity(to_process.len());

        let mut task_set =
            Self::spawn_tasks_for_stream_batches(Arc::clone(&event_access), to_process);

        while let Some(res) = task_set.join_next().await {
            let streams = match res {
                Ok(task_res) => match task_res {
                    Ok(streams) => streams,
                    Err(error) => {
                        warn!(?error, "Error encountered processing stream batch");
                        continue;
                    }
                },
                Err(e) => {
                    error!(error=?e, "Failed to join task spawned to process stream batch");
                    continue;
                }
            };
            for (_, stream_events) in &streams {
                self.deliverable
                    .extend(stream_events.new_deliverable.iter());
            }
            processed_streams.extend(streams);
        }

        match self.persist_ready_events(&event_access).await {
            Ok(_) => {}
            Err(err) => {
                if Self::log_error(err) {
                    // if it's fatal we will tell the task to exit
                    // if not, we return the state to try again
                    return Err(());
                } else {
                    // Clear the queue as we'll rediscover it on the next run, rather than try to double update everything.
                    // We will no-op the updates so it doesn't really hurt but it's unnecessary.
                    // The StreamEvents in our pending_by_stream map all have their state updated in memory so we can pick up where we left off.
                    self.deliverable.clear();
                    return Ok(self);
                }
            }
        }
        // keep things that still have missing history but don't process them again until we get something new
        for (cid, mut stream) in processed_streams.into_iter() {
            if !stream.processing_completed() {
                self.pending_by_stream.insert(cid, stream);
            }
        }

        debug!(remaining_streams=%self.pending_by_stream.len(), "Finished processing streams");
        trace!(stream_state=?self, "Finished processing streams");

        Ok(self)
    }

    /// Splits `to_process` into chunks and spawns at most `MAX_STREAM_PROCESSING_TASKS` to review and order
    /// the events for each chunk of streams.
    fn spawn_tasks_for_stream_batches(
        event_access: Arc<EventAccess>,
        to_process: HashMap<Cid, StreamEvents>,
    ) -> JoinSet<Result<Vec<(Cid, StreamEvents)>>> {
        let mut task_set = JoinSet::new();

        let chunk_size =
            (to_process.len() / MAX_STREAM_PROCESSING_TASKS).max(MIN_NUM_STREAMS_PER_BATCH);

        let chunks = &to_process.into_iter().chunks(chunk_size);
        // chunks uses a RefCell and isn't send, so we need to allocate again and move them to avoid sync issues
        let mut streams_to_send = Vec::with_capacity(MAX_STREAM_PROCESSING_TASKS);

        for chunk in chunks {
            let streams: Vec<(Cid, StreamEvents)> = chunk.collect();
            streams_to_send.push(streams);
        }

        for mut streams in streams_to_send.into_iter() {
            let event_access_cln = Arc::clone(&event_access);
            task_set.spawn(async move {
                for (_, stream_events) in streams.iter_mut() {
                    stream_events.order_events(&event_access_cln).await?;
                }
                Ok(streams)
            });
        }
        task_set
    }

    /// Process all undelivered events in the database. This is a blocking operation that could take a long time.
    /// It is intended to be run at startup but could be used on an interval or after some errors to recover.
    async fn process_all_undelivered_events(
        event_access: Arc<EventAccess>,
        max_iterations: usize,
        batch_size: u32,
        tx: Sender<DiscoveredEvent>,
        partition_size: u32,
        shutdown_signal: std::pin::Pin<Box<dyn Future<Output = ()>>>,
    ) -> Result<usize> {
        info!("Attempting to process all undelivered events. This could take some time.");
        let (rx, tasks) = Self::spawn_tasks_for_undelivered_event_processing(
            event_access,
            max_iterations,
            batch_size,
            tx,
            partition_size,
        );

        tokio::select! {
            event_cnt = Self::collect_ordering_task_output(rx, tasks) => {
                Ok(event_cnt)
            }
            _ = shutdown_signal => {
                Err(Error::new_app(anyhow!("Ordering tasks aborted due to shutdown signal")))
            }
        }
    }

    async fn collect_ordering_task_output(
        mut rx: tokio::sync::mpsc::Receiver<OrderingTaskStatus>,
        mut tasks: JoinSet<Result<()>>,
    ) -> usize {
        let mut event_cnt = 0;
        while let Some(OrderingTaskStatus {
            number_discovered,
            new_highwater,
            task_id,
        }) = rx.recv().await
        {
            event_cnt += number_discovered;
            if event_cnt % LOG_EVERY_N_ENTRIES < number_discovered {
                // these values are useful but can be slightly misleading. the highwater mark will move forward/backward
                // based on the task reporting, and we're counting the number discovered and sent by the task, even though
                // the task doing the ordering may discover and order additional events while reviewing the events sent
                info!(count=%event_cnt, highwater=%new_highwater, %task_id, "Processed undelivered events");
            }
        }
        while let Some(res) = tasks.join_next().await {
            match res {
                Ok(v) => match v {
                    Ok(_) => {
                        // task finished so nothing to do
                    }
                    Err(error) => {
                        warn!(?error, "event ordering task failed while processing");
                    }
                },
                Err(error) => {
                    warn!(?error, "event ordering task failed with JoinError");
                }
            }
        }
        event_cnt
    }

    /// Process all undelivered events in the database. This is a blocking operation that could take a long time.
    /// It is intended to be run at startup but could be used on an interval or after some errors to recover.
    fn spawn_tasks_for_undelivered_event_processing(
        event_access: Arc<EventAccess>,
        max_iterations: usize,
        batch_size: u32,
        tx: Sender<DiscoveredEvent>,
        partition_size: u32,
    ) -> (
        tokio::sync::mpsc::Receiver<OrderingTaskStatus>,
        JoinSet<Result<()>>,
    ) {
        let mut tasks: JoinSet<Result<()>> = JoinSet::new();
        let (cnt_tx, rx) = tokio::sync::mpsc::channel(8);
        for task_id in 0..partition_size {
            debug!("starting task {task_id} of {partition_size} to process undelivered events");
            let tx = tx.clone();
            let cnt_tx = cnt_tx.clone();
            let event_access = event_access.clone();
            tasks.spawn(async move {
                let mut iter_cnt = 0;
                let mut highwater = 0;
                while iter_cnt < max_iterations {
                    iter_cnt += 1;
                    let (undelivered, new_highwater) = event_access
                        .undelivered_with_values(highwater, batch_size.into(), partition_size, task_id)
                        .await?;
                    highwater = new_highwater;
                    let found_something = !undelivered.is_empty();
                    let found_everything = undelivered.len() < batch_size as usize;
                    if found_something {
                        trace!(new_batch_count=%undelivered.len(), %task_id, "Found undelivered events in the database to process.");
                        // We can start processing and we'll follow the stream history if we have it. In that case, we either arrive
                        // at the beginning and mark them all delivered, or we find a gap and stop processing and leave them in memory.
                        // In this case, we won't discover them until we start running recon with a peer, so maybe we should drop them
                        // or otherwise mark them ignored somehow. When this function ends, we do drop everything so for now it's probably okay.
                        let number_discovered = OrderingState::process_undelivered_events_batch(
                            &event_access,
                            undelivered,
                            &tx,
                        )
                        .await?;
                        if cnt_tx.send(OrderingTaskStatus {number_discovered, new_highwater, task_id}).await.is_err() {
                            warn!("undelivered task manager not available... exiting task_id={task_id}");
                            return Err(crate::Error::new_fatal(anyhow!("delivered task manager not available... exiting task_id={task_id}")));
                        }
                    }
                    if !found_something || found_everything {
                        break;
                    }
                }
                if iter_cnt > max_iterations {
                    warn!(%batch_size, iterations=%iter_cnt, %task_id, "Exceeded max iterations for finding undelivered events!");
                }
                debug!(%task_id, "Finished processing undelivered events");
                Ok(())
            });
        }
        (rx, tasks)
    }

    async fn process_undelivered_events_batch(
        event_access: &EventAccess,
        event_data: Vec<(Cid, unvalidated::Event<Ipld>)>,
        tx: &Sender<DiscoveredEvent>,
    ) -> Result<usize> {
        trace!(cnt=%event_data.len(), "Processing undelivered events batch");
        let mut event_cnt = 0;
        let mut discovered_inits = Vec::new();
        for (cid, parsed_event) in event_data {
            event_cnt += 1;
            if parsed_event.is_init() {
                discovered_inits.push((
                    cid,
                    DiscoveredEvent {
                        cid,
                        prev: None,
                        id: cid,
                        known_deliverable: true,
                    },
                ));

                continue;
            }

            let stream_cid = parsed_event.stream_cid();
            let prev = parsed_event
                .prev()
                .expect("prev must exist for non-init events");

            if let Err(_e) = tx
                .send(DiscoveredEvent {
                    cid,
                    prev: Some(*prev),
                    id: *stream_cid,
                    known_deliverable: false,
                })
                .await
            {
                error!("task managing undelivered event insertion exited... unable to continue");
                return Err(Error::new_fatal(anyhow!(
                    "undelivered event insertion task exited"
                )));
            }
        }
        // while undelivered init events should be unreachable, we can fix the state if it happens so we won't panic in release mode
        // and simply correct things in the database. We could make this fatal in the future, but for now it's just a warning to
        // make sure we aren't overlooking something with the migration from ipfs or the previous state of the database.
        if !discovered_inits.is_empty() {
            warn!(
                count=%discovered_inits.len(),
                cids=?discovered_inits,
                "Found init events in undelivered batch. This should never happen.",
            );
            debug_assert!(false);
            // first update the deliverable flag and then tell the task as it expects all init events to have been properly
            // handled during the original insertion
            let mut db_tx = event_access.begin_tx().await?;
            for (cid, _) in &discovered_inits {
                event_access.mark_ready_to_deliver(&mut db_tx, cid).await?;
            }
            db_tx.commit().await?;
            for (_, discovered_init) in discovered_inits {
                if let Err(_e) = tx.send(discovered_init).await {
                    error!(
                        "task managing undelivered event insertion exited... unable to continue"
                    );
                    return Err(Error::new_fatal(anyhow!(
                        "undelivered event insertion task exited"
                    )));
                }
            }
        }

        Ok(event_cnt)
    }

    /// We should improve the error handling and likely add some batching if the number of ready events is very high.
    /// We copy the events up front to avoid losing any events if the task is cancelled.
    async fn persist_ready_events(&mut self, event_access: &EventAccess) -> Result<()> {
        if !self.deliverable.is_empty() {
            tracing::debug!(count=%self.deliverable.len(), "Marking events as ready to deliver");
            let mut tx = event_access.begin_tx().await?;
            // We process the ready events as a FIFO queue so they are marked delivered before events that were added after and depend on them.
            // Could use `pop_front` but we want to make sure we commit and then clear everything at once.
            for cid in &self.deliverable {
                event_access.mark_ready_to_deliver(&mut tx, cid).await?;
            }
            tx.commit().await?;
            self.deliverable.clear();
        }
        Ok(())
    }
}

#[derive(Debug)]
struct OrderingTaskStatus {
    task_id: u32,
    new_highwater: i64,
    number_discovered: usize,
}

#[cfg(test)]
mod test {
    use crate::store::EventInsertable;
    use ceramic_sql::sqlite::SqlitePool;
    use rand::{seq::SliceRandom as _, thread_rng};
    use test_log::test;

    use crate::{
        event::validator::{UnvalidatedEvent, ValidatedEvent},
        tests::get_n_events,
    };

    use super::*;
    fn fake_shutdown_signal() -> Box<dyn Future<Output = ()>> {
        Box::new(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await
            }
        })
    }

    async fn get_n_insertable_events(n: usize) -> Vec<EventInsertable> {
        let mut res = Vec::with_capacity(n);
        let events = get_n_events(n).await;
        for event in events {
            let event = ValidatedEvent::into_insertable(
                ValidatedEvent::from_unvalidated_unchecked(
                    UnvalidatedEvent::try_from(&event).unwrap(),
                ),
                None,
            );
            res.push(event);
        }
        res
    }

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_undelivered_batch_empty() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let event_access = Arc::new(EventAccess::try_new(pool).await.unwrap());
        let processed = OrderingTask::process_all_undelivered_events(
            event_access,
            1,
            5,
            fake_shutdown_signal(),
        )
        .await
        .unwrap();
        assert_eq!(0, processed);
    }

    async fn insert_10_with_9_undelivered(event_access: Arc<EventAccess>) -> Vec<EventInsertable> {
        let mut insertable = get_n_insertable_events(10).await;
        let mut init = insertable.remove(0);
        init.set_deliverable(true);

        let new = event_access.insert_many(insertable.iter()).await.unwrap();

        assert_eq!(9, new.inserted.len());
        assert_eq!(
            0,
            new.inserted
                .iter()
                .filter(|e| e.inserted.deliverable())
                .count()
        );

        let new = event_access.insert_many([&init].into_iter()).await.unwrap();
        assert_eq!(1, new.inserted.len());
        assert_eq!(
            1,
            new.inserted
                .iter()
                .filter(|e| e.inserted.deliverable())
                .count()
        );
        insertable
    }

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_undelivered_batch_offset() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let event_access = Arc::new(EventAccess::try_new(pool).await.unwrap());

        insert_10_with_9_undelivered(Arc::clone(&event_access)).await;
        let (_, events) = event_access.new_events_since_value(0, 100).await.unwrap();
        assert_eq!(1, events.len());

        // we make sure to use 1 task in this test as we want to measure the progress of each iteration
        let processed = OrderingTask::process_all_undelivered_events_with_tasks(
            event_access.clone(),
            1,
            5,
            1,
            fake_shutdown_signal(),
        )
        .await
        .unwrap();
        assert_eq!(5, processed);
        let (_, events) = event_access.new_events_since_value(0, 100).await.unwrap();
        assert_eq!(6, events.len());
        let processed = OrderingTask::process_all_undelivered_events_with_tasks(
            event_access.clone(),
            1,
            5,
            1,
            fake_shutdown_signal(),
        )
        .await
        .unwrap();
        assert_eq!(4, processed);
        let (_, events) = event_access.new_events_since_value(0, 100).await.unwrap();
        assert_eq!(10, events.len());
        let processed = OrderingTask::process_all_undelivered_events_with_tasks(
            event_access.clone(),
            1,
            5,
            1,
            fake_shutdown_signal(),
        )
        .await
        .unwrap();
        assert_eq!(0, processed);
    }

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_undelivered_batch_iterations_ends_early() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let event_access = Arc::new(EventAccess::try_new(pool).await.unwrap());
        // create 5 streams with 9 undelivered events each
        insert_10_with_9_undelivered(Arc::clone(&event_access)).await;
        insert_10_with_9_undelivered(Arc::clone(&event_access)).await;
        insert_10_with_9_undelivered(Arc::clone(&event_access)).await;
        insert_10_with_9_undelivered(Arc::clone(&event_access)).await;
        insert_10_with_9_undelivered(Arc::clone(&event_access)).await;

        let (_hw, event) = event_access.new_events_since_value(0, 1000).await.unwrap();
        assert_eq!(5, event.len());
        // we specify 1 task so we can easily expect how far it gets each run, rather than doing math against the number of spawned tasks
        let processed = OrderingTask::process_all_undelivered_events_with_tasks(
            event_access.clone(),
            4,
            10,
            1,
            fake_shutdown_signal(),
        )
        .await
        .unwrap();

        let (_hw, event) = event_access.new_events_since_value(0, 1000).await.unwrap();
        assert_eq!(45, event.len());
        assert_eq!(40, processed);
    }

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_undelivered_batch_iterations_ends_when_cant_order() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let event_access = Arc::new(EventAccess::try_new(pool).await.unwrap());
        let mut insertable = get_n_insertable_events(10).await;
        let _missing_init = insertable.remove(0);

        let _new = event_access.insert_many(insertable.iter()).await.unwrap();
        let (_hw, event) = event_access.new_events_since_value(0, 1000).await.unwrap();
        assert_eq!(0, event.len());
        let res = OrderingTask::process_all_undelivered_events(
            Arc::clone(&event_access),
            4,
            3,
            fake_shutdown_signal(),
        )
        .await
        .unwrap();
        assert_eq!(res, 9);

        let (_hw, event) = event_access.new_events_since_value(0, 1000).await.unwrap();
        assert_eq!(0, event.len());
    }

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_undelivered_batch_iterations_ends_when_all_found() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let event_access = Arc::new(EventAccess::try_new(pool).await.unwrap());
        // create 5 streams with 9 undelivered events each
        insert_10_with_9_undelivered(Arc::clone(&event_access)).await;
        insert_10_with_9_undelivered(Arc::clone(&event_access)).await;
        insert_10_with_9_undelivered(Arc::clone(&event_access)).await;
        insert_10_with_9_undelivered(Arc::clone(&event_access)).await;
        insert_10_with_9_undelivered(Arc::clone(&event_access)).await;

        let (_hw, event) = event_access.new_events_since_value(0, 1000).await.unwrap();
        assert_eq!(5, event.len());

        let processed = OrderingTask::process_all_undelivered_events(
            event_access.clone(),
            100_000_000,
            5,
            fake_shutdown_signal(),
        )
        .await
        .unwrap();

        let (_hw, event) = event_access.new_events_since_value(0, 1000).await.unwrap();
        assert_eq!(50, event.len());
        assert_eq!(45, processed);
    }

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_undelivered_batch_iterations_ends_when_all_found_many_events() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let event_access = Arc::new(EventAccess::try_new(pool).await.unwrap());
        // 1000 sets of 10 (1000 init events delivered)
        for _ in 0..1000 {
            insert_10_with_9_undelivered(Arc::clone(&event_access)).await;
        }

        let (_hw, event) = event_access
            .new_events_since_value(0, 100_000)
            .await
            .unwrap();
        assert_eq!(1000, event.len());
        let _res = OrderingTask::process_all_undelivered_events(
            Arc::clone(&event_access),
            100_000_000,
            5,
            fake_shutdown_signal(),
        )
        .await
        .unwrap();

        let (_hw, event) = event_access
            .new_events_since_value(0, 100_000)
            .await
            .unwrap();
        assert_eq!(10000, event.len());
    }

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_undelivered_batch_iterations_ends_when_all_found_many_events_out_of_order() {
        // adds `per_stream` events to `num_streams` streams, mixes up the event order for each stream, inserts half
        // the events for each stream before mixing up the stream order and inserting the rest
        // took like a minute on my machine to run 100 streams with 1000 events each, mostly inserting :( since ordering only gets 5 seconds
        let per_stream = 100;
        let num_streams = 10;
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let event_access = Arc::new(EventAccess::try_new(pool).await.unwrap());
        let expected = per_stream * num_streams;

        let mut unsorted_events = Vec::with_capacity(expected);
        for _ in 0..num_streams {
            let mut events = get_n_insertable_events(per_stream).await;
            events.get_mut(0).unwrap().set_deliverable(true); // init events must be deliverable since we're skipping service logic
            events.shuffle(&mut thread_rng());
            unsorted_events.extend(events);
        }
        unsorted_events.shuffle(&mut thread_rng());

        let new = event_access
            .insert_many(unsorted_events.iter())
            .await
            .unwrap()
            .inserted
            .len();

        assert_eq!(expected, new);

        let (_hw, delivered) = event_access
            .new_events_since_value(0, 100_000)
            .await
            .unwrap();

        assert_eq!(num_streams, delivered.len()); // 1 per stream
        let _res = OrderingTask::process_all_undelivered_events(
            Arc::clone(&event_access),
            100_000_000,
            100,
            fake_shutdown_signal(),
        )
        .await
        .unwrap();
        let (_hw, delivered) = event_access
            .new_events_since_value(0, 100_000)
            .await
            .unwrap();
        assert_eq!(expected, delivered.len());
    }

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    async fn test_process_all_undelivered_one_batch() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let event_access = Arc::new(EventAccess::try_new(pool).await.unwrap());
        // create 5 streams with 9 undelivered events each
        let expected_a = Vec::from_iter(
            insert_10_with_9_undelivered(Arc::clone(&event_access))
                .await
                .into_iter()
                .map(|e| *e.cid()),
        );
        let expected_b = Vec::from_iter(
            insert_10_with_9_undelivered(Arc::clone(&event_access))
                .await
                .into_iter()
                .map(|e| *e.cid()),
        );
        let expected_c = Vec::from_iter(
            insert_10_with_9_undelivered(Arc::clone(&event_access))
                .await
                .into_iter()
                .map(|e| *e.cid()),
        );
        let expected_d = Vec::from_iter(
            insert_10_with_9_undelivered(Arc::clone(&event_access))
                .await
                .into_iter()
                .map(|e| *e.cid()),
        );
        let expected_e = Vec::from_iter(
            insert_10_with_9_undelivered(Arc::clone(&event_access))
                .await
                .into_iter()
                .map(|e| *e.cid()),
        );

        let (_hw, event) = event_access.new_events_since_value(0, 1000).await.unwrap();
        assert_eq!(5, event.len());
        let processed = OrderingTask::process_all_undelivered_events(
            event_access.clone(),
            1,
            100,
            fake_shutdown_signal(),
        )
        .await
        .unwrap();

        let (_hw, cids) = event_access.new_events_since_value(0, 1000).await.unwrap();
        assert_eq!(50, cids.len());
        assert_eq!(45, processed);
        assert_eq!(expected_a, build_expected(&cids, &expected_a));
        assert_eq!(expected_b, build_expected(&cids, &expected_b));
        assert_eq!(expected_c, build_expected(&cids, &expected_c));
        assert_eq!(expected_d, build_expected(&cids, &expected_d));
        assert_eq!(expected_e, build_expected(&cids, &expected_e));
    }

    fn build_expected(new_cids: &[Cid], expected: &[Cid]) -> Vec<Cid> {
        let mut output = Vec::with_capacity(new_cids.len());
        for e in new_cids {
            if expected.iter().any(|i| i == e) {
                output.push(*e);
            }
        }
        output
    }
}
