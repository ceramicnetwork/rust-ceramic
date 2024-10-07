//! Main library entry point for ceramic_api_server implementation.

// See https://github.com/tokio-rs/tracing/pull/2880
#![allow(clippy::blocks_in_conditions)]
#![allow(unused_imports)]

mod event;

use std::collections::HashMap;
use std::time::Duration;
use std::{future::Future, ops::Range};
use std::{marker::PhantomData, ops::RangeBounds};
use std::{net::SocketAddr, ops::Bound};
use std::{
    ops::ControlFlow,
    task::{Context, Poll},
};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use async_trait::async_trait;
use ceramic_api_server::models::{BadRequestResponse, ErrorResponse, EventData};
use ceramic_api_server::{
    models::{self, Event},
    DebugHeapGetResponse, EventsEventIdGetResponse, EventsPostResponse,
    InterestsSortKeySortValuePostResponse, LivenessGetResponse, VersionGetResponse,
    VersionPostResponse,
};
use ceramic_api_server::{
    Api, ConfigNetworkGetResponse, ExperimentalEventsSepSepValueGetResponse,
    ExperimentalInterestsGetResponse, FeedEventsGetResponse, FeedResumeTokenGetResponse,
    InterestsPostResponse,
};
use ceramic_core::{Cid, EventId, Interest, Network, NodeId, PeerId, StreamId};
use futures::TryFutureExt;
use recon::Key;
use swagger::{ApiError, ByteArray};
#[cfg(not(target_env = "msvc"))]
use tikv_jemalloc_ctl::epoch;
use tokio::sync::broadcast;
use tracing::{instrument, Level};

use crate::server::event::event_id_from_car;
use crate::ResumeToken;

/// How many events to try to process at once i.e. read from the channel in batches.
const EVENTS_TO_RECEIVE: usize = 10;
/// When the incoming events queue has at least this many items, we'll store them.
/// This imples when we're getting writes faster than the flush interval.
const EVENT_INSERT_QUEUE_SIZE: usize = 3;
/// How often we should flush the queue of events to the store. This applies when we have fewer than `EVENT_INSERT_QUEUE_SIZE` events,
/// in order to avoid stalling a single write from being processed for too long, while still reducing contention when we have a lot of writes.
/// This is quite low, but in my benchmarking adding a longer interval just slowed ingest down, without changing contention noticeably.
const FLUSH_INTERVAL_MS: u64 = 10;

/// How long are we willing to wait to enqueue an insert to the database service loop before we tell the call it was full.
const INSERT_ENQUEUE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
/// How long are we willing to wait for the database service to respond to an insert request before we tell the caller it was too slow.
/// Aborting and returning an error doesn't mean that the write won't be processed, only that the caller will get an error indicating it timed out.
const INSERT_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

// Helper to build responses consistent as we can't implement for the api_server::models directly
pub struct BuildResponse {}
impl BuildResponse {
    pub fn event(id: Cid, data: Option<Vec<u8>>) -> models::Event {
        let id = id.to_string();
        let mut res = models::Event::new(id);
        if data.as_ref().map_or(false, |e| !e.is_empty()) {
            res.data = Some(multibase::encode(multibase::Base::Base64, data.unwrap()));
        }
        res
    }
}

fn convert_base(input: &str, to: multibase::Base) -> Result<String, String> {
    if input.is_empty() {
        Err("Input cannot be empty. Expected multibase encoded value.".to_owned())
    } else {
        let (base, bytes) =
            multibase::decode(input).map_err(|err| format!("multibase error: {err}"))?;
        if base == to {
            Ok(input.to_owned())
        } else {
            Ok(multibase::encode(to, bytes))
        }
    }
}

#[derive(Debug, Clone)]
struct ValidatedInterest {
    /// 'model' typically
    sep: String,
    /// Base36 encoded stream ID
    sep_value: String,
    /// DID
    controller: Option<String>,
    /// Base36 encoded stream ID
    stream_id: Option<String>,
}

impl TryFrom<models::Interest> for ValidatedInterest {
    type Error = String;
    fn try_from(interest: models::Interest) -> Result<Self, Self::Error> {
        let sep = if interest.sep.is_empty() {
            return Err("'sep' cannot be empty".to_owned());
        } else {
            interest.sep
        };
        let sep_value = convert_base(&interest.sep_value, multibase::Base::Base36Lower)?;
        let controller = interest
            .controller
            .map(|c| {
                if c.is_empty() {
                    Err("'controller' cannot be empty if it's included")
                } else {
                    Ok(c)
                }
            })
            .transpose()?;
        let stream_id = interest
            .stream_id
            .map(|id| convert_base(&id, multibase::Base::Base36Lower))
            .transpose()?;
        Ok(ValidatedInterest {
            sep,
            sep_value,
            controller,
            stream_id,
        })
    }
}

/// Trait for accessing persistent storage of Interests
#[async_trait]
pub trait InterestService: Send + Sync {
    /// Returns true if the key was newly inserted, false if it already existed.
    async fn insert(&self, key: Interest) -> Result<bool>;
    async fn range(
        &self,
        start: &Interest,
        end: &Interest,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Interest>>;
}

#[async_trait]
impl<S: InterestService> InterestService for Arc<S> {
    async fn insert(&self, key: Interest) -> Result<bool> {
        self.as_ref().insert(key).await
    }

    async fn range(
        &self,
        start: &Interest,
        end: &Interest,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Interest>> {
        self.as_ref().range(start, end, offset, limit).await
    }
}

#[derive(Debug, Clone)]
pub enum EventInsertResult {
    Success(EventId),
    Failed(EventId, String),
}

impl EventInsertResult {
    pub fn new_ok(id: EventId) -> Self {
        Self::Success(id)
    }
    pub fn new_failed(id: EventId, reason: String) -> Self {
        Self::Failed(id, reason)
    }

    pub fn id(&self) -> &EventId {
        match self {
            Self::Success(id) => id,
            Self::Failed(id, _) => id,
        }
    }

    pub fn success(&self) -> bool {
        match self {
            Self::Success(_) => true,
            Self::Failed(_, _) => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventDataResult {
    /// The CID of the event
    pub id: ceramic_core::Cid,
    /// The data as a car file. Can be none if not requested.
    pub data: Option<Vec<u8>>,
}

impl EventDataResult {
    pub fn new(id: ceramic_core::Cid, data: Option<Vec<u8>>) -> Self {
        Self { id, data }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncludeEventData {
    None,
    Full,
}

impl TryFrom<String> for IncludeEventData {
    type Error = String;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "none" => Ok(Self::None),
            "full" => Ok(Self::Full),
            _ => Err(format!("Invalid value: {}.", value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiItem {
    /// The recon event ID for the payload
    pub key: EventId,
    /// The event payload as a carfile
    pub value: Arc<Vec<u8>>,
}

impl ApiItem {
    /// Create a new item from an ID and carfile
    pub fn new(key: EventId, value: Vec<u8>) -> Self {
        Self {
            key,
            value: Arc::new(value),
        }
    }

    /// Create a new item from an ID and an Arc of a carfile
    pub fn new_arced(key: EventId, value: Arc<Vec<u8>>) -> Self {
        Self { key, value }
    }
}

/// Trait for accessing persistent storage of Events
#[async_trait]
pub trait EventService: Send + Sync {
    /// Returns (new_key, new_value) where true if was newly inserted, false if it already existed.
    async fn insert_many(
        &self,
        items: Vec<ApiItem>,
        informant: NodeId,
    ) -> Result<Vec<EventInsertResult>>;
    async fn range_with_values(
        &self,
        range: Range<EventId>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(Cid, Vec<u8>)>>;

    /**
     * Returns the event value bytes as a CAR file, identified by the order key (EventID).
     */
    async fn value_for_order_key(&self, key: &EventId) -> Result<Option<Vec<u8>>>;

    /**
     * Returns the event value bytes as a CAR file, identified by the root CID of the event.
     */
    async fn value_for_cid(&self, key: &Cid) -> Result<Option<Vec<u8>>>;

    // it's likely `highwater` will be a string or struct when we have alternative storage for now we
    // keep it simple to allow easier error propagation. This isn't currently public outside of this repo.
    // `include_data` indicates whether the payload (carfile) of the event should be returned or just the ID
    async fn events_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
        include_data: IncludeEventData,
    ) -> Result<(i64, Vec<EventDataResult>)>;

    async fn highwater_mark(&self) -> Result<i64>;

    async fn get_block(&self, cid: &Cid) -> Result<Option<Vec<u8>>>;
}

#[async_trait::async_trait]
impl<S: EventService> EventService for Arc<S> {
    async fn insert_many(
        &self,
        items: Vec<ApiItem>,
        informant: NodeId,
    ) -> Result<Vec<EventInsertResult>> {
        self.as_ref().insert_many(items, informant).await
    }

    async fn range_with_values(
        &self,
        range: Range<EventId>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(Cid, Vec<u8>)>> {
        self.as_ref().range_with_values(range, offset, limit).await
    }

    async fn value_for_order_key(&self, key: &EventId) -> Result<Option<Vec<u8>>> {
        self.as_ref().value_for_order_key(key).await
    }

    async fn value_for_cid(&self, key: &Cid) -> Result<Option<Vec<u8>>> {
        self.as_ref().value_for_cid(key).await
    }

    async fn events_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
        include_data: IncludeEventData,
    ) -> Result<(i64, Vec<EventDataResult>)> {
        self.as_ref()
            .events_since_highwater_mark(highwater, limit, include_data)
            .await
    }

    async fn highwater_mark(&self) -> Result<i64> {
        self.as_ref().highwater_mark().await
    }
    async fn get_block(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
        self.as_ref().get_block(cid).await
    }
}

struct EventInsert {
    id: EventId,
    data: Vec<u8>,
    tx: tokio::sync::oneshot::Sender<Result<EventInsertResult>>,
}

struct InsertTask {
    _handle: tokio::task::JoinHandle<()>,
    tx: tokio::sync::mpsc::Sender<EventInsert>,
}

#[derive(Clone)]
pub struct Server<C, I, M> {
    node_id: NodeId,
    network: Network,
    interest: I,
    model: Arc<M>,
    // If we need to restart this ever, we'll need a mutex. For now we want to avoid locking the channel
    // so we just keep track to gracefully shutdown, but if the task dies, the server is in a fatal error state.
    insert_task: Arc<InsertTask>,
    marker: PhantomData<C>,
    authentication: bool,
}

impl<C, I, M> Server<C, I, M>
where
    I: InterestService,
    M: EventService + 'static,
{
    pub fn new(
        node_id: NodeId,
        network: Network,
        interest: I,
        model: Arc<M>,
        shutdown_signal: broadcast::Receiver<()>,
    ) -> Self {
        let (tx, event_rx) = tokio::sync::mpsc::channel::<EventInsert>(1024);
        let event_store = model.clone();

        let handle = Self::start_insert_task(event_store, event_rx, node_id, shutdown_signal);
        let insert_task = Arc::new(InsertTask {
            _handle: handle,
            tx,
        });
        Server {
            node_id,
            network,
            interest,
            model,
            insert_task,
            marker: PhantomData,
            authentication: false,
        }
    }

    pub fn with_authentication(&mut self, authentication: bool) -> &mut Self {
        self.authentication = authentication;
        self
    }

    fn start_insert_task(
        event_store: Arc<M>,
        mut event_rx: tokio::sync::mpsc::Receiver<EventInsert>,
        node_id: NodeId,
        mut shutdown_signal: broadcast::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(FLUSH_INTERVAL_MS));
            let mut events = vec![];
            // could bias towards processing the queue of events over accepting more, but we'll
            // rely on the channel depth for backpressure. the goal is to keep the queue close to empty
            // without processing one at a time. when we stop parsing the carfile in the store
            // i.e. validate before sending here and this is just an insert, we may want to process more at once.
            let mut shutdown = false;
            loop {
                let mut buf = Vec::with_capacity(EVENTS_TO_RECEIVE);
                let mut process_early = false;
                tokio::select! {
                    _ = interval.tick() => {
                        process_early = true;
                    }
                    val = event_rx.recv_many(&mut buf, EVENTS_TO_RECEIVE) => {
                        if val > 0 {
                            events.extend(buf);
                        }
                    }
                    _ = shutdown_signal.recv() => {
                        tracing::debug!("Insert many task got shutdown signal");
                        shutdown = true;
                    }
                };
                if shutdown {
                    tracing::info!("Shutting down insert task after processing current batch");
                    if !event_rx.is_empty() {
                        let remaining_event_cnt = event_rx.len();
                        let mut buf = Vec::with_capacity(remaining_event_cnt);
                        tracing::info!(
                            "Receiving {remaining_event_cnt} remaining events for insert task before exiting"
                        );
                        event_rx.recv_many(&mut buf, event_rx.len()).await;
                        events.extend(buf);
                    }

                    Self::process_events(&mut events, &event_store, node_id).await;
                    return;
                }
                // process events at the interval or when we're under heavy load.
                // we do it outside the select! to avoid any cancel safety issues
                // even though we should be okay since we're using tokio channels/intervals
                if events.len() >= EVENT_INSERT_QUEUE_SIZE || process_early {
                    Self::process_events(&mut events, &event_store, node_id).await;
                }
            }
        })
    }

    async fn process_events(events: &mut Vec<EventInsert>, event_store: &Arc<M>, node_id: NodeId) {
        if events.is_empty() {
            return;
        }
        let mut oneshots = HashMap::with_capacity(events.len());
        let mut items = Vec::with_capacity(events.len());
        events.drain(..).for_each(|req: EventInsert| {
            oneshots.insert(req.id.to_bytes(), req.tx);
            items.push(ApiItem::new(req.id, req.data));
        });
        tracing::trace!("calling insert many with {} items.", items.len());
        match event_store.insert_many(items, node_id).await {
            Ok(results) => {
                tracing::debug!("insert many returned {} results.", results.len());
                for result in results {
                    let id = result.id();
                    if let Some(tx) = oneshots.remove(&id.to_bytes()) {
                        if let Err(e) = tx.send(Ok(result)) {
                            tracing::warn!(
                                "failed to send success response to api listener: {:?}",
                                e
                            );
                        }
                    } else {
                        tracing::warn!(
                            "lost channel to respond to API listener for event ID: {:?}",
                            id
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!("failed to insert events: {e}");
                for tx in oneshots.into_values() {
                    if let Err(e) = tx.send(Err(anyhow::anyhow!("Failed to insert event: {e}"))) {
                        tracing::warn!("failed to send failed response to api listener: {:?}", e);
                    }
                }
            }
        };
    }

    pub async fn get_event_feed(
        &self,
        resume_at: Option<String>,
        limit: Option<i32>,
        include_data: Option<String>,
    ) -> Result<FeedEventsGetResponse, ErrorResponse> {
        let hw = resume_at.map(ResumeToken::new).unwrap_or_default();
        let limit = limit.unwrap_or(100);
        let include_data = match include_data.map_or(Ok(IncludeEventData::None), |v| {
            IncludeEventData::try_from(v)
        }) {
            Ok(v) => v,
            Err(e) => {
                return Ok(FeedEventsGetResponse::BadRequest(
                    models::BadRequestResponse::new(format!(
                        "{} must be one of 'none' or 'full'",
                        e
                    )),
                ))
            }
        };

        if limit > 10_000 && !matches!(include_data, IncludeEventData::None) {
            return Ok(FeedEventsGetResponse::BadRequest(
                models::BadRequestResponse::new(format!(
                    "The limit value must be less than 10000 when including data ({}) ",
                    limit
                )),
            ));
        }
        let hw = match (&hw).try_into() {
            Ok(hw) => hw,
            Err(err) => {
                return Ok(FeedEventsGetResponse::BadRequest(
                    models::BadRequestResponse::new(format!(
                        "Invalid resume token '{}'. {}",
                        hw, err
                    )),
                ))
            }
        };
        let (new_hw, event_ids) = self
            .model
            .events_since_highwater_mark(hw, limit as i64, include_data)
            .await
            .map_err(|e| ErrorResponse::new(format!("failed to get event data: {e}")))?;
        let events = event_ids
            .into_iter()
            .map(|ev| BuildResponse::event(ev.id, ev.data))
            .collect();

        Ok(FeedEventsGetResponse::Success(models::EventFeed {
            resume_token: new_hw.to_string(),
            events,
        }))
    }

    pub async fn get_feed_resume_token(&self) -> Result<FeedResumeTokenGetResponse, ErrorResponse> {
        let hw = self
            .model
            .highwater_mark()
            .await
            .map_err(|e| ErrorResponse::new(format!("failed to get highwater mark: {e}")))?;

        Ok(FeedResumeTokenGetResponse::Success(
            models::FeedResumeTokenGet200Response {
                resume_token: hw.to_string(),
            },
        ))
    }

    pub async fn get_interests(
        &self,
        peer_id: Option<String>,
    ) -> Result<ExperimentalInterestsGetResponse, ErrorResponse> {
        let interests = self
            .interest
            .range(
                &Interest::min_value(),
                &Interest::max_value(),
                0,
                usize::MAX,
            )
            .await
            .map_err(|e| ErrorResponse::new(format!("failed to get interests: {e}")))?;

        let peer_id = peer_id
            .map(|id| PeerId::from_str(&id))
            .transpose()
            .map_err(|e| ErrorResponse::new(format!("failed to parse peer_id: {e}")))?;

        Ok(ExperimentalInterestsGetResponse::Success(
            models::InterestsGet {
                interests: interests
                    .into_iter()
                    .filter_map(|i| {
                        if peer_id.is_some() {
                            // return only matching interests
                            if i.peer_id() == peer_id {
                                Some(models::InterestsGetInterestsInner {
                                    data: i.to_string(),
                                })
                            } else {
                                None
                            }
                        } else {
                            // return all interests
                            Some(models::InterestsGetInterestsInner {
                                data: i.to_string(),
                            })
                        }
                    })
                    .collect(),
            },
        ))
    }

    pub async fn get_events_sort_key_sort_value(
        &self,
        sep_key: String,
        sep_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> Result<ExperimentalEventsSepSepValueGetResponse, ErrorResponse> {
        let limit: usize =
            limit.map_or(10000, |l| if l.is_negative() { 10000 } else { l }) as usize;
        let offset = offset.map_or(0, |o| if o.is_negative() { 0 } else { o }) as usize;
        let sep_value = match decode_multibase_data(&sep_value) {
            Ok(v) => v,
            Err(e) => return Ok(ExperimentalEventsSepSepValueGetResponse::BadRequest(e)),
        };
        // Should we validate that sep_value and stream_id are base36 encoded or just rely on input directly?
        let (start, stop) =
            self.build_start_stop_range(&sep_key, &sep_value, controller, stream_id)?;

        let events = self
            .model
            .range_with_values(start..stop, offset, limit)
            .await
            .map_err(|err| ErrorResponse::new(format!("failed to get keys: {err}")))?
            .into_iter()
            .map(|(id, data)| BuildResponse::event(id, Some(data)))
            .collect::<Vec<_>>();

        let event_cnt = events.len();
        Ok(ExperimentalEventsSepSepValueGetResponse::Success(
            models::EventsGet {
                resume_offset: (offset + event_cnt) as i32,
                events,
                is_complete: event_cnt < limit,
            },
        ))
    }

    pub async fn post_events(&self, event: EventData) -> Result<EventsPostResponse, ErrorResponse> {
        let event_data = match decode_multibase_data(&event.data) {
            Ok(v) => v,
            Err(e) => return Ok(EventsPostResponse::BadRequest(e)),
        };

        let event_id =
            match event_id_from_car(self.network.clone(), event_data.as_slice(), &self.model).await
            {
                Ok(id) => id,
                Err(err) => {
                    return Ok(EventsPostResponse::BadRequest(BadRequestResponse::new(
                        format!("Failed to parse EventID from event CAR file data: {err}"),
                    )))
                }
            };

        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::time::timeout(
            INSERT_ENQUEUE_TIMEOUT,
            self.insert_task.tx.send(EventInsert {
                id: event_id,
                data: event_data,
                tx,
            }),
        )
        .map_err(|_| {
            ErrorResponse::new("Database service queue is too full to accept requests".to_owned())
        })
        .await?
        .map_err(|_| ErrorResponse::new("Database service not available".to_owned()))?;

        let new = tokio::time::timeout(INSERT_REQUEST_TIMEOUT, rx)
            .await
            .map_err(|_| {
                ErrorResponse::new("Timeout waiting for database service response".to_owned())
            })?
            .map_err(|_| ErrorResponse::new("No response. Database service crashed".to_owned()))?
            .map_err(|e| ErrorResponse::new(format!("Failed to insert event: {e}")))?;

        match new {
            EventInsertResult::Success(_) => Ok(EventsPostResponse::Success),
            EventInsertResult::Failed(_, reason) => Ok(EventsPostResponse::BadRequest(
                BadRequestResponse::new(reason),
            )),
        }
    }

    pub async fn post_interests(
        &self,
        interest: models::Interest,
    ) -> Result<InterestsPostResponse, ErrorResponse> {
        let interest = match ValidatedInterest::try_from(interest) {
            Ok(v) => v,
            Err(e) => {
                return Ok(InterestsPostResponse::BadRequest(
                    models::BadRequestResponse::new(e),
                ))
            }
        };
        let sep_value = match decode_multibase_data(&interest.sep_value) {
            Ok(v) => v,
            Err(e) => return Ok(InterestsPostResponse::BadRequest(e)),
        };
        // Construct start and stop event id based on provided data.
        let (start, stop) = self.build_start_stop_range(
            &interest.sep,
            &sep_value,
            interest.controller,
            interest.stream_id,
        )?;
        // Update interest ranges to include this new subscription.
        let interest = Interest::builder()
            .with_sep_key(&interest.sep)
            .with_peer_id(&self.node_id.peer_id())
            .with_range((start.as_slice(), stop.as_slice()))
            .with_not_after(0)
            .build();
        self.interest
            .insert(interest)
            .await
            .map_err(|err| ErrorResponse::new(format!("failed to update interest: {err}")))?;
        Ok(InterestsPostResponse::Success)
    }

    /// Gets the event data by id.  First interprets the event id as the root CID of the event,
    /// but falls back to interpreting it as an EventID. Using EventID is only meant for internal
    /// testing and debugging, all real callers should use the root Cid.
    /// TODO: Remove the ability to get with an EventID and only support getting events by their
    /// root CID.
    pub async fn get_events_event_id(
        &self,
        event_id: String,
    ) -> Result<EventsEventIdGetResponse, ErrorResponse> {
        let (cid, data) = match decode_cid(&event_id) {
            Ok(decoded_cid) => (decoded_cid, self.model.value_for_cid(&decoded_cid).await),
            Err(err) => {
                // If the string isn't a valid CID, try interpreting it as an EventId.
                if let Ok(decoded_event_id) = decode_event_id(&event_id) {
                    (
                        decoded_event_id.cid().unwrap(),
                        self.model.value_for_order_key(&decoded_event_id).await,
                    )
                } else {
                    return Ok(EventsEventIdGetResponse::BadRequest(err));
                }
            }
        };
        match data {
            Ok(Some(data)) => {
                let event = BuildResponse::event(cid, Some(data));
                Ok(EventsEventIdGetResponse::Success(event))
            }
            Ok(None) => Ok(EventsEventIdGetResponse::EventNotFound(format!(
                "Event not found : {}",
                event_id
            ))),
            Err(err) => Err(ErrorResponse::new(format!("failed to get event: {err}"))),
        }
    }

    fn build_start_stop_range(
        &self,
        sep_key: &str,
        sep_value: &[u8],
        controller: Option<String>,
        stream_id: Option<String>,
    ) -> Result<(EventId, EventId), ErrorResponse> {
        let start_builder = EventId::builder()
            .with_network(&self.network)
            .with_sep(sep_key, sep_value);
        let stop_builder = EventId::builder()
            .with_network(&self.network)
            .with_sep(sep_key, sep_value);

        let (start_builder, stop_builder) = match (controller, stream_id) {
            (Some(controller), Some(stream_id)) => {
                let stream_id = StreamId::from_str(&stream_id)
                    .map_err(|err| ErrorResponse::new(format!("stream_id: {err}")))?;
                (
                    start_builder
                        .with_controller(&controller)
                        .with_init(&stream_id.cid),
                    stop_builder
                        .with_controller(&controller)
                        .with_init(&stream_id.cid),
                )
            }
            (Some(controller), None) => (
                start_builder.with_controller(&controller).with_min_init(),
                stop_builder.with_controller(&controller).with_max_init(),
            ),
            (None, Some(_)) => {
                return Err(ErrorResponse::new(
                    "controller is required if stream_id is specified".to_owned(),
                ))
            }
            (None, None) => (
                start_builder.with_min_controller().with_min_init(),
                stop_builder.with_max_controller().with_max_init(),
            ),
        };

        let start = start_builder.with_min_event().build_fencepost();
        let stop = stop_builder.with_max_event().build_fencepost();
        Ok((start, stop))
    }
}

pub(crate) fn decode_event_id(value: &str) -> Result<EventId, BadRequestResponse> {
    multibase::decode(value)
        .map_err(|err| {
            BadRequestResponse::new(format!("Invalid Event ID: multibase error: {err}"))
        })?
        .1
        .try_into()
        .map_err(|err| BadRequestResponse::new(format!("Invalid event id: {err}")))
}

pub(crate) fn decode_cid(value: &str) -> Result<Cid, BadRequestResponse> {
    multibase::decode(value)
        .map_err(|err| BadRequestResponse::new(format!("multibase error: {err}")))?
        .1
        .try_into()
        .map_err(|err| BadRequestResponse::new(format!("Invalid Cid: {err}")))
}

pub(crate) fn decode_multibase_data(value: &str) -> Result<Vec<u8>, BadRequestResponse> {
    Ok(multibase::decode(value)
        .map_err(|err| {
            BadRequestResponse::new(format!("Invalid event data: multibase error: {err}"))
        })?
        .1)
}

#[async_trait]
impl<C, I, M> Api<C> for Server<C, I, M>
where
    C: Send + Sync,
    I: InterestService + Sync,
    M: EventService + Sync + 'static,
{
    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn liveness_get(
        &self,
        _context: &C,
    ) -> std::result::Result<LivenessGetResponse, ApiError> {
        Ok(LivenessGetResponse::Success)
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn debug_heap_get(
        &self,
        _context: &C,
    ) -> std::result::Result<DebugHeapGetResponse, ApiError> {
        #[cfg(not(target_env = "msvc"))]
        epoch::advance().unwrap();

        // might be on BSD and others
        #[cfg(target_os = "linux")]
        {
            let mut prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().unwrap().lock().await;
            if !prof_ctl.activated() {
                return Ok(DebugHeapGetResponse::BadRequest(BadRequestResponse {
                    message: "heap profiling not enabled".to_string(),
                }));
            }
            prof_ctl
                .dump_pprof()
                .map_err(|e| ErrorResponse::new(format!("failed to dump profile: {e}")))
                .map(|pprof| DebugHeapGetResponse::Success(ByteArray(pprof)))
                .or_else(|err| Ok(DebugHeapGetResponse::InternalServerError(err)))
        }
        #[cfg(not(target_os = "linux"))]
        Ok(DebugHeapGetResponse::BadRequest(
            models::BadRequestResponse::new("unsupported platform".to_string()),
        ))
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn version_get(&self, _context: &C) -> Result<VersionGetResponse, ApiError> {
        let resp = VersionGetResponse::Success(models::Version {
            version: Some(ceramic_metadata::Version::default().version),
        });
        Ok(resp)
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn version_post(&self, _context: &C) -> Result<VersionPostResponse, ApiError> {
        let resp = VersionPostResponse::Success(models::Version {
            version: Some(ceramic_metadata::Version::default().version),
        });
        Ok(resp)
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn feed_events_get(
        &self,
        resume_at: Option<String>,
        limit: Option<i32>,
        include_data: Option<String>,
        _context: &C,
    ) -> Result<FeedEventsGetResponse, ApiError> {
        self.get_event_feed(resume_at, limit, include_data)
            .await
            .or_else(|err| Ok(FeedEventsGetResponse::InternalServerError(err)))
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn feed_resume_token_get(
        &self,
        _context: &C,
    ) -> Result<FeedResumeTokenGetResponse, ApiError> {
        self.get_feed_resume_token()
            .await
            .or_else(|err| Ok(FeedResumeTokenGetResponse::InternalServerError(err)))
    }

    async fn experimental_interests_get(
        &self,
        peer_id: Option<String>,
        _context: &C,
    ) -> Result<ExperimentalInterestsGetResponse, ApiError> {
        self.get_interests(peer_id)
            .await
            .or_else(|err| Ok(ExperimentalInterestsGetResponse::InternalServerError(err)))
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn experimental_events_sep_sep_value_get(
        &self,
        sep: String,
        sep_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        offset: Option<i32>,
        limit: Option<i32>,
        _context: &C,
    ) -> Result<ExperimentalEventsSepSepValueGetResponse, ApiError> {
        self.get_events_sort_key_sort_value(sep, sep_value, controller, stream_id, offset, limit)
            .await
            .or_else(|err| Ok(ExperimentalEventsSepSepValueGetResponse::InternalServerError(err)))
    }

    #[instrument(skip(self, _context, event), fields(event.data.len = event.data.len()), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn events_post(
        &self,
        event: EventData,
        _context: &C,
    ) -> Result<EventsPostResponse, ApiError> {
        self.post_events(event)
            .await
            .or_else(|err| Ok(EventsPostResponse::InternalServerError(err)))
    }

    async fn interests_post(
        &self,
        interest: models::Interest,
        _context: &C,
    ) -> Result<InterestsPostResponse, ApiError> {
        self.post_interests(interest)
            .await
            .or_else(|err| Ok(InterestsPostResponse::InternalServerError(err)))
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn interests_sort_key_sort_value_post(
        &self,
        sep_key: String,
        sep_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        _context: &C,
    ) -> Result<InterestsSortKeySortValuePostResponse, ApiError> {
        let interest = models::Interest {
            sep: sep_key,
            sep_value,
            controller,
            stream_id,
        };
        match self.post_interests(interest).await {
            Ok(v) => match v {
                InterestsPostResponse::Success => {
                    Ok(InterestsSortKeySortValuePostResponse::Success)
                }
                InterestsPostResponse::BadRequest(r) => {
                    Ok(InterestsSortKeySortValuePostResponse::BadRequest(r))
                }
                InterestsPostResponse::InternalServerError(e) => Ok(
                    InterestsSortKeySortValuePostResponse::InternalServerError(e),
                ),
            },
            Err(err) => Ok(InterestsSortKeySortValuePostResponse::InternalServerError(
                err,
            )),
        }
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn events_event_id_get(
        &self,
        event_id: String,
        _context: &C,
    ) -> Result<EventsEventIdGetResponse, ApiError> {
        self.get_events_event_id(event_id)
            .await
            .or_else(|err| Ok(EventsEventIdGetResponse::InternalServerError(err)))
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn config_network_get(&self, _context: &C) -> Result<ConfigNetworkGetResponse, ApiError> {
        Ok(ConfigNetworkGetResponse::Success(models::NetworkInfo {
            name: self.network.name(),
        }))
    }

    /// cors
    async fn config_network_options(
        &self,
        _context: &C,
    ) -> Result<ceramic_api_server::ConfigNetworkOptionsResponse, ApiError> {
        Ok(ceramic_api_server::ConfigNetworkOptionsResponse::Cors)
    }

    /// cors
    async fn debug_heap_options(
        &self,
        _context: &C,
    ) -> Result<ceramic_api_server::DebugHeapOptionsResponse, ApiError> {
        Ok(ceramic_api_server::DebugHeapOptionsResponse::Cors)
    }

    /// cors
    async fn events_event_id_options(
        &self,
        _event_id: String,
        _context: &C,
    ) -> Result<ceramic_api_server::EventsEventIdOptionsResponse, ApiError> {
        Ok(ceramic_api_server::EventsEventIdOptionsResponse::Cors)
    }

    /// cors
    async fn events_options(
        &self,
        _context: &C,
    ) -> Result<ceramic_api_server::EventsOptionsResponse, ApiError> {
        Ok(ceramic_api_server::EventsOptionsResponse::Cors)
    }

    /// cors
    async fn experimental_events_sep_sep_value_options(
        &self,
        _sep: String,
        _sep_value: String,
        _context: &C,
    ) -> Result<ceramic_api_server::ExperimentalEventsSepSepValueOptionsResponse, ApiError> {
        Ok(ceramic_api_server::ExperimentalEventsSepSepValueOptionsResponse::Cors)
    }

    /// cors
    async fn experimental_interests_options(
        &self,
        _peer_id: Option<String>,
        _context: &C,
    ) -> Result<ceramic_api_server::ExperimentalInterestsOptionsResponse, ApiError> {
        Ok(ceramic_api_server::ExperimentalInterestsOptionsResponse::Cors)
    }

    /// cors
    async fn feed_events_options(
        &self,
        _context: &C,
    ) -> Result<ceramic_api_server::FeedEventsOptionsResponse, ApiError> {
        Ok(ceramic_api_server::FeedEventsOptionsResponse::Cors)
    }

    /// cors
    async fn feed_resume_token_options(
        &self,
        _context: &C,
    ) -> Result<ceramic_api_server::FeedResumeTokenOptionsResponse, ApiError> {
        Ok(ceramic_api_server::FeedResumeTokenOptionsResponse::Cors)
    }

    /// cors
    async fn interests_options(
        &self,
        _context: &C,
    ) -> Result<ceramic_api_server::InterestsOptionsResponse, ApiError> {
        Ok(ceramic_api_server::InterestsOptionsResponse::Cors)
    }

    /// cors
    async fn interests_sort_key_sort_value_options(
        &self,
        _sort_key: String,
        _sort_value: String,
        _context: &C,
    ) -> Result<ceramic_api_server::InterestsSortKeySortValueOptionsResponse, ApiError> {
        Ok(ceramic_api_server::InterestsSortKeySortValueOptionsResponse::Cors)
    }

    /// Test the liveness of the Ceramic node

    /// cors
    async fn liveness_options(
        &self,
        _context: &C,
    ) -> Result<ceramic_api_server::LivenessOptionsResponse, ApiError> {
        Ok(ceramic_api_server::LivenessOptionsResponse::Cors)
    }

    /// cors
    async fn version_options(
        &self,
        _context: &C,
    ) -> Result<ceramic_api_server::VersionOptionsResponse, ApiError> {
        Ok(ceramic_api_server::VersionOptionsResponse::Cors)
    }
}
