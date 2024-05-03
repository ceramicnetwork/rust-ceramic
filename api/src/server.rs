//! Main library entry point for ceramic_api_server implementation.

#![allow(unused_imports)]

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

use anyhow::{bail, Result};
use async_trait::async_trait;
use ceramic_api_server::models::{BadRequestResponse, ErrorResponse};
use futures::{future, Stream, StreamExt, TryFutureExt, TryStreamExt};
use hyper::service::Service;
use hyper::{server::conn::Http, Request};
use recon::{AssociativeHash, InterestProvider, Key, Store};
use serde::{Deserialize, Serialize};
use swagger::{ByteArray, EmptyContext, XSpanIdString};
use tokio::net::TcpListener;
use tracing::{debug, info, instrument, Level};

use ceramic_api_server::server::MakeService;
use ceramic_api_server::{
    models::{self, Event},
    DebugHeapGetResponse, EventsEventIdGetResponse, EventsPostResponse,
    InterestsSortKeySortValuePostResponse, LivenessGetResponse, VersionPostResponse,
};
use ceramic_api_server::{
    Api, ExperimentalEventsSepSepValueGetResponse, FeedEventsGetResponse, InterestsPostResponse,
};
use ceramic_core::{interest, EventId, Interest, Network, PeerId, StreamId};
use std::error::Error;
use swagger::ApiError;
#[cfg(not(target_env = "msvc"))]
use tikv_jemalloc_ctl::{epoch, stats};

use crate::ResumeToken;

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
    pub fn event(id: EventId, data: Vec<u8>) -> models::Event {
        let id = multibase::encode(multibase::Base::Base16Lower, id.as_bytes());
        let data = if data.is_empty() {
            String::default()
        } else {
            multibase::encode(multibase::Base::Base64, data)
        };
        models::Event::new(id, data)
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

#[async_trait]
pub trait AccessInterestStore: Send + Sync {
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
impl<S: AccessInterestStore> AccessInterestStore for Arc<S> {
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

#[async_trait]
pub trait AccessModelStore: Send + Sync {
    /// Returns (new_key, new_value) where true if was newly inserted, false if it already existed.
    async fn insert_many(&self, items: &[(EventId, Option<Vec<u8>>)])
        -> Result<(Vec<bool>, usize)>;
    async fn range_with_values(
        &self,
        start: &EventId,
        end: &EventId,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(EventId, Vec<u8>)>>;

    async fn value_for_key(&self, key: &EventId) -> Result<Option<Vec<u8>>>;

    // it's likely `highwater` will be a string or struct when we have alternative storage for now we
    // keep it simple to allow easier error propagation. This isn't currently public outside of this repo.
    async fn keys_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
    ) -> Result<(i64, Vec<EventId>)>;
}

#[async_trait::async_trait]
impl<S: AccessModelStore> AccessModelStore for Arc<S> {
    async fn insert_many(
        &self,
        items: &[(EventId, Option<Vec<u8>>)],
    ) -> Result<(Vec<bool>, usize)> {
        self.as_ref().insert_many(items).await
    }

    async fn range_with_values(
        &self,
        start: &EventId,
        end: &EventId,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(EventId, Vec<u8>)>> {
        self.as_ref()
            .range_with_values(start, end, offset, limit)
            .await
    }

    async fn value_for_key(&self, key: &EventId) -> Result<Option<Vec<u8>>> {
        self.as_ref().value_for_key(key).await
    }

    async fn keys_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
    ) -> Result<(i64, Vec<EventId>)> {
        self.as_ref()
            .keys_since_highwater_mark(highwater, limit)
            .await
    }
}

struct EventInsert {
    id: EventId,
    data: Vec<u8>,
    tx: tokio::sync::oneshot::Sender<Result<bool>>,
}

struct InsertTask {
    _handle: tokio::task::JoinHandle<()>,
    tx: tokio::sync::mpsc::Sender<EventInsert>,
}

#[derive(Clone)]
pub struct Server<C, I, M> {
    peer_id: PeerId,
    network: Network,
    interest: I,
    model: Arc<M>,
    // If we need to restart this ever, we'll need a mutex. For now we want to avoid locking the channel
    // so we just keep track to gracefully shutdown, but if the task dies, the server is in a fatal error state.
    insert_task: Arc<InsertTask>,
    marker: PhantomData<C>,
}

impl<C, I, M> Server<C, I, M>
where
    I: AccessInterestStore,
    M: AccessModelStore + 'static,
{
    pub fn new(peer_id: PeerId, network: Network, interest: I, model: Arc<M>) -> Self {
        let (tx, event_rx) = tokio::sync::mpsc::channel::<EventInsert>(1024);
        let event_store = model.clone();

        let handle = Self::start_insert_task(event_store, event_rx);
        let insert_task = Arc::new(InsertTask {
            _handle: handle,
            tx,
        });
        Server {
            peer_id,
            network,
            interest,
            model,
            insert_task,
            marker: PhantomData,
        }
    }

    fn start_insert_task(
        event_store: Arc<M>,
        mut event_rx: tokio::sync::mpsc::Receiver<EventInsert>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(FLUSH_INTERVAL_MS));
            let mut events = vec![];
            // could bias towards processing the queue of events over accepting more, but we'll
            // rely on the channel depth for backpressure. the goal is to keep the queue close to empty
            // without processing one at a time. when we stop parsing the carfile in the store
            // i.e. validate before sending here and this is just an insert, we may want to process more at once.
            loop {
                tokio::select! {
                    _ = interval.tick(), if !events.is_empty() => {
                        Self::process_events(&mut events, &event_store).await;
                    }
                    Some(req) = event_rx.recv() => {
                        events.push(req);
                    }
                }
                // make sure the events queue doesn't get too deep when we're under heavy load
                if events.len() >= EVENT_INSERT_QUEUE_SIZE {
                    Self::process_events(&mut events, &event_store).await;
                }
            }
        })
    }

    async fn process_events(events: &mut Vec<EventInsert>, event_store: &Arc<M>) {
        let mut oneshots = Vec::with_capacity(events.len());
        let mut items = Vec::with_capacity(events.len());
        events.drain(..).for_each(|req: EventInsert| {
            oneshots.push(req.tx);
            items.push((req.id, Some(req.data)));
        });
        tracing::trace!("calling insert many with {} items.", items.len());
        match event_store.insert_many(&items).await {
            Ok((results, _)) => {
                tracing::debug!("insert many returned {} results.", results.len());
                for (tx, result) in oneshots.into_iter().zip(results.into_iter()) {
                    if let Err(e) = tx.send(Ok(result)) {
                        tracing::warn!("failed to send success response to api listener: {:?}", e);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("failed to insert events: {e}");
                for tx in oneshots.into_iter() {
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
    ) -> Result<FeedEventsGetResponse, ErrorResponse> {
        let hw = resume_at.map(ResumeToken::new).unwrap_or_default();
        let limit = limit.unwrap_or(10000) as usize;
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
            .keys_since_highwater_mark(hw, limit as i64)
            .await
            .map_err(|e| ErrorResponse::new(format!("failed to get keys: {e}")))?;
        let events = event_ids
            .into_iter()
            .map(|id| BuildResponse::event(id, vec![]))
            .collect();

        Ok(FeedEventsGetResponse::Success(models::EventFeed {
            resume_token: new_hw.to_string(),
            events,
        }))
    }

    pub async fn get_events_sort_key_sort_value(
        &self,
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        offset: Option<i32>,
        limit: Option<i32>,
    ) -> Result<ExperimentalEventsSepSepValueGetResponse, ErrorResponse> {
        let limit: usize =
            limit.map_or(10000, |l| if l.is_negative() { 10000 } else { l }) as usize;
        let offset = offset.map_or(0, |o| if o.is_negative() { 0 } else { o }) as usize;
        // Should we validate that sort_value and stream_id are base36 encoded or just rely on input directly?
        let (start, stop) =
            self.build_start_stop_range(&sort_key, &sort_value, controller, stream_id)?;

        let events = self
            .model
            .range_with_values(&start, &stop, offset, limit)
            .await
            .map_err(|err| ErrorResponse::new(format!("failed to get keys: {err}")))?
            .into_iter()
            .map(|(id, data)| BuildResponse::event(id, data))
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

    pub async fn post_events(&self, event: Event) -> Result<EventsPostResponse, ErrorResponse> {
        let event_id = match decode_event_id(&event.id) {
            Ok(v) => v,
            Err(e) => return Ok(EventsPostResponse::BadRequest(e)),
        };
        let event_data = match decode_event_data(&event.data) {
            Ok(v) => v,
            Err(e) => return Ok(EventsPostResponse::BadRequest(e)),
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

        let _new = tokio::time::timeout(INSERT_REQUEST_TIMEOUT, rx)
            .await
            .map_err(|_| {
                ErrorResponse::new("Timeout waiting for database service response".to_owned())
            })?
            .map_err(|_| ErrorResponse::new("No response. Database service crashed".to_owned()))?
            .map_err(|e| ErrorResponse::new(format!("Failed to insert event: {e}")))?;

        Ok(EventsPostResponse::Success)
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
        self.store_interest(interest).await?;
        Ok(InterestsPostResponse::Success)
    }

    pub async fn get_events_event_id(
        &self,
        event_id: String,
    ) -> Result<EventsEventIdGetResponse, ErrorResponse> {
        let decoded_event_id = match decode_event_id(&event_id) {
            Ok(v) => v,
            Err(e) => return Ok(EventsEventIdGetResponse::BadRequest(e)),
        };
        match self.model.value_for_key(&decoded_event_id).await {
            Ok(Some(data)) => {
                let event = BuildResponse::event(decoded_event_id, data);
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
        sort_key: &str,
        sort_value: &str,
        controller: Option<String>,
        stream_id: Option<String>,
    ) -> Result<(EventId, EventId), ErrorResponse> {
        let start_builder = EventId::builder()
            .with_network(&self.network)
            .with_sort_value(sort_key, sort_value);
        let stop_builder = EventId::builder()
            .with_network(&self.network)
            .with_sort_value(sort_key, sort_value);

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

        let start = start_builder.with_min_event_height().build_fencepost();
        let stop = stop_builder.with_max_event_height().build_fencepost();
        Ok((start, stop))
    }

    async fn store_interest(
        &self,
        interest: ValidatedInterest,
    ) -> Result<(EventId, EventId), ErrorResponse> {
        // Construct start and stop event id based on provided data.
        let (start, stop) = self.build_start_stop_range(
            &interest.sep,
            &interest.sep_value,
            interest.controller,
            interest.stream_id,
        )?;
        // Update interest ranges to include this new subscription.
        let interest = Interest::builder()
            .with_sort_key(&interest.sep)
            .with_peer_id(&self.peer_id)
            .with_range((start.as_slice(), stop.as_slice()))
            .with_not_after(0)
            .build();
        self.interest
            .insert(interest)
            .await
            .map_err(|err| ErrorResponse::new(format!("failed to update interest: {err}")))?;

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
pub(crate) fn decode_event_data(value: &str) -> Result<Vec<u8>, BadRequestResponse> {
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
    I: AccessInterestStore + Sync,
    M: AccessModelStore + Sync + 'static,
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
        _context: &C,
    ) -> Result<FeedEventsGetResponse, ApiError> {
        self.get_event_feed(resume_at, limit)
            .await
            .or_else(|err| Ok(FeedEventsGetResponse::InternalServerError(err)))
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

    #[instrument(skip(self, _context, event), fields(event.id = event.id, event.data.len = event.data.len()), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn events_post(
        &self,
        event: Event,
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
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        _context: &C,
    ) -> Result<InterestsSortKeySortValuePostResponse, ApiError> {
        let interest = models::Interest {
            sep: sort_key,
            sep_value: sort_value,
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
}
