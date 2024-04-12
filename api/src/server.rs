//! Main library entry point for ceramic_api_server implementation.

#![allow(unused_imports)]

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
use ceramic_api_server::models::ErrorResponse;
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
    EventsEventIdGetResponse, EventsPostResponse, InterestsSortKeySortValuePostResponse,
    LivenessGetResponse, VersionPostResponse,
};
use ceramic_api_server::{
    Api, EventsSortKeySortValueGetResponse, ExperimentalEventsSepSepValueGetResponse,
    FeedEventsGetResponse, InterestsPostResponse,
};
use ceramic_core::{interest, EventId, Interest, Network, PeerId, StreamId};
use std::error::Error;
use swagger::ApiError;

use crate::ResumeToken;

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
    async fn insert(&self, key: EventId, value: Option<Vec<u8>>) -> Result<(bool, bool)>;
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
    async fn insert(&self, key: EventId, value: Option<Vec<u8>>) -> Result<(bool, bool)> {
        self.as_ref().insert(key, value).await
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

#[derive(Clone)]
pub struct Server<C, I, M> {
    peer_id: PeerId,
    network: Network,
    interest: I,
    model: M,
    marker: PhantomData<C>,
}

impl<C, I, M> Server<C, I, M>
where
    I: AccessInterestStore,
    M: AccessModelStore,
{
    pub fn new(peer_id: PeerId, network: Network, interest: I, model: M) -> Self {
        Server {
            peer_id,
            network,
            interest,
            model,
            marker: PhantomData,
        }
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
        let event_id = decode_event_id(&event.id)?;
        let event_data = decode_event_data(&event.data)?;
        self.model
            .insert(event_id, Some(event_data))
            .await
            .map_err(|err| ErrorResponse::new(format!("failed to insert key: {err}")))?;

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
        let decoded_event_id = decode_event_id(&event_id)?;
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

pub(crate) fn decode_event_id(value: &str) -> Result<EventId, ErrorResponse> {
    multibase::decode(value)
        .map_err(|err| ErrorResponse::new(format!("multibase error: {err}")))?
        .1
        .try_into()
        .map_err(|err| ErrorResponse::new(format!("invalid event id: {err}")))
}
pub(crate) fn decode_event_data(value: &str) -> Result<Vec<u8>, ErrorResponse> {
    Ok(multibase::decode(value)
        .map_err(|err| ErrorResponse::new(format!("multibase error: {err}")))?
        .1)
}

#[async_trait]
impl<C, I, M> Api<C> for Server<C, I, M>
where
    C: Send + Sync,
    I: AccessInterestStore + Sync,
    M: AccessModelStore + Sync,
{
    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn liveness_get(
        &self,
        _context: &C,
    ) -> std::result::Result<LivenessGetResponse, ApiError> {
        Ok(LivenessGetResponse::Success)
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

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn events_sort_key_sort_value_get(
        &self,
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        offset: Option<i32>,
        limit: Option<i32>,
        _context: &C,
    ) -> Result<EventsSortKeySortValueGetResponse, ApiError> {
        match self
            .get_events_sort_key_sort_value(
                sort_key, sort_value, controller, stream_id, offset, limit,
            )
            .await
        {
            Ok(v) => match v {
                ExperimentalEventsSepSepValueGetResponse::Success(s) => {
                    Ok(EventsSortKeySortValueGetResponse::Success(s))
                }
                ExperimentalEventsSepSepValueGetResponse::BadRequest(r) => {
                    Ok(EventsSortKeySortValueGetResponse::BadRequest(r))
                }
                ExperimentalEventsSepSepValueGetResponse::InternalServerError(err) => {
                    Ok(EventsSortKeySortValueGetResponse::InternalServerError(err))
                }
            },
            Err(err) => Ok(EventsSortKeySortValueGetResponse::InternalServerError(err)),
        }
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
