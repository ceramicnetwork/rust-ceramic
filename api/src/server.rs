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
use futures::{future, Stream, StreamExt, TryFutureExt, TryStreamExt};
use hyper::service::Service;
use hyper::{server::conn::Http, Request};
use recon::{AssociativeHash, InterestProvider, Key, Store};
use serde::{Deserialize, Serialize};
use swagger::{ByteArray, EmptyContext, XSpanIdString};
use tokio::net::TcpListener;
use tracing::{debug, info, instrument, Level};

use ceramic_api_server::{
    models::{self, Event, EventsPostRequest},
    EventsEventIdGetResponse, EventsPostResponse, InterestsSortKeySortValuePostResponse,
    LivenessGetResponse, VersionPostResponse,
};
use ceramic_core::{EventId, Interest, Network, PeerId, StreamId};

#[async_trait]
pub trait AccessInterestStore: Clone + Send + Sync {
    type Key: Key;
    type Hash: AssociativeHash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>;

    /// Returns true if the key was newly inserted, false if it already existed.
    async fn insert(&self, key: Self::Key) -> Result<bool>;
    async fn range(
        &self,
        start: Self::Key,
        end: Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Self::Key>>;
}

#[async_trait]
pub trait AccessModelStore: Clone + Send + Sync {
    type Key: Key;
    type Hash: AssociativeHash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>;

    /// Returns (new_key, new_value) where true if was newly inserted, false if it already existed.
    async fn insert(&self, key: Self::Key, value: Option<Vec<u8>>) -> Result<(bool, bool)>;
    async fn range_with_values(
        &self,
        start: Self::Key,
        end: Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(Self::Key, Vec<u8>)>>;

    async fn value_for_key(&self, key: Self::Key) -> Result<Option<Vec<u8>>>;

    // it's likely `highwater` will be a string or struct when we have alternative storage for now we
    // keep it simple to allow easier error propagation. This isn't currently public outside of this repo.
    async fn keys_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
    ) -> anyhow::Result<(i64, Vec<Self::Key>)>;
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
    I: AccessInterestStore<Key = Interest>,
    M: AccessModelStore<Key = EventId>,
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

    fn build_start_stop_range(
        &self,
        sort_key: &str,
        sort_value: &str,
        controller: Option<String>,
        stream_id: Option<String>,
    ) -> Result<(EventId, EventId), ApiError> {
        let start_builder = EventId::builder()
            .with_network(&self.network)
            .with_sort_value(sort_key, sort_value);
        let stop_builder = EventId::builder()
            .with_network(&self.network)
            .with_sort_value(sort_key, sort_value);

        let (start_builder, stop_builder) = match (controller, stream_id) {
            (Some(controller), Some(stream_id)) => {
                let stream_id = StreamId::from_str(&stream_id)
                    .map_err(|err| ApiError(format!("stream_id: {err}")))?;
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
                return Err(ApiError(
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
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
    ) -> Result<(EventId, EventId), ApiError> {
        // Construct start and stop event id based on provided data.
        let (start, stop) =
            self.build_start_stop_range(&sort_key, &sort_value, controller, stream_id)?;
        // Update interest ranges to include this new subscription.
        let interest = Interest::builder()
            .with_sort_key(&sort_key)
            .with_peer_id(&self.peer_id)
            .with_range((start.as_slice(), stop.as_slice()))
            .with_not_after(0)
            .build();
        self.interest
            .insert(interest)
            .await
            .map_err(|err| ApiError(format!("failed to update interest: {err}")))?;

        Ok((start, stop))
    }
}

use ceramic_api_server::server::MakeService;
use ceramic_api_server::{Api, EventsSortKeySortValueGetResponse, FeedEventsGetResponse};
use std::error::Error;
use swagger::ApiError;

use crate::ResumeToken;

// Helper to build responses consistent as we can't implement for the api_server::models directly
struct BuildResponse {}
impl BuildResponse {
    fn event(id: EventId, data: Vec<u8>) -> models::Event {
        let id = multibase::encode(multibase::Base::Base16Lower, id.as_bytes());
        let data = if data.is_empty() {
            String::default()
        } else {
            multibase::encode(multibase::Base::Base64, data)
        };
        models::Event::new(id, data)
    }
}

#[async_trait]
impl<C, I, M> Api<C> for Server<C, I, M>
where
    C: Send + Sync,
    I: AccessInterestStore<Key = Interest> + Sync,
    M: AccessModelStore<Key = EventId> + Sync,
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
        let hw = resume_at.map(ResumeToken::new).unwrap_or_default();
        let limit = limit.unwrap_or(10000) as usize;
        let hw = match (&hw).try_into() {
            Ok(hw) => hw,
            Err(err) => {
                return Ok(FeedEventsGetResponse::BadRequest(format!(
                    "Invalid resume token '{}'. {}",
                    hw, err
                )))
            }
        };
        let (new_hw, event_ids) = self
            .model
            .keys_since_highwater_mark(hw, limit as i64)
            .await
            .map_err(|e| ApiError(format!("failed to get keys: {e}")))?;
        let events = event_ids
            .into_iter()
            .map(|id| BuildResponse::event(id, vec![]))
            .collect();

        Ok(FeedEventsGetResponse::Success(models::EventFeed {
            resume_token: new_hw.to_string(),
            events,
        }))
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
        let limit: usize =
            limit.map_or(10000, |l| if l.is_negative() { 10000 } else { l }) as usize;
        let offset = offset.map_or(0, |o| if o.is_negative() { 0 } else { o }) as usize;
        let (start, stop) =
            self.build_start_stop_range(&sort_key, &sort_value, controller, stream_id)?;

        let events = self
            .model
            .range_with_values(start, stop, offset, limit)
            .await
            .map_err(|err| ApiError(format!("failed to get keys: {err}")))?
            .into_iter()
            .map(|(id, data)| BuildResponse::event(id, data))
            .collect::<Vec<_>>();

        let event_cnt = events.len();
        Ok(EventsSortKeySortValueGetResponse::Success(
            models::EventsGet {
                resume_offset: (offset + event_cnt) as i32,
                events,
                is_complete: event_cnt < limit,
            },
        ))
    }

    #[instrument(skip(self, _context, event), fields(event.id = event.id.as_ref().or(event.event_id.as_ref()), event.data.len = event.data.as_ref().or(event.event_data.as_ref()).map(|d| d.len()).unwrap_or(0)), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn events_post(
        &self,
        event: EventsPostRequest,
        _context: &C,
    ) -> Result<EventsPostResponse, ApiError> {
        // we want to support event_id/event_data and id/data without breaking the API.
        // openAPI generator doesn't support oneOf very well right now (it'd be better as an untagged enum with serde)
        // so we have a temp type that has everything optional that we manually validate.
        let (id, data) = if event.id.is_some() && event.data.is_some() {
            (event.id.unwrap(), event.data.unwrap())
        } else if event.event_id.is_some() && event.event_data.is_some() {
            (event.event_id.unwrap(), event.event_data.unwrap())
        } else {
            return Ok(EventsPostResponse::BadRequest(
                "event fields 'id' and 'data' or 'event_id' and 'event_data' are required"
                    .to_owned(),
            ));
        };

        let event_id = decode_event_id(&id)?;
        let event_data = decode_event_data(&data)?;

        if let Some(network_id) = event_id.network_id() {
            if let Ok(event_network) = Network::try_from_id(network_id) {
                if event_network != self.network {
                    return Ok(EventsPostResponse::BadRequest(format!(
                        "event network does not match node network: {} != {}",
                        event_network, self.network
                    )));
                }
            } else {
                return Ok(EventsPostResponse::BadRequest(format!(
                    "event does not have valid network id {}",
                    network_id,
                )));
            }
        } else {
            return Ok(EventsPostResponse::BadRequest(
                "event does not have network id".to_string(),
            ));
        }

        self.model
            .insert(event_id, Some(event_data))
            .await
            .map_err(|err| ApiError(format!("failed to insert key: {err}")))?;

        Ok(EventsPostResponse::Success)
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
        self.store_interest(sort_key, sort_value, controller, stream_id)
            .await?;
        Ok(InterestsSortKeySortValuePostResponse::Success)
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn events_event_id_get(
        &self,
        event_id: String,
        _context: &C,
    ) -> Result<EventsEventIdGetResponse, ApiError> {
        let decoded_event_id = decode_event_id(&event_id)?;
        match self.model.value_for_key(decoded_event_id.clone()).await {
            Ok(Some(data)) => {
                let event = BuildResponse::event(decoded_event_id, data);
                Ok(EventsEventIdGetResponse::Success(event))
            }
            Ok(None) => Ok(EventsEventIdGetResponse::EventNotFound(format!(
                "Event not found : {}",
                event_id
            ))),
            Err(err) => Err(ApiError(format!("failed to get event: {err}"))),
        }
    }
}

fn decode_event_id(value: &str) -> Result<EventId, ApiError> {
    multibase::decode(value)
        .map_err(|err| ApiError(format!("multibase error: {err}")))?
        .1
        .try_into()
        .map_err(|err| ApiError(format!("invalid event id: {err}")))
}
fn decode_event_data(value: &str) -> Result<Vec<u8>, ApiError> {
    Ok(multibase::decode(value)
        .map_err(|err| ApiError(format!("multibase error: {err}")))?
        .1)
}

#[cfg(test)]
mod tests {
    use super::*;

    use ceramic_core::Cid;
    use expect_test::expect;
    use mockall::{mock, predicate};
    use multibase::Base;
    use recon::Sha256a;
    use test_log::test;
    use tracing::event;

    struct Context;
    mock! {
        pub ReconInterestTest {
            fn insert(&self, key: Interest, value: Option<Vec<u8>>) -> Result<bool>;
            fn range_with_values(
                &self,
                start: Interest,
                end: Interest,
                offset: usize,
                limit: usize,
            ) -> Result<Vec<(Interest, Vec<u8>)>>;
        }

        impl Clone for ReconInterestTest {
            fn clone(&self) -> Self;
        }
    }

    #[async_trait]
    impl AccessInterestStore for MockReconInterestTest {
        type Key = Interest;
        type Hash = Sha256a;
        async fn insert(&self, key: Self::Key) -> Result<bool> {
            self.insert(key, None)
        }
        async fn range(
            &self,
            start: Self::Key,
            end: Self::Key,
            offset: usize,
            limit: usize,
        ) -> Result<Vec<Self::Key>> {
            let res = self.range_with_values(start, end, offset, limit)?;
            Ok(res.into_iter().map(|(k, _)| k).collect())
        }
    }

    mock! {
        pub ReconModelTest {
            fn insert(&self, key: EventId, value: Option<Vec<u8>>) -> Result<(bool, bool)>;
            fn range_with_values(
                &self,
                start: EventId,
                end: EventId,
                offset: usize,
                limit: usize,
            ) -> Result<Vec<(EventId,Vec<u8>)>>;
            fn value_for_key(&self, key: EventId) -> Result<Option<Vec<u8>>>;
        }
        impl Clone for ReconModelTest {
            fn clone(&self) -> Self;
        }
    }

    #[async_trait]
    impl AccessModelStore for MockReconModelTest {
        type Key = EventId;
        type Hash = Sha256a;
        async fn insert(&self, key: Self::Key, value: Option<Vec<u8>>) -> Result<(bool, bool)> {
            self.insert(key, value)
        }
        async fn range_with_values(
            &self,
            start: Self::Key,
            end: Self::Key,
            offset: usize,
            limit: usize,
        ) -> Result<Vec<(Self::Key, Vec<u8>)>> {
            self.range_with_values(start, end, offset, limit)
        }
        async fn value_for_key(&self, key: Self::Key) -> Result<Option<Vec<u8>>> {
            self.value_for_key(key)
        }

        async fn keys_since_highwater_mark(
            &self,
            _highwater: i64,
            _limit: i64,
        ) -> anyhow::Result<(i64, Vec<Self::Key>)> {
            Ok((0, vec![]))
        }
    }

    #[test(tokio::test)]
    async fn create_event() {
        let peer_id = PeerId::random();
        let network = Network::InMemory;
        let event_id = EventId::new(
            &network,
            "model",
            "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9", // cspell:disable-line
            "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1",          // cspell:disable-line
            &Cid::from_str("baejbeihyr3kf77etqdccjfoc33dmko2ijyugn6qk6yucfkioasjssz3bbu").unwrap(), // cspell:disable-line
            0,
            &Cid::from_str("baejbeicqtpe5si4qvbffs2s7vtbk5ccbsfg6owmpidfj3zeluqz4hlnz6m").unwrap(), // cspell:disable-line
        );
        let event_id_str = multibase::encode(Base::Base16Lower, event_id.to_bytes());
        let event_data = "f".to_string();
        let mock_interest = MockReconInterestTest::new();
        let mut mock_model = MockReconModelTest::new();
        mock_model
            .expect_insert()
            .with(
                predicate::eq(event_id),
                predicate::eq(Some(decode_event_data(event_data.as_str()).unwrap())),
            )
            .times(1)
            .returning(|_, _| Ok((true, true)));
        let server = Server::new(peer_id, network, mock_interest, mock_model);
        let resp = server
            .events_post(
                models::EventsPostRequest {
                    event_id: Some(event_id_str),
                    event_data: Some(event_data),
                    data: None,
                    id: None,
                },
                &Context,
            )
            .await
            .unwrap();
        assert!(matches!(resp, EventsPostResponse::Success));
    }

    #[test(tokio::test)]
    async fn register_interest_sort_value() {
        let peer_id = PeerId::random();
        let network = Network::InMemory;
        let model = "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9"; // cspell:disable-line

        // Construct start and end event ids
        let start = EventId::builder()
            .with_network(&network)
            .with_sort_value("model", model)
            .with_min_controller()
            .with_min_init()
            .with_min_event_height()
            .build_fencepost();
        let end = EventId::builder()
            .with_network(&network)
            .with_sort_value("model", model)
            .with_max_controller()
            .with_max_init()
            .with_max_event_height()
            .build_fencepost();

        // Setup mock expectations
        let mut mock_interest = MockReconInterestTest::new();
        mock_interest
            .expect_insert()
            .with(
                predicate::eq(
                    Interest::builder()
                        .with_sort_key("model")
                        .with_peer_id(&peer_id)
                        .with_range((start.as_slice(), end.as_slice()))
                        .with_not_after(0)
                        .build(),
                ),
                predicate::eq(None),
            )
            .times(1)
            .returning(|_, _| Ok(true));
        let mock_model = MockReconModelTest::new();
        let server = Server::new(peer_id, network, mock_interest, mock_model);
        let resp = server
            .interests_sort_key_sort_value_post(
                "model".to_string(),
                model.to_owned(),
                None,
                None,
                &Context,
            )
            .await
            .unwrap();
        assert_eq!(resp, InterestsSortKeySortValuePostResponse::Success);
    }

    #[test(tokio::test)]
    async fn register_interest_sort_value_controller() {
        let peer_id = PeerId::random();
        let network = Network::InMemory;
        let model = "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9"; // cspell:disable-line
        let controller = "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1";
        let start = EventId::builder()
            .with_network(&network)
            .with_sort_value("model", model)
            .with_controller(controller)
            .with_min_init()
            .with_min_event_height()
            .build_fencepost();
        let end = EventId::builder()
            .with_network(&network)
            .with_sort_value("model", model)
            .with_controller(controller)
            .with_max_init()
            .with_max_event_height()
            .build_fencepost();
        let mut mock_interest = MockReconInterestTest::new();
        mock_interest
            .expect_insert()
            .with(
                predicate::eq(
                    Interest::builder()
                        .with_sort_key("model")
                        .with_peer_id(&peer_id)
                        .with_range((start.as_slice(), end.as_slice()))
                        .with_not_after(0)
                        .build(),
                ),
                predicate::eq(None),
            )
            .times(1)
            .returning(|_, _| Ok(true));
        let mock_model = MockReconModelTest::new();
        let server = Server::new(peer_id, network, mock_interest, mock_model);
        let resp = server
            .interests_sort_key_sort_value_post(
                "model".to_string(),
                model.to_owned(),
                Some(controller.to_owned()),
                None,
                &Context,
            )
            .await
            .unwrap();
        assert_eq!(resp, InterestsSortKeySortValuePostResponse::Success);
    }
    #[test(tokio::test)]
    async fn register_interest_value_controller_stream() {
        let peer_id = PeerId::random();
        let network = Network::InMemory;
        let model = "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9"; // cspell:disable-line
        let controller = "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1";
        let stream =
            StreamId::from_str("k2t6wz4ylx0qs435j9oi1s6469uekyk6qkxfcb21ikm5ag2g1cook14ole90aw") // cspell:disable-line
                .unwrap();
        let start = EventId::builder()
            .with_network(&network)
            .with_sort_value("model", model)
            .with_controller(controller)
            .with_init(&stream.cid)
            .with_min_event_height()
            .build_fencepost();
        let end = EventId::builder()
            .with_network(&network)
            .with_sort_value("model", model)
            .with_controller(controller)
            .with_init(&stream.cid)
            .with_max_event_height()
            .build_fencepost();
        let mut mock_interest = MockReconInterestTest::new();
        mock_interest
            .expect_insert()
            .with(
                predicate::eq(
                    Interest::builder()
                        .with_sort_key("model")
                        .with_peer_id(&peer_id)
                        .with_range((start.as_slice(), end.as_slice()))
                        .with_not_after(0)
                        .build(),
                ),
                predicate::eq(None),
            )
            .times(1)
            .returning(|_, _| Ok(true));
        let mock_model = MockReconModelTest::new();
        let server = Server::new(peer_id, network, mock_interest, mock_model);
        let resp = server
            .interests_sort_key_sort_value_post(
                "model".to_string(),
                model.to_owned(),
                Some(controller.to_owned()),
                Some(stream.to_string()),
                &Context,
            )
            .await
            .unwrap();
        assert_eq!(resp, InterestsSortKeySortValuePostResponse::Success);
    }

    #[test(tokio::test)]
    async fn get_events_for_interest_range() {
        let peer_id = PeerId::random();
        let network = Network::InMemory;
        let model = "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9"; // cspell:disable-line
        let controller = "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1";
        let stream =
            StreamId::from_str("k2t6wz4ylx0qs435j9oi1s6469uekyk6qkxfcb21ikm5ag2g1cook14ole90aw") // cspell:disable-line
                .unwrap();
        let start = EventId::builder()
            .with_network(&network)
            .with_sort_value("model", model)
            .with_controller(controller)
            .with_init(&stream.cid)
            .with_min_event_height()
            .build_fencepost();
        let end = EventId::builder()
            .with_network(&network)
            .with_sort_value("model", model)
            .with_controller(controller)
            .with_init(&stream.cid)
            .with_max_event_height()
            .build_fencepost();
        /*
        l: Success(EventsGet { events: [Event { id: "fce0105ff012616e0f0c1e987ef0f772afbe2c7f05c50102bc800", data: "" }, Event { id: "fce0105ff012616e0f0c1e987ef0f772afbe2c7f05c50102bc8ff", data: "" }], resume_offset: 2, is_complete: false })
        r: Success(EventsGet { events: [Event { id: "fce0105ff012616e0f0c1e987ef0f772afbe2c7f05c50102bc800", data: "" }], resume_offset: 1, is_complete: false })
                */
        let mock_interest = MockReconInterestTest::new();
        let expected = BuildResponse::event(start.clone(), vec![]);
        let mut mock_model = MockReconModelTest::new();
        mock_model
            .expect_range_with_values()
            .with(
                predicate::eq(start),
                predicate::eq(end),
                predicate::eq(0),
                predicate::eq(1),
            )
            .times(1)
            .returning(|s, _, _, _| Ok(vec![(s, vec![])]));
        let server = Server::new(peer_id, network, mock_interest, mock_model);
        let resp = server
            .events_sort_key_sort_value_get(
                "model".to_string(),
                model.to_owned(),
                Some(controller.to_owned()),
                Some(stream.to_string()),
                None,
                Some(1),
                &Context,
            )
            .await
            .unwrap();
        assert_eq!(
            resp,
            EventsSortKeySortValueGetResponse::Success(models::EventsGet {
                resume_offset: 1,
                events: vec![expected],
                is_complete: false,
            })
        );
    }

    #[test(tokio::test)]
    async fn test_events_event_id_get_success() {
        let peer_id = PeerId::random();
        let network = Network::InMemory;
        let event_id = EventId::new(
            &network,
            "model",
            "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9", // cspell:disable-line
            "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1",          // cspell:disable-line
            &Cid::from_str("baejbeihyr3kf77etqdccjfoc33dmko2ijyugn6qk6yucfkioasjssz3bbu").unwrap(), // cspell:disable-line
            0,
            &Cid::from_str("baejbeicqtpe5si4qvbffs2s7vtbk5ccbsfg6owmpidfj3zeluqz4hlnz6m").unwrap(), // cspell:disable-line
        );
        let event_id_str = multibase::encode(Base::Base16Lower, event_id.to_bytes());
        let event_data = b"event data".to_vec();
        let event_data_base64 = multibase::encode(multibase::Base::Base64, &event_data);

        let mut mock_model = MockReconModelTest::new();
        mock_model
            .expect_value_for_key()
            .with(predicate::eq(event_id.clone()))
            .times(1)
            .returning(move |_| Ok(Some(event_data.clone())));

        let mock_interest = MockReconInterestTest::new();

        let server = Server::new(peer_id, network, mock_interest, mock_model);

        let result = server.events_event_id_get(event_id_str, &Context).await;

        let EventsEventIdGetResponse::Success(event) = result.unwrap() else {
            panic!("Expected EventsEventIdGetResponse::Success but got another variant");
        };
        assert_eq!(
            event.id,
            multibase::encode(multibase::Base::Base16Lower, event_id.as_bytes())
        );
        assert_eq!(event.data, event_data_base64);
    }

    #[test(tokio::test)]
    async fn test_events_bad_event_network() {
        let peer_id = PeerId::random();
        let network = Network::InMemory;
        let event_id = EventId::new(
            &network,
            "model",
            "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9", // cspell:disable-line
            "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1",          // cspell:disable-line
            &Cid::from_str("baejbeihyr3kf77etqdccjfoc33dmko2ijyugn6qk6yucfkioasjssz3bbu").unwrap(), // cspell:disable-line
            0,
            &Cid::from_str("baejbeicqtpe5si4qvbffs2s7vtbk5ccbsfg6owmpidfj3zeluqz4hlnz6m").unwrap(), // cspell:disable-line
        );
        let event_id_str = multibase::encode(Base::Base16Lower, event_id.to_bytes());
        let event_data = "f".to_string();
        let mock_interest = MockReconInterestTest::new();
        let mock_model = MockReconModelTest::new();
        // Use a differnet network for the server than the event
        let server = Server::new(peer_id, Network::DevUnstable, mock_interest, mock_model);

        let result = server
            .events_post(
                models::EventsPostRequest {
                    id: Some(event_id_str),
                    data: Some(event_data),
                    event_id: None,
                    event_data: None,
                },
                &Context,
            )
            .await
            .unwrap();
        expect![[r#"
            BadRequest(
                "event network does not match node network: /ceramic/inmemory != /ceramic/dev-unstable",
            )
        "#]]
        .assert_debug_eq(&result);
    }
}
