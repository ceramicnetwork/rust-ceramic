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
use ceramic_api_server::models::EventDeprecated;
use futures::{future, Stream, StreamExt, TryFutureExt, TryStreamExt};
use hyper::service::Service;
use hyper::{server::conn::Http, Request};
use recon::{AssociativeHash, InterestProvider, Key, Store};
use serde::{Deserialize, Serialize};
use swagger::{ByteArray, EmptyContext, XSpanIdString};
use tokio::net::TcpListener;
use tracing::{debug, info, instrument, Level};

use ceramic_api_server::{
    models::{self, Event},
    EventsPostResponse, InterestsSortKeySortValuePostResponse, LivenessGetResponse,
    SubscribeSortKeySortValueGetResponse, VersionPostResponse,
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

    async fn store_interest(
        &self,
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
    ) -> Result<(EventId, EventId), ApiError> {
        // Construct start and stop event id based on provided data.
        let start_builder = EventId::builder()
            .with_network(&self.network)
            .with_sort_value(&sort_key, &sort_value);
        let stop_builder = EventId::builder()
            .with_network(&self.network)
            .with_sort_value(&sort_key, &sort_value);

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
use ceramic_api_server::{Api, FeedEventsGetResponse};
use std::error::Error;
use swagger::ApiError;

use crate::ResumeToken;

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
            .map(|id| Event {
                id: multibase::encode(multibase::Base::Base16Lower, id.as_bytes()),
                data: String::default(),
            })
            .collect();

        Ok(FeedEventsGetResponse::Success(models::EventFeed {
            resume_token: new_hw.to_string(),
            events,
        }))
    }
    #[instrument(skip(self, _context, event), fields(event.id = event.event_id, event.data.len = event.event_data.len()), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn events_post(
        &self,
        event: EventDeprecated,
        _context: &C,
    ) -> Result<EventsPostResponse, ApiError> {
        let event_id = decode_event_id(&event.event_id)?;
        let event_data = decode_event_data(&event.event_data)?;
        self.model
            .insert(event_id.clone(), Some(event_data))
            .await
            .map_err(|err| ApiError(format!("failed to insert key: {err}")))?;

        Ok(EventsPostResponse::Success)
    }

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn subscribe_sort_key_sort_value_get(
        &self,
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        offset: Option<f64>,
        limit: Option<f64>,
        _context: &C,
    ) -> Result<SubscribeSortKeySortValueGetResponse, ApiError> {
        debug!(
            ?self.network,
            sort_key, sort_value, controller, "subscribe params"
        );
        let offset = offset
            .map(|float| {
                let int = float as usize;
                if int as f64 == float {
                    Ok(int)
                } else {
                    Err(ApiError("offset must be an integer".to_owned()))
                }
            })
            .transpose()?
            .unwrap_or(0usize);
        let limit = limit
            .map(|float| {
                let int = float as usize;
                if int as f64 == float {
                    Ok(int)
                } else {
                    Err(ApiError("limit must be an integer".to_owned()))
                }
            })
            .transpose()?
            .unwrap_or(usize::MAX);

        let (start, stop) = self
            .store_interest(sort_key, sort_value, controller, stream_id)
            .await?;

        let events = self
            .model
            .range_with_values(start, stop, offset, limit)
            .await
            .map_err(|err| ApiError(format!("failed to get keys: {err}")))?
            .into_iter()
            .map(|(id, data)| EventDeprecated {
                event_id: multibase::encode(multibase::Base::Base16Lower, id.as_bytes()),
                event_data: multibase::encode(multibase::Base::Base64, data),
            })
            .collect();
        Ok(SubscribeSortKeySortValueGetResponse::Success(events))
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
}

fn decode_event_id(value: &str) -> Result<EventId, ApiError> {
    Ok(multibase::decode(value)
        .map_err(|err| ApiError(format!("multibase error: {err}")))?
        .1
        .into())
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
    use tracing_test::traced_test;

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
        async fn value_for_key(&self, _key: Self::Key) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn keys_since_highwater_mark(
            &self,
            _highwater: i64,
            _limit: i64,
        ) -> anyhow::Result<(i64, Vec<Self::Key>)> {
            Ok((0, vec![]))
        }
    }

    #[tokio::test]
    #[traced_test]
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
                models::EventDeprecated {
                    event_id: event_id_str,
                    event_data,
                },
                &Context,
            )
            .await
            .unwrap();
        assert!(matches!(resp, EventsPostResponse::Success));
    }

    #[tokio::test]
    #[traced_test]
    async fn subscribe_sort_value() {
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

        // Construct event to be returned
        let event_id = EventId::builder()
            .with_network(&network)
            .with_sort_value("model", model)
            .with_controller("did:key:zH3RdYVn3WtYR3LXmXk8wA6XWUjWMLgRa5ia1tgR3uR9D")
            .with_init(
                &StreamId::from_str(
                    "k2t6wz4ylx0qmwkzy91072jnjj1w0jvxlvyatf5jox9om5wp69euipjoqdejkd", // cspell:disable-line
                )
                .unwrap()
                .cid,
            )
            .with_event_height(9)
            .with_event(
                &Cid::from_str("baejbeibmbnp2ffyug3cfamsw7cno3plz5qkwfednrkyse6azcz7fycdym4") // cspell:disable-line
                    .unwrap(),
            )
            .build();
        let event_data = b"hello world";
        let event = models::EventDeprecated {
            event_id: multibase::encode(multibase::Base::Base16Lower, event_id.as_slice()),
            event_data: multibase::encode(multibase::Base::Base64, event_data),
        };
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
        let mut mock_model = MockReconModelTest::new();
        mock_model
            .expect_range_with_values()
            .with(
                predicate::eq(start),
                predicate::eq(end),
                predicate::eq(0),
                predicate::eq(usize::MAX),
            )
            .times(1)
            .returning(move |_, _, _, _| Ok(vec![(event_id.clone(), event_data.into())]));
        let server = Server::new(peer_id, network, mock_interest, mock_model);
        let resp = server
            .subscribe_sort_key_sort_value_get(
                "model".to_string(),
                model.to_owned(),
                None,
                None,
                None,
                None,
                &Context,
            )
            .await
            .unwrap();
        assert_eq!(
            resp,
            SubscribeSortKeySortValueGetResponse::Success(vec![event])
        );
    }
    #[tokio::test]
    #[traced_test]
    async fn subscribe_sort_value_controller() {
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
        let mut mock_model = MockReconModelTest::new();
        mock_model
            .expect_range_with_values()
            .with(
                predicate::eq(start),
                predicate::eq(end),
                predicate::eq(0),
                predicate::eq(usize::MAX),
            )
            .times(1)
            .returning(|_, _, _, _| Ok(vec![]));
        let server = Server::new(peer_id, network, mock_interest, mock_model);
        let resp = server
            .subscribe_sort_key_sort_value_get(
                "model".to_string(),
                model.to_owned(),
                Some(controller.to_owned()),
                None,
                None,
                None,
                &Context,
            )
            .await
            .unwrap();
        assert_eq!(resp, SubscribeSortKeySortValueGetResponse::Success(vec![]));
    }
    #[tokio::test]
    #[traced_test]
    async fn subscribe_sort_value_controller_stream() {
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
        let mut mock_model = MockReconModelTest::new();
        mock_model
            .expect_range_with_values()
            .with(
                predicate::eq(start),
                predicate::eq(end),
                predicate::eq(0),
                predicate::eq(usize::MAX),
            )
            .times(1)
            .returning(|_, _, _, _| Ok(vec![]));
        let server = Server::new(peer_id, network, mock_interest, mock_model);
        let resp = server
            .subscribe_sort_key_sort_value_get(
                "model".to_string(),
                model.to_owned(),
                Some(controller.to_owned()),
                Some(stream.to_string()),
                None,
                None,
                &Context,
            )
            .await
            .unwrap();
        assert_eq!(resp, SubscribeSortKeySortValueGetResponse::Success(vec![]));
    }
    #[tokio::test]
    #[traced_test]
    async fn subscribe_sort_value_controller_stream_offset_limit() {
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
        let mut mock_model = MockReconModelTest::new();
        mock_model
            .expect_range_with_values()
            .with(
                predicate::eq(start),
                predicate::eq(end),
                predicate::eq(100),
                predicate::eq(200),
            )
            .times(1)
            .returning(|_, _, _, _| Ok(vec![]));
        let server = Server::new(peer_id, network, mock_interest, mock_model);
        let resp = server
            .subscribe_sort_key_sort_value_get(
                "model".to_string(),
                model.to_owned(),
                Some(controller.to_owned()),
                Some(stream.to_string()),
                Some(100f64),
                Some(200f64),
                &Context,
            )
            .await
            .unwrap();
        assert_eq!(resp, SubscribeSortKeySortValueGetResponse::Success(vec![]));
    }

    #[tokio::test]
    #[traced_test]
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

    #[tokio::test]
    #[traced_test]
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
    #[tokio::test]
    #[traced_test]
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
}
