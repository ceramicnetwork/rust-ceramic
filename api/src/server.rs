//! Main library entry point for ceramic_api_server implementation.

#![allow(unused_imports)]

use anyhow::{bail, Result};
use async_trait::async_trait;
use futures::{future, Stream, StreamExt, TryFutureExt, TryStreamExt};
use hyper::service::Service;
use hyper::{server::conn::Http, Request};
use recon::{AssociativeHash, InterestProvider, Key, Store};
use serde::{Deserialize, Serialize};
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
use swagger::{ByteArray, EmptyContext, XSpanIdString};
use tokio::net::TcpListener;
use tracing::{debug, info, instrument, Level};

use ceramic_api_server::{
    models::{self, Event},
    EventsEventIdGetResponse, EventsPostResponse, InterestsSortKeySortValuePostResponse,
    LivenessGetResponse, SubscribeSortKeySortValueGetResponse, VersionPostResponse,
};
use ceramic_core::{EventId, Interest, Network, PeerId, StreamId};

#[async_trait]
pub trait Recon: Clone + Send + Sync {
    type Key: Key;
    type Hash: AssociativeHash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>;

    async fn insert(&self, key: Self::Key, value: Option<Vec<u8>>) -> Result<()>;
    async fn range_with_values(
        &self,
        start: Self::Key,
        end: Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(Self::Key, Vec<u8>)>>;

    async fn value_for_key(&self, key: Self::Key) -> Result<Option<Vec<u8>>>;
}

#[async_trait]
impl<K, H> Recon for recon::Client<K, H>
where
    K: Key,
    H: AssociativeHash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>,
{
    type Key = K;
    type Hash = H;

    async fn insert(&self, key: Self::Key, value: Option<Vec<u8>>) -> Result<()> {
        let _ = recon::Client::insert(self, key, value).await?;
        Ok(())
    }

    async fn range_with_values(
        &self,
        start: Self::Key,
        end: Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(Self::Key, Vec<u8>)>> {
        Ok(
            recon::Client::range_with_values(self, start, end, offset, limit)
                .await?
                .collect(),
        )
    }
    async fn value_for_key(&self, key: Self::Key) -> Result<Option<Vec<u8>>> {
        recon::Client::value_for_key(self, key).await
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
    I: Recon<Key = Interest>,
    M: Recon<Key = EventId>,
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
            // We must store a value for the interest otherwise Recon will try forever to
            // synchronize the value.
            // In the case of interests an empty value is sufficient.
            .insert(interest, Some(vec![]))
            .await
            .map_err(|err| ApiError(format!("failed to update interest: {err}")))?;

        Ok((start, stop))
    }
}

use ceramic_api_server::server::MakeService;
use ceramic_api_server::Api;
use std::error::Error;
use swagger::ApiError;

#[async_trait]
impl<C, I, M> Api<C> for Server<C, I, M>
where
    C: Send + Sync,
    I: Recon<Key = Interest> + Sync,
    M: Recon<Key = EventId> + Sync,
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

    #[instrument(skip(self, _context, event), fields(event.id = event.event_id, event.data.len = event.event_data.len()), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn events_post(
        &self,
        event: Event,
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
            .map(|(id, data)| Event {
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

    #[instrument(skip(self, _context), ret(level = Level::DEBUG), err(level = Level::ERROR))]
    async fn events_event_id_get(
        &self,
        event_id: String,
        _context: &C,
    ) -> Result<EventsEventIdGetResponse, ApiError> {
        let decoded_event_id = decode_event_id(&event_id)?;
        match self.model.value_for_key(decoded_event_id.clone()).await {
            Ok(Some(data)) => {
                let event = Event {
                    event_id: multibase::encode(
                        multibase::Base::Base16Lower,
                        decoded_event_id.as_bytes(),
                    ),
                    event_data: multibase::encode(multibase::Base::Base64, data),
                };
                Ok(EventsEventIdGetResponse::Success(event))
            }
            Ok(None) => Err(ApiError("Event not found".to_owned())),
            Err(err) => Err(ApiError(format!("failed to get event: {err}"))),
        }
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
    use tracing::event;
    use tracing_test::traced_test;

    struct Context;
    mock! {
        pub ReconInterestTest {
            fn insert(&self, key: Interest, value: Option<Vec<u8>>) -> Result<()>;
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
    impl Recon for MockReconInterestTest {
        type Key = Interest;
        type Hash = Sha256a;
        async fn insert(&self, key: Self::Key, value: Option<Vec<u8>>) -> Result<()> {
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
    }

    mock! {
        pub ReconModelTest {
            fn insert(&self, key: EventId, value: Option<Vec<u8>>) -> Result<()>;
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
    impl Recon for MockReconModelTest {
        type Key = EventId;
        type Hash = Sha256a;
        async fn insert(&self, key: Self::Key, value: Option<Vec<u8>>) -> Result<()> {
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
            .returning(|_, _| Ok(()));
        let server = Server::new(peer_id, network, mock_interest, mock_model);
        let resp = server
            .events_post(
                models::Event {
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
        let event = models::Event {
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
                predicate::eq(Some(vec![])),
            )
            .times(1)
            .returning(|_, _| Ok(()));
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
                predicate::eq(Some(vec![])),
            )
            .times(1)
            .returning(|_, _| Ok(()));
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
                predicate::eq(Some(vec![])),
            )
            .times(1)
            .returning(|_, _| Ok(()));
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
                predicate::eq(Some(vec![])),
            )
            .times(1)
            .returning(|_, _| Ok(()));
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
                predicate::eq(Some(vec![])),
            )
            .times(1)
            .returning(|_, _| Ok(()));
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
                predicate::eq(Some(vec![])),
            )
            .times(1)
            .returning(|_, _| Ok(()));
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
                predicate::eq(Some(vec![])),
            )
            .times(1)
            .returning(|_, _| Ok(()));
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

    #[tokio::test]
    #[traced_test]
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

        let EventsEventIdGetResponse::Success(event) = result.unwrap();
        assert_eq!(
            event.event_id,
            multibase::encode(multibase::Base::Base16Lower, event_id.as_bytes())
        );
        assert_eq!(event.event_data, event_data_base64);
    }
}
