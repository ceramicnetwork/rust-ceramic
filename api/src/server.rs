//! Main library entry point for ceramic_api_server implementation.
//! o

#![allow(unused_imports)]

use anyhow::{bail, Result};
use async_trait::async_trait;
use futures::{future, Stream, StreamExt, TryFutureExt, TryStreamExt};
use hyper::server::conn::Http;
use hyper::service::Service;
use recon::{AssociativeHash, InterestProvider, Key, Store};
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
use swagger::auth::MakeAllowAllAuthenticator;
use swagger::EmptyContext;
use tokio::net::TcpListener;
use tracing::{debug, info};
use unimock::unimock;

use ceramic_api_server::{
    models::{self, Event},
    CeramicEventsPostResponse, CeramicSubscribeSortKeySortValueGetResponse,
};
use ceramic_core::{EventId, Interest, Network, PeerId, StreamId};

/// Builds an SSL implementation for Simple HTTPS from some hard-coded file names
pub async fn create(
    peer_id: PeerId,
    network: Network,
    addr: &str,
    interest: impl Recon<Key = Interest> + 'static,
    model: impl Recon<Key = EventId> + 'static,
) {
    let addr = addr.parse().expect("Failed to parse bind address");

    let server = Server::new(peer_id, network, interest, model);

    let service = MakeService::new(server);

    let service = MakeAllowAllAuthenticator::new(service, "cosmo");

    #[allow(unused_mut)]
    let mut service =
        ceramic_api_server::server::context::MakeAddContext::<_, EmptyContext>::new(service);

    // Using HTTP
    hyper::server::Server::bind(&addr)
        .serve(service)
        .await
        .unwrap()
}

pub trait Recon: Clone + Send + Sync {
    type Key: Key + Send;
    fn insert(&self, key: &Self::Key) -> Result<()>;
    fn range(
        &self,
        start: &Self::Key,
        end: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Vec<Self::Key>;
}

impl<K, H, S, I> Recon for Arc<Mutex<recon::Recon<K, H, S, I>>>
where
    K: Key + Send,
    H: AssociativeHash,
    S: Store<Key = K, Hash = H> + Send,
    I: InterestProvider<Key = K> + Send,
{
    type Key = K;

    fn insert(&self, key: &Self::Key) -> Result<()> {
        self.lock()
            .expect("should be able to acquire lock")
            .insert(key)
    }

    fn range(
        &self,
        start: &Self::Key,
        end: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Vec<Self::Key> {
        self.lock()
            .expect("should be able to acquire lock")
            .range(start, end, offset, limit)
            .map(|event| event.to_owned())
            .collect()
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
    async fn ceramic_events_post(
        &self,
        event: models::Event,
        _context: &C,
    ) -> Result<CeramicEventsPostResponse, ApiError> {
        debug!(event_id = event.event_id, "ceramic_events_post");
        let event_id = decode_event_id(&event.event_id)?;
        self.model
            .insert(&event_id)
            .map_err(|err| ApiError(format!("failed to insert key: {err}")))?;
        Ok(CeramicEventsPostResponse::Success)
    }

    async fn ceramic_subscribe_sort_key_sort_value_get(
        &self,
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        offset: Option<f64>,
        limit: Option<f64>,
        _context: &C,
    ) -> Result<CeramicSubscribeSortKeySortValueGetResponse, ApiError> {
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
        // Construct start and stop event id based on provided data.
        let start_builder = EventId::builder()
            .with_network(&self.network)
            .with_sort_value(&sort_value);
        let stop_builder = EventId::builder()
            .with_network(&self.network)
            .with_sort_value(&sort_value);
        let (start_builder, stop_builder) = if let Some(controller) = controller {
            if let Some(stream_id) = stream_id {
                let stream_id = StreamId::from_str(&stream_id)
                    .map_err(|err| ApiError(format!("stream_id: {err}")))?;
                // We have both controller and stream id
                (
                    start_builder
                        .with_controller(&controller)
                        .with_init(&stream_id.cid),
                    stop_builder
                        .with_controller(&controller)
                        .with_init(&stream_id.cid),
                )
            } else {
                // We have a controller without a stream id
                (
                    start_builder.with_controller(&controller).with_min_init(),
                    stop_builder.with_controller(&controller).with_max_init(),
                )
            }
        } else {
            if stream_id.is_some() {
                return Err(ApiError(
                    "controller is required if stream_id is specified".to_owned(),
                ));
            }
            // We have no controller or stream id
            (
                start_builder.with_min_controller().with_min_init(),
                stop_builder.with_max_controller().with_max_init(),
            )
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
        debug!(
            ?interest,
            ?sort_key, ?self.peer_id, ?start, ?stop, "saving interest"
        );
        self.interest
            .insert(&interest)
            .map_err(|err| ApiError(format!("failed to update interest: {err}")))?;

        Ok(CeramicSubscribeSortKeySortValueGetResponse::Success(
            self.model
                .range(&start, &stop, offset, limit)
                .into_iter()
                .map(|id| Event {
                    event_id: multibase::encode(multibase::Base::Base16Lower, id.as_bytes()),
                })
                .collect(),
        ))
    }
}

fn decode_event_id(value: &str) -> Result<EventId, ApiError> {
    Ok(multibase::decode(value)
        .map_err(|err| ApiError(format!("multibase error: {err}")))?
        .1
        .into())
}

#[cfg(test)]
mod tests {
    use super::*;

    use ceramic_core::Cid;
    use expect_test::expect;
    use mockall::{mock, predicate};
    use multibase::Base;
    use tracing_test::traced_test;
    use unimock::{matching, MockFn, Unimock};

    struct Context;
    mock! {
        ReconInterestTest {}
        impl Recon for ReconInterestTest
        {
            type Key = Interest;

            fn insert(&self, key: &Interest) -> Result<()>;

            fn range(
                &self,
                start: &Interest,
                end: &Interest,
                offset: usize,
                limit: usize,
            ) -> Vec<Interest>;
        }
        impl Clone for ReconInterestTest {
            fn clone(&self) -> Self;
        }
    }
    mock! {
        ReconModelTest {}
        impl Recon for ReconModelTest
        {
            type Key = EventId;

            fn insert(&self, key: &EventId) -> Result<()>;

            fn range(
                &self,
                start: &EventId,
                end: &EventId,
                offset: usize,
                limit: usize,
            ) -> Vec<EventId>;
        }
        impl Clone for ReconModelTest {
            fn clone(&self) -> Self;
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn create_event() {
        let peer_id = PeerId::random();
        let network = Network::InMemory;
        let event_id = EventId::new(
            &network,
            "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9",
            "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1",
            &Cid::from_str("baejbeihyr3kf77etqdccjfoc33dmko2ijyugn6qk6yucfkioasjssz3bbu").unwrap(),
            0,
            &Cid::from_str("baejbeicqtpe5si4qvbffs2s7vtbk5ccbsfg6owmpidfj3zeluqz4hlnz6m").unwrap(),
        );
        let event_id_str = multibase::encode(Base::Base16Lower, event_id.to_bytes());
        let mock_interest = MockReconInterestTest::new();
        let mut mock_model = MockReconModelTest::new();
        mock_model
            .expect_insert()
            .with(predicate::eq(event_id))
            .times(1)
            .returning(|_| Ok(()));
        let server = Server::new(peer_id, network, mock_interest, mock_model);
        let resp = server
            .ceramic_events_post(
                models::Event {
                    event_id: event_id_str,
                },
                &Context,
            )
            .await
            .unwrap();
        assert!(matches!(resp, CeramicEventsPostResponse::Success));
    }
    #[tokio::test]
    #[traced_test]
    async fn subscribe_sort_value() {
        let peer_id = PeerId::random();
        let network = Network::InMemory;
        let model = "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9";
        // Construct start and end event ids
        let start = EventId::builder()
            .with_network(&network)
            .with_sort_value(model)
            .with_min_controller()
            .with_min_init()
            .with_min_event_height()
            .build_fencepost();
        let end = EventId::builder()
            .with_network(&network)
            .with_sort_value(model)
            .with_max_controller()
            .with_max_init()
            .with_max_event_height()
            .build_fencepost();

        // Construct event to be returned
        let event_id = EventId::builder()
            .with_network(&network)
            .with_sort_value(model)
            .with_controller("did:key:zH3RdYVn3WtYR3LXmXk8wA6XWUjWMLgRa5ia1tgR3uR9D")
            .with_init(
                &StreamId::from_str(
                    "k2t6wz4ylx0qmwkzy91072jnjj1w0jvxlvyatf5jox9om5wp69euipjoqdejkd",
                )
                .unwrap()
                .cid,
            )
            .with_event_height(9)
            .with_event(
                &Cid::from_str("baejbeibmbnp2ffyug3cfamsw7cno3plz5qkwfednrkyse6azcz7fycdym4")
                    .unwrap(),
            )
            .build();
        let event = models::Event {
            event_id: multibase::encode(multibase::Base::Base16Lower, event_id.as_slice()),
        };
        // Setup mock expectations
        let mut mock_interest = MockReconInterestTest::new();
        mock_interest
            .expect_insert()
            .with(predicate::eq(
                Interest::builder()
                    .with_sort_key("model")
                    .with_peer_id(&peer_id)
                    .with_range((start.as_slice(), end.as_slice()))
                    .with_not_after(0)
                    .build(),
            ))
            .times(1)
            .returning(|_| Ok(()));
        let mut mock_model = MockReconModelTest::new();
        mock_model
            .expect_range()
            .with(
                predicate::eq(start),
                predicate::eq(end),
                predicate::eq(0),
                predicate::eq(usize::MAX),
            )
            .times(1)
            .returning(move |_, _, _, _| vec![event_id.clone()]);
        let server = Server::new(peer_id, network, mock_interest, mock_model);
        let resp = server
            .ceramic_subscribe_sort_key_sort_value_get(
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
            CeramicSubscribeSortKeySortValueGetResponse::Success(vec![event])
        );
    }
    #[tokio::test]
    #[traced_test]
    async fn subscribe_sort_value_controller() {
        let peer_id = PeerId::random();
        let network = Network::InMemory;
        let model = "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9";
        let controller = "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1";
        let start = EventId::builder()
            .with_network(&network)
            .with_sort_value(model)
            .with_controller(controller)
            .with_min_init()
            .with_min_event_height()
            .build_fencepost();
        let end = EventId::builder()
            .with_network(&network)
            .with_sort_value(model)
            .with_controller(controller)
            .with_max_init()
            .with_max_event_height()
            .build_fencepost();
        let mut mock_interest = MockReconInterestTest::new();
        mock_interest
            .expect_insert()
            .with(predicate::eq(
                Interest::builder()
                    .with_sort_key("model")
                    .with_peer_id(&peer_id)
                    .with_range((start.as_slice(), end.as_slice()))
                    .with_not_after(0)
                    .build(),
            ))
            .times(1)
            .returning(|_| Ok(()));
        let mut mock_model = MockReconModelTest::new();
        mock_model
            .expect_range()
            .with(
                predicate::eq(start),
                predicate::eq(end),
                predicate::eq(0),
                predicate::eq(usize::MAX),
            )
            .times(1)
            .returning(|_, _, _, _| vec![]);
        let server = Server::new(peer_id, network, mock_interest, mock_model);
        let resp = server
            .ceramic_subscribe_sort_key_sort_value_get(
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
        assert_eq!(
            resp,
            CeramicSubscribeSortKeySortValueGetResponse::Success(vec![])
        );
    }
    #[tokio::test]
    #[traced_test]
    async fn subscribe_sort_value_controller_stream() {
        let peer_id = PeerId::random();
        let network = Network::InMemory;
        let model = "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9";
        let controller = "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1";
        let stream =
            StreamId::from_str("k2t6wz4ylx0qs435j9oi1s6469uekyk6qkxfcb21ikm5ag2g1cook14ole90aw")
                .unwrap();
        let start = EventId::builder()
            .with_network(&network)
            .with_sort_value(model)
            .with_controller(controller)
            .with_init(&stream.cid)
            .with_min_event_height()
            .build_fencepost();
        let end = EventId::builder()
            .with_network(&network)
            .with_sort_value(model)
            .with_controller(controller)
            .with_init(&stream.cid)
            .with_max_event_height()
            .build_fencepost();
        let mut mock_interest = MockReconInterestTest::new();
        mock_interest
            .expect_insert()
            .with(predicate::eq(
                Interest::builder()
                    .with_sort_key("model")
                    .with_peer_id(&peer_id)
                    .with_range((start.as_slice(), end.as_slice()))
                    .with_not_after(0)
                    .build(),
            ))
            .times(1)
            .returning(|_| Ok(()));
        let mut mock_model = MockReconModelTest::new();
        mock_model
            .expect_range()
            .with(
                predicate::eq(start),
                predicate::eq(end),
                predicate::eq(0),
                predicate::eq(usize::MAX),
            )
            .times(1)
            .returning(|_, _, _, _| vec![]);
        let server = Server::new(peer_id, network, mock_interest, mock_model);
        let resp = server
            .ceramic_subscribe_sort_key_sort_value_get(
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
        assert_eq!(
            resp,
            CeramicSubscribeSortKeySortValueGetResponse::Success(vec![])
        );
    }
    #[tokio::test]
    #[traced_test]
    async fn subscribe_sort_value_controller_stream_offset_limit() {
        let peer_id = PeerId::random();
        let network = Network::InMemory;
        let model = "k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9";
        let controller = "did:key:zGs1Det7LHNeu7DXT4nvoYrPfj3n6g7d6bj2K4AMXEvg1";
        let stream =
            StreamId::from_str("k2t6wz4ylx0qs435j9oi1s6469uekyk6qkxfcb21ikm5ag2g1cook14ole90aw")
                .unwrap();
        let start = EventId::builder()
            .with_network(&network)
            .with_sort_value(model)
            .with_controller(controller)
            .with_init(&stream.cid)
            .with_min_event_height()
            .build_fencepost();
        let end = EventId::builder()
            .with_network(&network)
            .with_sort_value(model)
            .with_controller(controller)
            .with_init(&stream.cid)
            .with_max_event_height()
            .build_fencepost();
        let mut mock_interest = MockReconInterestTest::new();
        mock_interest
            .expect_insert()
            .with(predicate::eq(
                Interest::builder()
                    .with_sort_key("model")
                    .with_peer_id(&peer_id)
                    .with_range((start.as_slice(), end.as_slice()))
                    .with_not_after(0)
                    .build(),
            ))
            .times(1)
            .returning(|_| Ok(()));
        let mut mock_model = MockReconModelTest::new();
        mock_model
            .expect_range()
            .with(
                predicate::eq(start),
                predicate::eq(end),
                predicate::eq(100),
                predicate::eq(200),
            )
            .times(1)
            .returning(|_, _, _, _| vec![]);
        let server = Server::new(peer_id, network, mock_interest, mock_model);
        let resp = server
            .ceramic_subscribe_sort_key_sort_value_get(
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
        assert_eq!(
            resp,
            CeramicSubscribeSortKeySortValueGetResponse::Success(vec![])
        );
    }
}
