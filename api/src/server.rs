//! Main library entry point for ceramic_api_server implementation.

#![allow(unused_imports)]

use anyhow::bail;
use async_trait::async_trait;
use futures::{future, Stream, StreamExt, TryFutureExt, TryStreamExt};
use hyper::server::conn::Http;
use hyper::service::Service;
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
    CeramicEventsPostResponse, CeramicSubscribeSortValueGetResponse,
};
use ceramic_core::{EventId, Network, StreamId};

/// Builds an SSL implementation for Simple HTTPS from some hard-coded file names
pub async fn create(network: Network, addr: &str, recon: impl Recon) {
    let addr = addr.parse().expect("Failed to parse bind address");

    let server = Server::new(network, recon);

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
    fn insert_key(&self, key: &EventId);
    fn range<R>(&self, range: R) -> Vec<EventId>
    where
        R: RangeBounds<EventId> + 'static;
}

impl Recon for Arc<Mutex<recon::Recon>> {
    fn insert_key(&self, key: &EventId) {
        self.lock()
            .expect("should be able to acquire lock")
            .insert(key)
    }

    fn range<R>(&self, range: R) -> Vec<EventId>
    where
        R: RangeBounds<EventId> + 'static,
    {
        self.lock()
            .expect("should be able to acquire lock")
            .range(range)
            .map(|event| event.to_owned())
            .collect()
    }
}

#[derive(Clone)]
pub struct Server<C, R> {
    network: Network,
    recon: R,
    marker: PhantomData<C>,
}

impl<C, R: Recon> Server<C, R> {
    pub fn new(network: Network, recon: R) -> Self {
        Server {
            network,
            recon,
            marker: PhantomData,
        }
    }
}

use ceramic_api_server::server::MakeService;
use ceramic_api_server::Api;
use std::error::Error;
use swagger::ApiError;

#[async_trait]
impl<C, R> Api<C> for Server<C, R>
where
    C: Send + Sync,
    R: Recon + Sync,
{
    async fn ceramic_events_post(
        &self,
        event: models::Event,
        _context: &C,
    ) -> Result<CeramicEventsPostResponse, ApiError> {
        debug!(event_id = event.event_id, "ceramic_events_post");
        let event_id = decode_event_id(&event.event_id)?;
        self.recon.insert_key(&event_id);
        Ok(CeramicEventsPostResponse::Success)
    }

    async fn ceramic_subscribe_sort_value_get(
        &self,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        _context: &C,
    ) -> Result<CeramicSubscribeSortValueGetResponse, ApiError> {
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
                    .map_err(|err| ApiError(format!("stream_id: {}", err)))?;
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

        debug!(%start, %stop, "subscribe");
        Ok(CeramicSubscribeSortValueGetResponse::Success(
            self.recon
                .range((Bound::Included(start), Bound::Excluded(stop)))
                .into_iter()
                .map(|id| Event {
                    event_id: multibase::encode(multibase::Base::Base16Lower, id.as_slice()),
                })
                .collect(),
        ))
    }
}

fn decode_event_id(value: &str) -> Result<EventId, ApiError> {
    Ok(multibase::decode(value)
        .map_err(|err| ApiError(format!("multibase error: {}", err)))?
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
        ReconTest {}
        impl Recon for ReconTest {
            fn insert_key(&self, key: &EventId);

            fn range<R>(&self, range: R) -> Vec<EventId>
            where
                R: RangeBounds<EventId> + 'static;
        }
        impl Clone for ReconTest {
            fn clone(&self) -> Self;
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn create_event() {
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
        let mut mock = MockReconTest::new();
        mock.expect_insert_key()
            .with(predicate::eq(event_id))
            .times(1)
            .returning(|_| ());
        let server = Server::new(network, mock);
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
        let mut mock = MockReconTest::new();
        mock.expect_range()
            .with(predicate::eq((
                Bound::Included(start),
                Bound::Excluded(end),
            )))
            .times(1)
            .returning(move |_| vec![event_id.clone()]);
        let server = Server::new(network, mock);
        let resp = server
            .ceramic_subscribe_sort_value_get(model.to_owned(), None, None, &Context)
            .await
            .unwrap();
        assert_eq!(
            resp,
            CeramicSubscribeSortValueGetResponse::Success(vec![event])
        );
    }
    #[tokio::test]
    #[traced_test]
    async fn subscribe_sort_value_controller() {
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
        let mut mock = MockReconTest::new();
        mock.expect_range()
            .with(predicate::eq((
                Bound::Included(start),
                Bound::Excluded(end),
            )))
            .times(1)
            .returning(|_| vec![]);
        let server = Server::new(network, mock);
        let resp = server
            .ceramic_subscribe_sort_value_get(
                model.to_owned(),
                Some(controller.to_owned()),
                None,
                &Context,
            )
            .await
            .unwrap();
        assert_eq!(resp, CeramicSubscribeSortValueGetResponse::Success(vec![]));
    }
    #[tokio::test]
    #[traced_test]
    async fn subscribe_sort_value_controller_stream() {
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
        let mut mock = MockReconTest::new();
        mock.expect_range()
            .with(predicate::eq((
                Bound::Included(start),
                Bound::Excluded(end),
            )))
            .times(1)
            .returning(|_| vec![]);
        let server = Server::new(network, mock);
        let resp = server
            .ceramic_subscribe_sort_value_get(
                model.to_owned(),
                Some(controller.to_owned()),
                Some(stream.to_string()),
                &Context,
            )
            .await
            .unwrap();
        assert_eq!(resp, CeramicSubscribeSortValueGetResponse::Success(vec![]));
    }
}
