#![allow(suspicious_double_ref_op)]
//! Main library entry point for ceramic_api_server implementation.

#![allow(unused_imports)]

use async_trait::async_trait;
use futures::{future, Stream, StreamExt, TryFutureExt, TryStreamExt};
use hyper::server::conn::Http;
use hyper::service::Service;
use log::info;
use std::future::Future;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use swagger::auth::MakeAllowAllAuthenticator;
use swagger::EmptyContext;
use swagger::{Has, XSpanIdString};
use tokio::net::TcpListener;

#[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "ios")))]
use openssl::ssl::{Ssl, SslAcceptor, SslAcceptorBuilder, SslFiletype, SslMethod};

use ceramic_api_server::models;

/// Builds an SSL implementation for Simple HTTPS from some hard-coded file names
pub async fn create(addr: &str, https: bool) {
    let addr = addr.parse().expect("Failed to parse bind address");

    let server = Server::new();

    let service = MakeService::new(server);

    let service = MakeAllowAllAuthenticator::new(service, "cosmo");

    #[allow(unused_mut)]
    let mut service =
        ceramic_api_server::server::context::MakeAddContext::<_, EmptyContext>::new(service);

    if https {
        #[cfg(any(target_os = "macos", target_os = "windows", target_os = "ios"))]
        {
            unimplemented!("SSL is not implemented for the examples on MacOS, Windows or iOS");
        }

        #[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "ios")))]
        {
            let mut ssl = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())
                .expect("Failed to create SSL Acceptor");

            // Server authentication
            ssl.set_private_key_file("examples/server-key.pem", SslFiletype::PEM)
                .expect("Failed to set private key");
            ssl.set_certificate_chain_file("examples/server-chain.pem")
                .expect("Failed to set certificate chain");
            ssl.check_private_key()
                .expect("Failed to check private key");

            let tls_acceptor = ssl.build();
            let tcp_listener = TcpListener::bind(&addr).await.unwrap();

            loop {
                if let Ok((tcp, _)) = tcp_listener.accept().await {
                    let ssl = Ssl::new(tls_acceptor.context()).unwrap();
                    let addr = tcp.peer_addr().expect("Unable to get remote address");
                    let service = service.call(addr);

                    tokio::spawn(async move {
                        let tls = tokio_openssl::SslStream::new(ssl, tcp).map_err(|_| ())?;
                        let service = service.await.map_err(|_| ())?;

                        Http::new()
                            .serve_connection(tls, service)
                            .await
                            .map_err(|_| ())
                    });
                }
            }
        }
    } else {
        // Using HTTP
        hyper::server::Server::bind(&addr)
            .serve(service)
            .await
            .unwrap()
    }
}

#[derive(Copy, Clone)]
pub struct Server<C> {
    marker: PhantomData<C>,
}

impl<C> Server<C> {
    pub fn new() -> Self {
        Server {
            marker: PhantomData,
        }
    }
}

use ceramic_api_server::server::MakeService;
use ceramic_api_server::{
    Api, ConfigNetworkGetResponse, ConfigNetworkOptionsResponse, DebugHeapGetResponse,
    DebugHeapOptionsResponse, EventsEventIdGetResponse, EventsEventIdOptionsResponse,
    EventsOptionsResponse, EventsPostResponse, ExperimentalEventsSepSepValueGetResponse,
    ExperimentalEventsSepSepValueOptionsResponse, ExperimentalInterestsGetResponse,
    ExperimentalInterestsOptionsResponse, FeedEventsGetResponse, FeedEventsOptionsResponse,
    FeedResumeTokenGetResponse, FeedResumeTokenOptionsResponse, InterestsOptionsResponse,
    InterestsPostResponse, InterestsSortKeySortValueOptionsResponse,
    InterestsSortKeySortValuePostResponse, LivenessGetResponse, LivenessOptionsResponse,
    VersionGetResponse, VersionOptionsResponse, VersionPostResponse,
};
use std::error::Error;
use swagger::ApiError;

#[async_trait]
impl<C> Api<C> for Server<C>
where
    C: Has<XSpanIdString> + Send + Sync,
{
    /// Get info about the Ceramic network the node is connected to
    async fn config_network_get(&self, context: &C) -> Result<ConfigNetworkGetResponse, ApiError> {
        info!(
            "config_network_get() - X-Span-ID: {:?}",
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// cors
    async fn config_network_options(
        &self,
        context: &C,
    ) -> Result<ConfigNetworkOptionsResponse, ApiError> {
        info!(
            "config_network_options() - X-Span-ID: {:?}",
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Get the heap statistics of the Ceramic node
    async fn debug_heap_get(&self, context: &C) -> Result<DebugHeapGetResponse, ApiError> {
        info!(
            "debug_heap_get() - X-Span-ID: {:?}",
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// cors
    async fn debug_heap_options(&self, context: &C) -> Result<DebugHeapOptionsResponse, ApiError> {
        info!(
            "debug_heap_options() - X-Span-ID: {:?}",
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Get event data
    async fn events_event_id_get(
        &self,
        event_id: String,
        context: &C,
    ) -> Result<EventsEventIdGetResponse, ApiError> {
        info!(
            "events_event_id_get(\"{}\") - X-Span-ID: {:?}",
            event_id,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// cors
    async fn events_event_id_options(
        &self,
        event_id: String,
        context: &C,
    ) -> Result<EventsEventIdOptionsResponse, ApiError> {
        info!(
            "events_event_id_options(\"{}\") - X-Span-ID: {:?}",
            event_id,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// cors
    async fn events_options(&self, context: &C) -> Result<EventsOptionsResponse, ApiError> {
        info!(
            "events_options() - X-Span-ID: {:?}",
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Creates a new event
    async fn events_post(
        &self,
        event_data: models::EventData,
        context: &C,
    ) -> Result<EventsPostResponse, ApiError> {
        info!(
            "events_post({:?}) - X-Span-ID: {:?}",
            event_data,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Get events matching the interest stored on the node
    async fn experimental_events_sep_sep_value_get(
        &self,
        sep: String,
        sep_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        offset: Option<i32>,
        limit: Option<i32>,
        context: &C,
    ) -> Result<ExperimentalEventsSepSepValueGetResponse, ApiError> {
        info!("experimental_events_sep_sep_value_get(\"{}\", \"{}\", {:?}, {:?}, {:?}, {:?}) - X-Span-ID: {:?}", sep, sep_value, controller, stream_id, offset, limit, context.get().0.clone());
        Err(ApiError("Generic failure".into()))
    }

    /// cors
    async fn experimental_events_sep_sep_value_options(
        &self,
        sep: String,
        sep_value: String,
        context: &C,
    ) -> Result<ExperimentalEventsSepSepValueOptionsResponse, ApiError> {
        info!(
            "experimental_events_sep_sep_value_options(\"{}\", \"{}\") - X-Span-ID: {:?}",
            sep,
            sep_value,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Get the interests stored on the node
    async fn experimental_interests_get(
        &self,
        context: &C,
    ) -> Result<ExperimentalInterestsGetResponse, ApiError> {
        info!(
            "experimental_interests_get() - X-Span-ID: {:?}",
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// cors
    async fn experimental_interests_options(
        &self,
        context: &C,
    ) -> Result<ExperimentalInterestsOptionsResponse, ApiError> {
        info!(
            "experimental_interests_options() - X-Span-ID: {:?}",
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Get all new event keys since resume token
    async fn feed_events_get(
        &self,
        resume_at: Option<String>,
        limit: Option<i32>,
        include_data: Option<String>,
        context: &C,
    ) -> Result<FeedEventsGetResponse, ApiError> {
        info!(
            "feed_events_get({:?}, {:?}, {:?}) - X-Span-ID: {:?}",
            resume_at,
            limit,
            include_data,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// cors
    async fn feed_events_options(
        &self,
        context: &C,
    ) -> Result<FeedEventsOptionsResponse, ApiError> {
        info!(
            "feed_events_options() - X-Span-ID: {:?}",
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Get the current (maximum) highwater mark/continuation token of the feed. Allows starting `feed/events` from 'now'.
    async fn feed_resume_token_get(
        &self,
        context: &C,
    ) -> Result<FeedResumeTokenGetResponse, ApiError> {
        info!(
            "feed_resume_token_get() - X-Span-ID: {:?}",
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// cors
    async fn feed_resume_token_options(
        &self,
        context: &C,
    ) -> Result<FeedResumeTokenOptionsResponse, ApiError> {
        info!(
            "feed_resume_token_options() - X-Span-ID: {:?}",
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// cors
    async fn interests_options(&self, context: &C) -> Result<InterestsOptionsResponse, ApiError> {
        info!(
            "interests_options() - X-Span-ID: {:?}",
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Register interest for a sort key
    async fn interests_post(
        &self,
        interest: models::Interest,
        context: &C,
    ) -> Result<InterestsPostResponse, ApiError> {
        info!(
            "interests_post({:?}) - X-Span-ID: {:?}",
            interest,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// cors
    async fn interests_sort_key_sort_value_options(
        &self,
        sort_key: String,
        sort_value: String,
        context: &C,
    ) -> Result<InterestsSortKeySortValueOptionsResponse, ApiError> {
        info!(
            "interests_sort_key_sort_value_options(\"{}\", \"{}\") - X-Span-ID: {:?}",
            sort_key,
            sort_value,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Register interest for a sort key
    async fn interests_sort_key_sort_value_post(
        &self,
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        context: &C,
    ) -> Result<InterestsSortKeySortValuePostResponse, ApiError> {
        info!(
            "interests_sort_key_sort_value_post(\"{}\", \"{}\", {:?}, {:?}) - X-Span-ID: {:?}",
            sort_key,
            sort_value,
            controller,
            stream_id,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Test the liveness of the Ceramic node
    async fn liveness_get(&self, context: &C) -> Result<LivenessGetResponse, ApiError> {
        info!("liveness_get() - X-Span-ID: {:?}", context.get().0.clone());
        Err(ApiError("Generic failure".into()))
    }

    /// cors
    async fn liveness_options(&self, context: &C) -> Result<LivenessOptionsResponse, ApiError> {
        info!(
            "liveness_options() - X-Span-ID: {:?}",
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Get the version of the Ceramic node
    async fn version_get(&self, context: &C) -> Result<VersionGetResponse, ApiError> {
        info!("version_get() - X-Span-ID: {:?}", context.get().0.clone());
        Err(ApiError("Generic failure".into()))
    }

    /// cors
    async fn version_options(&self, context: &C) -> Result<VersionOptionsResponse, ApiError> {
        info!(
            "version_options() - X-Span-ID: {:?}",
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Get the version of the Ceramic node
    async fn version_post(&self, context: &C) -> Result<VersionPostResponse, ApiError> {
        info!("version_post() - X-Span-ID: {:?}", context.get().0.clone());
        Err(ApiError("Generic failure".into()))
    }
}
