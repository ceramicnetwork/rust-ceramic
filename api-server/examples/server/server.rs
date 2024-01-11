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
    Api, EventsPostResponse, LivenessGetResponse, ReconPostResponse,
    SubscribeSortKeySortValueGetResponse, VersionPostResponse,
};
use std::error::Error;
use swagger::ApiError;

#[async_trait]
impl<C> Api<C> for Server<C>
where
    C: Has<XSpanIdString> + Send + Sync,
{
    /// Creates a new event
    async fn events_post(
        &self,
        event: models::Event,
        context: &C,
    ) -> Result<EventsPostResponse, ApiError> {
        let context = context.clone();
        info!(
            "events_post({:?}) - X-Span-ID: {:?}",
            event,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Test the liveness of the Ceramic node
    async fn liveness_get(&self, context: &C) -> Result<LivenessGetResponse, ApiError> {
        let context = context.clone();
        info!("liveness_get() - X-Span-ID: {:?}", context.get().0.clone());
        Err(ApiError("Generic failure".into()))
    }

    /// Sends a Recon message
    async fn recon_post(
        &self,
        ring: models::Ring,
        body: swagger::ByteArray,
        context: &C,
    ) -> Result<ReconPostResponse, ApiError> {
        let context = context.clone();
        info!(
            "recon_post({:?}, {:?}) - X-Span-ID: {:?}",
            ring,
            body,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Get events for a stream
    async fn subscribe_sort_key_sort_value_get(
        &self,
        sort_key: String,
        sort_value: String,
        controller: Option<String>,
        stream_id: Option<String>,
        offset: Option<f64>,
        limit: Option<f64>,
        context: &C,
    ) -> Result<SubscribeSortKeySortValueGetResponse, ApiError> {
        let context = context.clone();
        info!("subscribe_sort_key_sort_value_get(\"{}\", \"{}\", {:?}, {:?}, {:?}, {:?}) - X-Span-ID: {:?}", sort_key, sort_value, controller, stream_id, offset, limit, context.get().0.clone());
        Err(ApiError("Generic failure".into()))
    }

    /// Get the version of the Ceramic node
    async fn version_post(&self, context: &C) -> Result<VersionPostResponse, ApiError> {
        let context = context.clone();
        info!("version_post() - X-Span-ID: {:?}", context.get().0.clone());
        Err(ApiError("Generic failure".into()))
    }
}
