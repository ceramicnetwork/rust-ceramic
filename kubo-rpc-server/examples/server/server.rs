#![allow(suspicious_double_ref_op)]
//! Main library entry point for ceramic_kubo_rpc_server implementation.

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

use ceramic_kubo_rpc_server::models;

/// Builds an SSL implementation for Simple HTTPS from some hard-coded file names
pub async fn create(addr: &str, https: bool) {
    let addr = addr.parse().expect("Failed to parse bind address");

    let server = Server::new();

    let service = MakeService::new(server);

    let service = MakeAllowAllAuthenticator::new(service, "cosmo");

    #[allow(unused_mut)]
    let mut service =
        ceramic_kubo_rpc_server::server::context::MakeAddContext::<_, EmptyContext>::new(service);

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

use ceramic_kubo_rpc_server::server::MakeService;
use ceramic_kubo_rpc_server::{
    Api, BlockGetPostResponse, BlockStatPostResponse, DagGetPostResponse, DagResolvePostResponse,
    IdPostResponse, PinAddPostResponse, PinRmPostResponse, SwarmConnectPostResponse,
    SwarmPeersPostResponse, VersionPostResponse,
};
use std::error::Error;
use swagger::ApiError;

#[async_trait]
impl<C> Api<C> for Server<C>
where
    C: Has<XSpanIdString> + Send + Sync,
{
    /// Get a single IPFS block
    async fn block_get_post(
        &self,
        arg: String,
        timeout: Option<String>,
        offline: Option<bool>,
        context: &C,
    ) -> Result<BlockGetPostResponse, ApiError> {
        info!(
            "block_get_post(\"{}\", {:?}, {:?}) - X-Span-ID: {:?}",
            arg,
            timeout,
            offline,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Report statistics about a block
    async fn block_stat_post(
        &self,
        arg: String,
        context: &C,
    ) -> Result<BlockStatPostResponse, ApiError> {
        info!(
            "block_stat_post(\"{}\") - X-Span-ID: {:?}",
            arg,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Get an IPLD node from IPFS
    async fn dag_get_post(
        &self,
        arg: String,
        output_codec: Option<models::Codecs>,
        context: &C,
    ) -> Result<DagGetPostResponse, ApiError> {
        info!(
            "dag_get_post(\"{}\", {:?}) - X-Span-ID: {:?}",
            arg,
            output_codec,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Resolve an IPFS path to a DAG node
    async fn dag_resolve_post(
        &self,
        arg: String,
        context: &C,
    ) -> Result<DagResolvePostResponse, ApiError> {
        info!(
            "dag_resolve_post(\"{}\") - X-Span-ID: {:?}",
            arg,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Report identifying information about a node
    async fn id_post(&self, arg: Option<String>, context: &C) -> Result<IdPostResponse, ApiError> {
        info!(
            "id_post({:?}) - X-Span-ID: {:?}",
            arg,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Add a block to the pin store
    async fn pin_add_post(
        &self,
        arg: String,
        recursive: Option<bool>,
        progress: Option<bool>,
        context: &C,
    ) -> Result<PinAddPostResponse, ApiError> {
        info!(
            "pin_add_post(\"{}\", {:?}, {:?}) - X-Span-ID: {:?}",
            arg,
            recursive,
            progress,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Remove a block from the pin store
    async fn pin_rm_post(&self, arg: String, context: &C) -> Result<PinRmPostResponse, ApiError> {
        info!(
            "pin_rm_post(\"{}\") - X-Span-ID: {:?}",
            arg,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Connect to peers
    async fn swarm_connect_post(
        &self,
        arg: &Vec<String>,
        context: &C,
    ) -> Result<SwarmConnectPostResponse, ApiError> {
        info!(
            "swarm_connect_post({:?}) - X-Span-ID: {:?}",
            arg,
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Report connected peers
    async fn swarm_peers_post(&self, context: &C) -> Result<SwarmPeersPostResponse, ApiError> {
        info!(
            "swarm_peers_post() - X-Span-ID: {:?}",
            context.get().0.clone()
        );
        Err(ApiError("Generic failure".into()))
    }

    /// Report server version
    async fn version_post(&self, context: &C) -> Result<VersionPostResponse, ApiError> {
        info!("version_post() - X-Span-ID: {:?}", context.get().0.clone());
        Err(ApiError("Generic failure".into()))
    }
}
