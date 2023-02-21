use std::{collections::HashMap, net};

use actix_web::{web, App, HttpServer};
use anyhow::Result;
use async_trait::async_trait;
use iroh_api::{Api, Bytes, Cid, IpfsPath, Multiaddr, PeerId};
use tracing_actix_web::TracingLogger;

mod dag;
mod error;
mod swarm;

#[derive(Clone)]
struct AppState<T>
where
    T: IrohClient,
{
    api: T,
}

/// Start the Kubo RPC mimic server.
///
/// Block until shutdown.
/// Automatically registers shutdown listeners for interrupt and kill signals.
/// See https://actix.rs/docs/server/#graceful-shutdown
pub async fn serve<T, A>(api: T, addrs: A) -> std::io::Result<()>
where
    T: IrohClient + Send + Clone + 'static,
    A: net::ToSocketAddrs,
{
    HttpServer::new(move || {
        App::new()
            .wrap(TracingLogger::default())
            .app_data(web::Data::new(AppState { api: api.clone() }))
            .service(
                web::scope("/api/v0")
                    .service(dag::scope::<T>())
                    .service(swarm::scope::<T>()),
            )
    })
    .bind(addrs)?
    .run()
    .await
}

/// Defines the behavior we need from Iroh in order to serve Kubo RPC calls.
/// The trait serves two purposes:
///     1. We are explicit about the API surface area we consume from Iroh.
///     2. We can provide an implementation for testing.
#[async_trait]
pub trait IrohClient {
    type StoreClient: StoreClient;
    type P2pClient: P2pClient;
    fn try_store(&self) -> Result<Self::StoreClient>;
    fn try_p2p(&self) -> Result<Self::P2pClient>;
    async fn resolve(&self, ipfs_path: &IpfsPath) -> Result<Vec<Cid>>;
}

#[async_trait]
pub trait StoreClient {
    async fn get(&self, cid: Cid) -> Result<Option<Bytes>>;
    async fn put(&self, cid: Cid, blob: Bytes, links: Vec<Cid>) -> Result<()>;
}
#[async_trait]
pub trait P2pClient {
    async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>>;
    async fn connect(&self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<()>;
}

#[async_trait]
impl IrohClient for Api {
    type StoreClient = iroh_rpc_client::StoreClient;
    type P2pClient = iroh_rpc_client::P2pClient;

    fn try_store(&self) -> Result<Self::StoreClient> {
        self.client().try_store()
    }

    fn try_p2p(&self) -> Result<Self::P2pClient> {
        self.client().try_p2p()
    }

    async fn resolve(&self, ipfs_path: &IpfsPath) -> Result<Vec<Cid>> {
        self.resolve(ipfs_path).await
    }
}

#[async_trait]
impl StoreClient for iroh_rpc_client::StoreClient {
    async fn get(&self, cid: Cid) -> Result<Option<Bytes>> {
        self.get(cid).await
    }
    async fn put(&self, cid: Cid, blob: Bytes, links: Vec<Cid>) -> Result<()> {
        self.put(cid, blob, links).await
    }
}
#[async_trait]
impl P2pClient for iroh_rpc_client::P2pClient {
    async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>> {
        self.get_peers().await
    }

    async fn connect(&self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<()> {
        self.connect(peer_id, addrs).await
    }
}
