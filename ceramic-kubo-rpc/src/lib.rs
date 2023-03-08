use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use iroh_api::{Api, Bytes, Cid, IpfsPath, Multiaddr, PeerId};

pub mod dag;
pub mod error;
#[cfg(feature = "http")]
pub mod http;
pub mod swarm;

/// Defines the behavior we need from Iroh in order to serve Kubo RPC calls.
/// The trait serves two purposes:
///     1. We are explicit about the API surface area we consume from Iroh.
///     2. We can provide an implementation for testing.
#[async_trait]
pub trait IrohClient: Clone {
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
