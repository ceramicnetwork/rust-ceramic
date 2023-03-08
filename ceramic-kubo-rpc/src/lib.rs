use std::collections::HashMap;

use async_trait::async_trait;
use iroh_api::{Api, Bytes, Cid, IpfsPath, Multiaddr, PeerId};
use unimock::unimock;

pub mod dag;
pub mod error;
#[cfg(feature = "http")]
pub mod http;
pub mod swarm;

use crate::error::Error;

/// Defines the behavior we need from Iroh in order to serve Kubo RPC calls.
/// The trait serves two purposes:
///     1. We are explicit about the API surface area we consume from Iroh.
///     2. We can provide a mock implementation for testing.
#[unimock(api=IpfsDepMock)]
#[async_trait]
pub trait IpfsDep: Clone {
    async fn get(&self, cid: Cid) -> Result<Option<Bytes>, Error>;
    async fn put(&self, cid: Cid, blob: Bytes, links: Vec<Cid>) -> Result<(), Error>;
    async fn resolve(&self, ipfs_path: &IpfsPath) -> Result<Vec<Cid>, Error>;
    async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>, Error>;
    async fn connect(&self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<(), Error>;
}

#[async_trait]
impl IpfsDep for Api {
    async fn get(&self, cid: Cid) -> Result<Option<Bytes>, Error> {
        Ok(self
            .client()
            .try_store()
            .map_err(|e| Error::Internal(e))?
            .get(cid)
            .await
            .map_err(|e| Error::Internal(e))?)
    }
    async fn put(&self, cid: Cid, blob: Bytes, links: Vec<Cid>) -> Result<(), Error> {
        Ok(self
            .client()
            .try_store()
            .map_err(|e| Error::Internal(e))?
            .put(cid, blob, links)
            .await
            .map_err(|e| Error::Internal(e))?)
    }
    async fn resolve(&self, ipfs_path: &IpfsPath) -> Result<Vec<Cid>, Error> {
        Ok(self
            .resolve(ipfs_path)
            .await
            .map_err(|e| Error::Internal(e))?)
    }
    async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>, Error> {
        Ok(self
            .client()
            .try_p2p()
            .map_err(|e| Error::Internal(e))?
            .get_peers()
            .await
            .map_err(|e| Error::Internal(e))?)
    }
    async fn connect(&self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<(), Error> {
        Ok(self
            .client()
            .try_p2p()
            .map_err(|e| Error::Internal(e))?
            .connect(peer_id, addrs)
            .await
            .map_err(|e| Error::Internal(e))?)
    }
}
