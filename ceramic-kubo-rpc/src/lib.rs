//! Provides an API for performing the Kubo RPC calls consumed by js-ceramic.
//!
//! Both a Rust API is provided along with an HTTP server implementation that follows
//! https://docs.ipfs.tech/reference/kubo/rpc/
//!
//! The http server implementation is behind the `http` feature.
#![deny(warnings)]
#![deny(missing_docs)]
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

/// Defines the behavior this crate needs from IPFS in order to serve Kubo RPC calls.
/// The trait serves two purposes:
///     1. We are explicit about the API surface area we consume from IPFS.
///     2. We can provide a mock implementation for testing.
#[unimock(api=IpfsDepMock)]
#[async_trait]
pub trait IpfsDep: Clone {
    /// Get a DAG node from IPFS.
    async fn get(&self, cid: Cid) -> Result<Option<Bytes>, Error>;
    /// Store a DAG node into IFPS.
    async fn put(&self, cid: Cid, blob: Bytes, links: Vec<Cid>) -> Result<(), Error>;
    /// Resolve an IPLD block.
    async fn resolve(&self, ipfs_path: &IpfsPath) -> Result<Vec<Cid>, Error>;
    /// Report all connected peers of the current node.
    async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>, Error>;
    /// Connect to a specific peer node.
    async fn connect(&self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<(), Error>;
}

#[async_trait]
impl IpfsDep for Api {
    async fn get(&self, cid: Cid) -> Result<Option<Bytes>, Error> {
        Ok(self
            .client()
            .try_store()
            .map_err(Error::Internal)?
            .get(cid)
            .await
            .map_err(Error::Internal)?)
    }
    async fn put(&self, cid: Cid, blob: Bytes, links: Vec<Cid>) -> Result<(), Error> {
        Ok(self
            .client()
            .try_store()
            .map_err(Error::Internal)?
            .put(cid, blob, links)
            .await
            .map_err(Error::Internal)?)
    }
    async fn resolve(&self, ipfs_path: &IpfsPath) -> Result<Vec<Cid>, Error> {
        Ok(self.resolve(ipfs_path).await.map_err(Error::Internal)?)
    }
    async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>, Error> {
        Ok(self
            .client()
            .try_p2p()
            .map_err(Error::Internal)?
            .get_peers()
            .await
            .map_err(Error::Internal)?)
    }
    async fn connect(&self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<(), Error> {
        Ok(self
            .client()
            .try_p2p()
            .map_err(Error::Internal)?
            .connect(peer_id, addrs)
            .await
            .map_err(Error::Internal)?)
    }
}
