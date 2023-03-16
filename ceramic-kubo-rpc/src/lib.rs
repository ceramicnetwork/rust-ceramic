//! Provides an API for performing the Kubo RPC calls consumed by js-ceramic.
//!
//! Both a Rust API is provided along with an HTTP server implementation that follows
//! https://docs.ipfs.tech/reference/kubo/rpc/
//!
//! The http server implementation is behind the `http` feature.
#![deny(warnings)]
#![deny(missing_docs)]
use std::collections::HashMap;

use anyhow::anyhow;
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use iroh_api::{Api, Bytes, Cid, GossipsubEvent, IpfsPath, Multiaddr, PeerId};
use unimock::unimock;

pub mod dag;
pub mod error;
#[cfg(feature = "http")]
pub mod http;
pub mod pubsub;
pub mod swarm;

use crate::error::Error;

/// Defines the behavior this crate needs from IPFS in order to serve Kubo RPC calls.
/// The trait serves two purposes:
///     1. We are explicit about the API surface area we consume from IPFS.
///     2. We can provide a mock implementation for testing.
#[unimock(api=IpfsDepMock)]
#[async_trait]
pub trait IpfsDep: Clone {
    /// Get a DAG node from IPFS returning the Cid of the resolved path and the bytes of the node.
    async fn get(&self, ipfs_path: &IpfsPath) -> Result<(Cid, Bytes), Error>;
    /// Store a DAG node into IFPS.
    async fn put(&self, cid: Cid, blob: Bytes, links: Vec<Cid>) -> Result<(), Error>;
    /// Resolve an IPLD block.
    async fn resolve(&self, ipfs_path: &IpfsPath) -> Result<Vec<Cid>, Error>;
    /// Report all connected peers of the current node.
    async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>, Error>;
    /// Connect to a specific peer node.
    async fn connect(&self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<(), Error>;
    /// Publish a message on a pub/sub Topic.
    async fn publish(&self, topic: String, data: Bytes) -> Result<(), Error>;
    /// Subscribe to a pub/sub Topic
    async fn subscribe(
        &self,
        topic: String,
    ) -> Result<BoxStream<'static, anyhow::Result<GossipsubEvent>>, Error>;
    /// List topics to which, we are currently subscribed
    async fn topics(&self) -> Result<Vec<String>, Error>;
}

#[async_trait]
impl IpfsDep for Api {
    async fn get(&self, ipfs_path: &IpfsPath) -> Result<(Cid, Bytes), Error> {
        // TODO(nathanielc): Iroh does not have support for DAG-JOSE,
        // therefore it cannot traverse paths as it cannot decode the intermediate steps.
        // We use `get_raw` below so that we can still get the data but as a result we do not support
        // any path below the Cid.
        if !ipfs_path.tail().is_empty() {
            return Err(Error::Invalid(anyhow!(
                "IPFS paths with path elements are not yet supported"
            )));
        }

        if let Some(cid) = ipfs_path.cid() {
            Ok((*cid, self.get_raw(*cid).await.map_err(Error::Internal)?))
        } else {
            Err(Error::Invalid(anyhow!("IPFS path does not refer to a CID")))
        }
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
    async fn publish(&self, topic: String, data: Bytes) -> Result<(), Error> {
        self.p2p()
            .map_err(Error::Internal)?
            .publish(topic, data)
            .await
            .map_err(Error::Internal)?;
        Ok(())
    }
    async fn subscribe(
        &self,
        topic: String,
    ) -> Result<BoxStream<'static, anyhow::Result<GossipsubEvent>>, Error> {
        Ok(Box::pin(
            self.p2p()
                .map_err(Error::Internal)?
                .subscribe(topic)
                .await
                .map_err(Error::Internal)?,
        ))
    }
    async fn topics(&self) -> Result<Vec<String>, Error> {
        Ok(self
            .client()
            .try_p2p()
            .map_err(Error::Internal)?
            .gossipsub_topics()
            .await
            .map_err(Error::Internal)?
            .iter()
            .map(|t| t.to_string())
            .collect())
    }
}
