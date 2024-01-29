//! Provides an API for performing the Kubo RPC calls consumed by js-ceramic.
//!
//! Both a Rust API is provided along with an HTTP server implementation that follows
//! <https://docs.ipfs.tech/reference/kubo/rpc/>
//!
//! The http server implementation is behind the `http` feature.
#![warn(missing_docs)]
use std::{
    collections::HashMap,
    fmt::{self, Display, Formatter},
    io::Cursor,
    path::PathBuf,
};
use std::{str::FromStr, sync::Arc};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use dag_jose::DagJoseCodec;
use iroh_rpc_client::P2pClient;
use libipld::{cbor::DagCborCodec, json::DagJsonCodec, prelude::Decode};
use tracing::instrument;

// Pub use any types we export as part of an trait or struct
pub use bytes::Bytes;
pub use ceramic_metadata::Version;
pub use cid::Cid;
pub use libipld::Ipld;
pub use libp2p::Multiaddr;
pub use libp2p_identity::PeerId;

// TODO(WS1-1310): Refactor Ipfs out of KuboRpc so we do not have these prefixed types.
pub use ipfs_metrics::{IpfsMetrics, IpfsMetricsMiddleware};

pub mod block;
pub mod dag;
pub mod error;
#[cfg(feature = "http")]
pub mod http;
pub mod id;
mod ipfs_metrics;
pub mod pin;
pub mod swarm;
pub mod version;

use crate::error::Error;

/// Information about a peer
#[derive(Debug)]
pub struct PeerInfo {
    /// Id of the peer.
    pub peer_id: PeerId,
    /// Protocol version of the peer.
    pub protocol_version: String,
    /// Agent version of the peer.
    pub agent_version: String,
    /// Publish listening address of the peer.
    pub listen_addrs: Vec<Multiaddr>,
    /// Protocols supported by the peer.
    pub protocols: Vec<String>,
}

/// An IPFS path {cid}/path/through/dag
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpfsPath {
    root: Cid,
    tail: Vec<String>,
}

impl IpfsPath {
    /// New path from a cid.
    pub fn from_cid(cid: Cid) -> Self {
        Self {
            root: cid,
            tail: Vec::new(),
        }
    }
    fn cid(&self) -> Cid {
        self.root
    }
    fn tail(&self) -> &[String] {
        self.tail.as_slice()
    }
    // used only for string path manipulation
    fn has_trailing_slash(&self) -> bool {
        !self.tail.is_empty() && self.tail.last().unwrap().is_empty()
    }
}

impl Display for IpfsPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "/{}", self.root)?;

        for part in &self.tail {
            if part.is_empty() {
                continue;
            }
            write!(f, "/{part}")?;
        }

        if self.has_trailing_slash() {
            write!(f, "/")?;
        }

        Ok(())
    }
}

impl FromStr for IpfsPath {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(&['/', '\\']).filter(|s| !s.is_empty());

        let first_part = parts.next().ok_or_else(|| anyhow!("path too short"))?;
        let root = if first_part.eq_ignore_ascii_case("ipfs") {
            parts.next().ok_or_else(|| anyhow!("path too short"))?
        } else {
            first_part
        };

        let root = Cid::from_str(root).context("invalid cid")?;

        let mut tail: Vec<String> = parts.map(Into::into).collect();

        if s.ends_with('/') {
            tail.push("".to_owned());
        }

        Ok(IpfsPath { root, tail })
    }
}

/// Defines the behavior this crate needs from IPFS in order to serve Kubo RPC calls.
/// The trait serves two purposes:
///     1. We are explicit about the API surface area we consume from IPFS.
///     2. We can provide a mock implementation for testing.
#[async_trait]
pub trait IpfsDep: Clone {
    /// Get information about the local peer.
    async fn lookup_local(&self) -> Result<PeerInfo, Error>;
    /// Get information about a peer.
    async fn lookup(&self, peer_id: PeerId) -> Result<PeerInfo, Error>;
    /// Get the size of an IPFS block.
    async fn block_size(&self, cid: Cid) -> Result<u64, Error>;
    /// Get a block from IPFS
    async fn block_get(&self, cid: Cid) -> Result<Bytes, Error>;
    /// Get a DAG node from IPFS returning the Cid of the resolved path and the bytes of the node.
    /// This will locally store the data as a result.
    async fn get(&self, ipfs_path: &IpfsPath) -> Result<(Cid, Ipld), Error>;
    /// Resolve an IPLD block.
    async fn resolve(&self, ipfs_path: &IpfsPath) -> Result<(Cid, String), Error>;
    /// Report all connected peers of the current node.
    async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>, Error>;
    /// Connect to a specific peer node.
    async fn connect(&self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<(), Error>;
    /// Current version of ceramic
    async fn version(&self) -> Result<ceramic_metadata::Version, Error>;
}

/// Implementation of IPFS APIs
pub struct IpfsService<S> {
    p2p: P2pClient,
    store: S,
    resolver: Resolver<S>,
}

impl<S> IpfsService<S>
where
    S: iroh_bitswap::Store + Clone,
{
    /// Create new IpfsService
    pub fn new(p2p: P2pClient, store: S) -> Self {
        let resolver = Resolver::new(store.clone());
        Self {
            p2p,
            store,
            resolver,
        }
    }
}

#[async_trait]
impl<S> IpfsDep for Arc<IpfsService<S>>
where
    S: iroh_bitswap::Store,
{
    /// Get the ID of the local peer.
    #[instrument(skip(self))]
    async fn lookup_local(&self) -> Result<PeerInfo, Error> {
        let l = self.p2p.lookup_local().await.map_err(Error::Internal)?;
        Ok(PeerInfo {
            peer_id: l.peer_id,
            protocol_version: l.protocol_version,
            agent_version: l.agent_version,
            listen_addrs: l.listen_addrs,
            protocols: l.protocols,
        })
    }
    /// Get information a peer.
    #[instrument(skip(self))]
    async fn lookup(&self, peer_id: PeerId) -> Result<PeerInfo, Error> {
        let l = self
            .p2p
            .lookup(peer_id, None)
            .await
            .map_err(Error::Internal)?;
        Ok(PeerInfo {
            peer_id: l.peer_id,
            protocol_version: l.protocol_version,
            agent_version: l.agent_version,
            listen_addrs: l.listen_addrs,
            protocols: l.protocols,
        })
    }
    #[instrument(skip(self))]
    async fn block_size(&self, cid: Cid) -> Result<u64, Error> {
        if self.store.has(&cid).await.map_err(Error::Internal)? {
            Ok(self.store.get_size(&cid).await.map_err(Error::Internal)? as u64)
        } else {
            Err(Error::NotFound)
        }
    }
    #[instrument(skip(self))]
    async fn block_get(&self, cid: Cid) -> Result<Bytes, Error> {
        // Read directly from the store
        if self.store.has(&cid).await.map_err(Error::Internal)? {
            Ok(self.store.get(&cid).await.map_err(Error::Internal)?.data)
        } else {
            Err(Error::NotFound)
        }
    }
    #[instrument(skip(self))]
    async fn get(&self, ipfs_path: &IpfsPath) -> Result<(Cid, Ipld), Error> {
        // TODO do we want to advertise on the DHT all Cids we have?
        let node = self.resolver.resolve(ipfs_path).await?;
        Ok((node.cid, node.data))
    }
    #[instrument(skip(self))]
    async fn resolve(&self, ipfs_path: &IpfsPath) -> Result<(Cid, String), Error> {
        let node = self.resolver.resolve(ipfs_path).await?;
        Ok((node.cid, node.path.to_string_lossy().to_string()))
    }
    #[instrument(skip(self))]
    async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>, Error> {
        Ok(self.p2p.get_peers().await.map_err(Error::Internal)?)
    }
    #[instrument(skip(self))]
    async fn connect(&self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<(), Error> {
        Ok(self
            .p2p
            .connect(peer_id, addrs)
            .await
            .map_err(Error::Internal)?)
    }
    #[instrument(skip(self))]
    async fn version(&self) -> Result<ceramic_metadata::Version, Error> {
        Ok(ceramic_metadata::Version::default())
    }
}

// Resolves IPFS paths to their DAG node
// Supports the following codecs:
// * dag-cbor
// * dag-json
// * dag-jose
struct Resolver<S> {
    store: S,
}

// Represents an IPFS DAG node
struct Node {
    // CID of the block that contains the node.
    cid: Cid,
    // Relative path of the node within block.
    path: PathBuf,
    // The node data itself.
    data: Ipld,
}

impl<S> Resolver<S>
where
    S: iroh_bitswap::Store,
{
    fn new(store: S) -> Self {
        Resolver { store }
    }
    #[instrument(skip(self))]
    async fn resolve(&self, path: &IpfsPath) -> Result<Node, Error> {
        let root_cid = path.cid();
        let root = self.load_cid(root_cid).await?;

        let mut current = root;

        let parts = path.tail();
        for part in parts.iter().filter(|s| !s.is_empty()) {
            // Parse part as an integer and if that fails parse as a string into an index.
            let index: libipld::ipld::IpldIndex = if let Ok(i) = part.parse::<usize>() {
                i.into()
            } else {
                part.clone().into()
            };
            current.path = current.path.join(part);
            current.data = current.data.take(index).map_err(|_| {
                Error::Invalid(anyhow!(
                    "IPLD resolve error: Couldn't find part {} in path '{}'",
                    part,
                    parts.join("/")
                ))
            })?;

            // Check if we have found a link and follow it
            if let Ipld::Link(c) = current.data {
                current = self.load_cid(c).await?;
            }
            // Treat payload/bytes that is a valid CID as a link
            // This is DAG-JOSE specific logic
            if current.path.ends_with("payload") {
                if let Ipld::Bytes(bytes) = &current.data {
                    if let Ok(c) = Cid::try_from(bytes.as_slice()) {
                        current = self.load_cid(c).await?;
                    }
                }
            }
        }
        Ok(current)
    }
    #[instrument(skip(self))]
    async fn load_cid_bytes(&self, cid: Cid) -> Result<Bytes, Error> {
        // Get the cid directly from the store
        Ok(self.store.get(&cid).await.map_err(Error::Internal)?.data)
    }
    #[instrument(skip(self))]
    async fn load_cid(&self, cid: Cid) -> Result<Node, Error> {
        let bytes = self.load_cid_bytes(cid).await?;
        let data = match cid.codec() {
            //TODO(nathanielc): create constants for these
            // dag-cbor
            0x71 => {
                Ipld::decode(DagCborCodec, &mut Cursor::new(&bytes)).map_err(Error::Internal)?
            }
            // dag-json
            0x0129 => {
                Ipld::decode(DagJsonCodec, &mut Cursor::new(&bytes)).map_err(Error::Internal)?
            }
            // dag-jose
            0x85 => {
                Ipld::decode(DagJoseCodec, &mut Cursor::new(&bytes)).map_err(Error::Internal)?
            }
            _ => return Err(Error::Invalid(anyhow!("unsupported codec {}", cid.codec()))),
        };
        let path = PathBuf::new();
        Ok(Node { cid, path, data })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use mockall::mock;

    mock! {
        pub IpfsDepTest {}
        #[async_trait]
        impl IpfsDep for IpfsDepTest {
            async fn lookup_local(&self) -> Result<PeerInfo, Error>;
            async fn lookup(&self, peer_id: PeerId) -> Result<PeerInfo, Error>;
            async fn block_size(&self, cid: Cid) -> Result<u64, Error>;
            async fn block_get(&self, cid: Cid) -> Result<Bytes, Error>;
            async fn get(&self, ipfs_path: &IpfsPath) -> Result<(Cid, Ipld), Error>;
            async fn resolve(&self, ipfs_path: &IpfsPath) -> Result<(Cid, String), Error>;
            async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>, Error>;
            async fn connect(&self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<(), Error>;
            async fn version(&self) -> Result<Version, Error>;
        }
        impl Clone for IpfsDepTest {
            fn clone(&self) -> Self;
        }
    }
}
