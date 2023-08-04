//! Provides an API for performing the Kubo RPC calls consumed by js-ceramic.
//!
//! Both a Rust API is provided along with an HTTP server implementation that follows
//! <https://docs.ipfs.tech/reference/kubo/rpc/>
//!
//! The http server implementation is behind the `http` feature.
#![deny(missing_docs)]
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
use futures_util::stream::BoxStream;
use iroh_rpc_client::{P2pClient, StoreClient};
use libipld::{cbor::DagCborCodec, json::DagJsonCodec, prelude::Decode};
use libp2p::gossipsub::TopicHash;
use tracing::{error, trace};
use unimock::unimock;

// Pub use any types we export as part of an trait or struct
pub use bytes::Bytes;
pub use cid::Cid;
pub use iroh_rpc_types::GossipsubEvent;
pub use libipld::Ipld;
pub use libp2p::Multiaddr;
pub use libp2p_identity::PeerId;

pub mod block;
pub mod dag;
pub mod error;
#[cfg(feature = "http")]
pub mod http;
pub mod id;
pub mod pin;
pub mod pubsub;
pub mod swarm;

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
#[unimock(api=IpfsDepMock)]
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
    /// Store a DAG node into IPFS.
    async fn put(&self, cid: Cid, blob: Bytes, links: Vec<Cid>) -> Result<(), Error>;
    /// Resolve an IPLD block.
    async fn resolve(&self, ipfs_path: &IpfsPath) -> Result<(Cid, String), Error>;
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

/// Implementation of IPFS APIs
pub struct IpfsService {
    p2p: P2pClient,
    store: StoreClient,
    resolver: Resolver,
}

impl IpfsService {
    /// Create new IpfsService
    pub fn new(p2p: P2pClient, store: StoreClient) -> Self {
        let loader = Loader {
            p2p: p2p.clone(),
            store: store.clone(),
        };
        let resolver = Resolver::new(loader);
        Self {
            p2p,
            store,
            resolver,
        }
    }
}
#[async_trait]
impl IpfsDep for Arc<IpfsService> {
    /// Get the ID of the local peer.
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
    async fn block_size(&self, cid: Cid) -> Result<u64, Error> {
        Ok(self
            .store
            .get_size(cid)
            .await
            .map_err(Error::Internal)?
            .ok_or(Error::NotFound)?)
    }
    async fn block_get(&self, cid: Cid) -> Result<Bytes, Error> {
        // TODO do we want to advertise on the DHT all Cids we have?
        Ok(self.resolver.load_cid_bytes(cid).await?)
    }
    async fn get(&self, ipfs_path: &IpfsPath) -> Result<(Cid, Ipld), Error> {
        // TODO do we want to advertise on the DHT all Cids we have?
        let node = self.resolver.resolve(ipfs_path).await?;
        Ok((node.cid, node.data))
    }
    async fn put(&self, cid: Cid, blob: Bytes, links: Vec<Cid>) -> Result<(), Error> {
        // Advertise we provide the content
        self.p2p
            .start_providing(&cid)
            .await
            .map_err(Error::Internal)?;
        Ok(self
            .store
            .put(cid, blob, links)
            .await
            .map_err(Error::Internal)?)
    }
    async fn resolve(&self, ipfs_path: &IpfsPath) -> Result<(Cid, String), Error> {
        let node = self.resolver.resolve(ipfs_path).await?;
        Ok((node.cid, node.path.to_string_lossy().to_string()))
    }
    async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>, Error> {
        Ok(self.p2p.get_peers().await.map_err(Error::Internal)?)
    }
    async fn connect(&self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<(), Error> {
        Ok(self
            .p2p
            .connect(peer_id, addrs)
            .await
            .map_err(Error::Internal)?)
    }
    async fn publish(&self, topic: String, data: Bytes) -> Result<(), Error> {
        let topic = TopicHash::from_raw(topic);
        self.p2p
            .gossipsub_publish(topic, data)
            .await
            .map_err(Error::Internal)?;
        Ok(())
    }
    async fn subscribe(
        &self,
        topic: String,
    ) -> Result<BoxStream<'static, anyhow::Result<GossipsubEvent>>, Error> {
        let topic = TopicHash::from_raw(topic);
        Ok(Box::pin(
            self.p2p
                .gossipsub_subscribe(topic)
                .await
                .map_err(Error::Internal)?,
        ))
    }
    async fn topics(&self) -> Result<Vec<String>, Error> {
        Ok(self
            .p2p
            .gossipsub_topics()
            .await
            .map_err(Error::Internal)?
            .iter()
            .map(|t| t.to_string())
            .collect())
    }
}

// Resolves IPFS paths to their DAG node
// Supports the following codecs:
// * dag-cbor
// * dag-json
// * dag-jose
struct Resolver {
    loader: Loader,
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

impl Resolver {
    fn new(loader: Loader) -> Self {
        Resolver { loader }
    }
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
    async fn load_cid_bytes(&self, cid: Cid) -> Result<Bytes, Error> {
        self.loader.load_cid(cid).await.map_err(Error::Internal)
    }
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

/// Loader is responsible for fetching Cids.
/// It tries local storage and then the network (via bitswap).
struct Loader {
    p2p: P2pClient,
    store: StoreClient,
}

impl Loader {
    // Load a Cid returning its bytes.
    // If the Cid was not stored locally it will be added to the local store.
    async fn load_cid(&self, cid: Cid) -> anyhow::Result<Bytes> {
        trace!(%cid, "loading cid");

        if let Some(loaded) = self.fetch_store(cid).await? {
            return Ok(loaded);
        }

        let loaded = self.fetch_bitswap(cid).await?;

        // Add loaded cid to the local store
        self.store_data(cid, loaded.clone());
        Ok(loaded)
    }

    async fn fetch_store(&self, cid: Cid) -> anyhow::Result<Option<Bytes>> {
        self.store.get(cid).await
    }
    async fn fetch_bitswap(&self, cid: Cid) -> anyhow::Result<Bytes> {
        // TODO can we check kad here and not use bitswap for content discovery?
        self.p2p.fetch_bitswap(0, cid, Default::default()).await
    }

    fn store_data(&self, cid: Cid, data: Bytes) {
        // trigger storage in the background
        let store = self.store.clone();

        tokio::spawn(async move {
            match store.put(cid, data, vec![]).await {
                Ok(_) => {}
                Err(err) => error!(?err, "failed to put cid into local store"),
            }
        });
    }
}
