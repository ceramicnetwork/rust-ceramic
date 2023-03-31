//! Provides an API for performing the Kubo RPC calls consumed by js-ceramic.
//!
//! Both a Rust API is provided along with an HTTP server implementation that follows
//! https://docs.ipfs.tech/reference/kubo/rpc/
//!
//! The http server implementation is behind the `http` feature.
#![deny(warnings)]
#![deny(missing_docs)]
use std::{collections::HashMap, io::Cursor, path::PathBuf};

use anyhow::anyhow;
use async_trait::async_trait;
use dag_jose::DagJoseCodec;
use futures_util::stream::BoxStream;
use iroh_api::{Api, Bytes, Cid, GossipsubEvent, IpfsPath, Multiaddr, PeerId};
use libipld::{cbor::DagCborCodec, json::DagJsonCodec, prelude::Decode, Ipld};
use unimock::unimock;

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
    /// Store a DAG node into IFPS.
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

#[async_trait]
impl IpfsDep for Api {
    /// Get the ID of the local peer.
    async fn lookup_local(&self) -> Result<PeerInfo, Error> {
        let l = self
            .client()
            .try_p2p()
            .map_err(Error::Internal)?
            .lookup_local()
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
    /// Get information a peer.
    async fn lookup(&self, peer_id: PeerId) -> Result<PeerInfo, Error> {
        let l = self
            .client()
            .try_p2p()
            .map_err(Error::Internal)?
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
            .client()
            .try_store()
            .map_err(Error::Internal)?
            .get_size(cid)
            .await
            .map_err(Error::Internal)?
            .ok_or(Error::NotFound)?)
    }
    async fn block_get(&self, cid: Cid) -> Result<Bytes, Error> {
        Ok(self.get_raw(cid).await.map_err(Error::Internal)?)
    }
    async fn get(&self, ipfs_path: &IpfsPath) -> Result<(Cid, Ipld), Error> {
        let resolver = Resolver {
            client: self.clone(),
        };
        let node = resolver.resolve(ipfs_path).await?;
        Ok((node.cid, node.data))
    }
    async fn put(&self, cid: Cid, blob: Bytes, links: Vec<Cid>) -> Result<(), Error> {
        // Advertise we provide the content
        self.client()
            .try_p2p()
            .map_err(Error::Internal)?
            .start_providing(&cid)
            .await
            .map_err(Error::Internal)?;
        Ok(self
            .client()
            .try_store()
            .map_err(Error::Internal)?
            .put(cid, blob, links)
            .await
            .map_err(Error::Internal)?)
    }
    async fn resolve(&self, ipfs_path: &IpfsPath) -> Result<(Cid, String), Error> {
        let resolver = Resolver {
            client: self.clone(),
        };
        let node = resolver.resolve(ipfs_path).await?;
        Ok((node.cid, node.path.to_string_lossy().to_string()))
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

// Resolves IPFS paths to their DAG node
// Suppots the following codecs:
// * dag-cbor
// * dag-json
// * dag-jose
struct Resolver {
    client: Api,
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
    async fn resolve(&self, path: &IpfsPath) -> Result<Node, Error> {
        let root_cid = *path
            .cid()
            .ok_or_else(|| Error::Invalid(anyhow!("path must start with a CID")))?;
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
    async fn load_cid(&self, cid: Cid) -> Result<Node, Error> {
        let bytes = self.client.get_raw(cid).await.map_err(Error::Internal)?;
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
