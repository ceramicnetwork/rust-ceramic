//! API to create and manage an IPFS service.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use ceramic_core::{EventId, Interest, NodeId, NodeKey, PeerKey};
use ceramic_kubo_rpc::{IpfsMetrics, IpfsMetricsMiddleware, IpfsService};
use ceramic_p2p::{Config as P2pConfig, Libp2pConfig, Node, PeerService};
use iroh_rpc_client::P2pClient;
use iroh_rpc_types::{p2p::P2pAddr, Addr};
use multiaddr::Protocol;
use recon::{libp2p::Recon, Sha256a};
use tokio::task::{self, JoinHandle};
use tracing::{debug, error};

/// Builder provides an ordered API for constructing an Ipfs service.
pub struct Builder<S: BuilderState> {
    state: S,
}

/// The state of the builder
pub trait BuilderState {}

/// Initial state of the builder.
pub struct Init {}
impl BuilderState for Init {}

/// A builder that has been configured with its p2p service.
pub struct WithP2p {
    p2p: Service<P2pAddr>,
}
impl BuilderState for WithP2p {}

/// Configure the p2p service
impl Builder<Init> {
    pub async fn with_p2p<P, I, M, S>(
        self,
        libp2p_config: Libp2pConfig,
        node_key: NodeKey,
        peer_svc: impl PeerService + 'static,
        recons: Option<(P, I, M)>,
        block_store: Arc<S>,
        metrics: ceramic_p2p::Metrics,
    ) -> anyhow::Result<Builder<WithP2p>>
    where
        P: Recon<Key = PeerKey, Hash = Sha256a>,
        I: Recon<Key = Interest, Hash = Sha256a>,
        M: Recon<Key = EventId, Hash = Sha256a>,
        S: iroh_bitswap::Store,
    {
        let addr = Addr::new_mem();

        let mut config = P2pConfig::default_with_rpc(addr.clone());

        config.libp2p = libp2p_config;

        let mut p2p = Node::new(
            config,
            addr.clone(),
            node_key,
            peer_svc,
            recons,
            block_store,
            metrics,
        )
        .await?;

        let task = task::spawn(async move {
            if let Err(err) = p2p.run().await {
                error!(%err, "failed to gracefully stop p2p task");
            }
            debug!("node task finished");
        });

        Ok(Builder {
            state: WithP2p {
                p2p: Service { addr, task },
            },
        })
    }
}

/// Finish the build
impl Builder<WithP2p> {
    pub async fn build<S>(self, block_store: Arc<S>, ipfs_metrics: IpfsMetrics) -> Result<Ipfs<S>>
    where
        S: iroh_bitswap::Store,
    {
        let client = P2pClient::new(self.state.p2p.addr.clone()).await?;
        let ipfs_service = Arc::new(IpfsService::new(client.clone(), block_store));
        let ipfs_service = IpfsMetricsMiddleware::new(ipfs_service, ipfs_metrics);
        Ok(Ipfs {
            api: ipfs_service,
            client,
            p2p: self.state.p2p,
        })
    }
}

// Provides Ipfs node implementation
pub struct Ipfs<S> {
    api: IpfsMetricsMiddleware<Arc<IpfsService<S>>>,
    client: P2pClient,
    p2p: Service<P2pAddr>,
}

impl<S> Ipfs<S> {
    pub fn builder() -> Builder<Init> {
        Builder { state: Init {} }
    }
    pub fn api(&self) -> IpfsMetricsMiddleware<Arc<IpfsService<S>>> {
        self.api.clone()
    }
    pub fn client(&self) -> IpfsClient {
        IpfsClient(self.client.clone())
    }
    pub async fn stop(self) -> Result<()> {
        self.p2p.stop().await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct IpfsClient(P2pClient);

#[async_trait]
impl ceramic_api::P2PService for IpfsClient {
    async fn peers(&self) -> Result<Vec<ceramic_api::PeerInfo>> {
        self.0
            .get_peers()
            .await?
            .into_iter()
            .map(|(peer_id, addresses)| {
                Ok(ceramic_api::PeerInfo {
                    id: NodeId::try_from_peer_id(&peer_id)?,
                    addresses,
                })
            })
            .collect()
    }
    async fn peer_connect(&self, addrs: &[ceramic_api::Multiaddr]) -> Result<()> {
        // Find peer id in one of the addresses
        let peer_id = addrs
            .iter()
            .filter_map(|addr| {
                addr.iter()
                    .filter_map(|protocol| {
                        if let Protocol::P2p(peer_id) = protocol {
                            Some(peer_id)
                        } else {
                            None
                        }
                    })
                    .next()
            })
            .next()
            .ok_or_else(|| anyhow!("multi addr must contain a peer id"))?;
        self.0.connect(peer_id, addrs.to_vec()).await
    }
}

struct Service<A> {
    addr: A,
    task: JoinHandle<()>,
}

impl<A> Service<A> {
    async fn stop(self) -> Result<()> {
        self.task.abort();
        // Because we currently don't do graceful termination we expect a cancelled error.
        match self.task.await {
            Ok(()) => Ok(()),
            Err(err) if err.is_cancelled() => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}
