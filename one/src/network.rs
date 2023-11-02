//! API to create and manage an IPFS service.

use std::sync::Arc;

use anyhow::Result;
use ceramic_core::{EventId, Interest};
use ceramic_kubo_rpc::{IpfsMetrics, IpfsMetricsMiddleware, IpfsService};
use ceramic_p2p::{Config as P2pConfig, Libp2pConfig, Node, SQLiteBlockStore};
use iroh_rpc_client::P2pClient;
use iroh_rpc_types::{p2p::P2pAddr, Addr};
use libp2p::identity::Keypair;
use recon::{libp2p::Recon, Sha256a};
use sqlx::SqlitePool;
use tokio::task::{self, JoinHandle};
use tracing::error;

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
    pub async fn with_p2p<I, M>(
        self,
        libp2p_config: Libp2pConfig,
        keypair: Keypair,
        recons: Option<(I, M)>,
        sql_pool: SqlitePool,
    ) -> anyhow::Result<Builder<WithP2p>>
    where
        I: Recon<Key = Interest, Hash = Sha256a>,
        M: Recon<Key = EventId, Hash = Sha256a>,
    {
        let addr = Addr::new_mem();

        let mut config = P2pConfig::default_with_rpc(addr.clone());

        config.libp2p = libp2p_config;

        let mut p2p = Node::new(config, addr.clone(), keypair, recons, sql_pool).await?;

        let task = task::spawn(async move {
            if let Err(err) = p2p.run().await {
                error!("{:?}", err);
            }
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
    pub async fn build(self, sql_pool: SqlitePool, ipfs_metrics: IpfsMetrics) -> Result<Ipfs> {
        let ipfs_service = Arc::new(IpfsService::new(
            P2pClient::new(self.state.p2p.addr.clone()).await?,
            SQLiteBlockStore::new(sql_pool).await?,
        ));
        let ipfs_service = IpfsMetricsMiddleware::new(ipfs_service, ipfs_metrics);
        Ok(Ipfs {
            api: ipfs_service,
            p2p: self.state.p2p,
        })
    }
}

// Provides Ipfs node implementation
pub struct Ipfs {
    api: IpfsMetricsMiddleware<Arc<IpfsService>>,
    p2p: Service<P2pAddr>,
}

impl Ipfs {
    pub fn builder() -> Builder<Init> {
        Builder { state: Init {} }
    }
    pub fn api(&self) -> IpfsMetricsMiddleware<Arc<IpfsService>> {
        self.api.clone()
    }
    pub async fn stop(self) -> Result<()> {
        self.p2p.stop().await?;
        Ok(())
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
