//! API to create and manage an IPFS service.

use anyhow::{Context, Result};
use ceramic_kubo_rpc::IpfsService;
use ceramic_p2p::{Config as P2pConfig, DiskStorage, Keychain, Libp2pConfig, Node};
use iroh_rpc_client::{P2pClient, StoreClient};
use iroh_rpc_types::{p2p::P2pAddr, store::StoreAddr, Addr};
use iroh_store::{Config as StoreConfig, Store};
use recon::libp2p::Recon;
use std::{path::PathBuf, sync::Arc};
use tokio::task::{self, JoinHandle};
use tracing::{error, info};

/// Builder provides an ordered API for constructing an Ipfs service.
pub struct Builder<S: BuilderState> {
    state: S,
}

/// The state of the builder
pub trait BuilderState {}

/// Initial state of the builder.
pub struct Init {}
impl BuilderState for Init {}

/// A builder that has been configured with its store service.
pub struct WithStore {
    store: Service<StoreAddr>,
}
impl BuilderState for WithStore {}

/// A builder that has been configured with its p2p service.
pub struct WithP2p {
    store: Service<StoreAddr>,
    p2p: Service<P2pAddr>,
}
impl BuilderState for WithP2p {}

/// Configure the local block store
impl Builder<Init> {
    pub async fn with_store(self, path: PathBuf) -> Result<Builder<WithStore>> {
        let addr = Addr::new_mem();
        let config = StoreConfig::with_rpc_addr(path, addr.clone());

        // This is the file RocksDB itself is looking for to determine if the database already
        // exists or not.  Just knowing the directory exists does not mean the database is
        // created.
        let marker = config.path.join("CURRENT");

        let store = if marker.exists() {
            info!("Opening store at {}", config.path.display());
            Store::open(config)
                .await
                .context("failed to open existing store")?
        } else {
            info!("Creating store at {}", config.path.display());
            Store::create(config)
                .await
                .context("failed to create new store")?
        };

        let rpc_addr = addr.clone();
        let task = tokio::spawn(async move {
            if let Err(err) = iroh_store::rpc::new(rpc_addr, store).await {
                error!("{:?}", err);
            }
        });

        Ok(Builder {
            state: WithStore {
                store: Service { addr, task },
            },
        })
    }
}

/// Configure the p2p service
impl Builder<WithStore> {
    pub async fn with_p2p<R: Recon>(
        self,
        libp2p_config: Libp2pConfig,
        key_store_path: PathBuf,
        recon: Option<R>,
        ceramic_peers_key: &str,
    ) -> anyhow::Result<Builder<WithP2p>> {
        let addr = Addr::new_mem();

        let mut config = P2pConfig::default_with_rpc(addr.clone());

        config.rpc_client.store_addr = Some(self.state.store.addr.clone());
        config.libp2p = libp2p_config;
        config.key_store_path = key_store_path;

        let kc = Keychain::<DiskStorage>::new(config.key_store_path.clone()).await?;

        let mut p2p = Node::new(config, addr.clone(), kc, recon, ceramic_peers_key).await?;

        let task = task::spawn(async move {
            if let Err(err) = p2p.run().await {
                error!("{:?}", err);
            }
        });

        Ok(Builder {
            state: WithP2p {
                store: self.state.store,
                p2p: Service { addr, task },
            },
        })
    }
}

/// Finish the build
impl Builder<WithP2p> {
    pub async fn build(self) -> Result<Ipfs> {
        Ok(Ipfs {
            api: Arc::new(IpfsService::new(
                P2pClient::new(self.state.p2p.addr.clone()).await?,
                StoreClient::new(self.state.store.addr.clone()).await?,
            )),
            store: self.state.store,
            p2p: self.state.p2p,
        })
    }
}

// Provides Ipfs node implementation
pub struct Ipfs {
    api: Arc<IpfsService>,
    p2p: Service<P2pAddr>,
    store: Service<StoreAddr>,
}

impl Ipfs {
    pub fn builder() -> Builder<Init> {
        Builder { state: Init {} }
    }
    pub fn api(&self) -> Arc<IpfsService> {
        self.api.clone()
    }
    pub async fn stop(self) -> Result<()> {
        let p2p_res = self.p2p.stop().await;
        let store_res = self.store.stop().await;
        match (p2p_res, store_res) {
            (Ok(_), Ok(_)) => Ok(()),
            (e @ Err(_), _) => e,
            (_, e @ Err(_)) => e,
        }
    }
}

struct Service<A> {
    addr: A,
    task: JoinHandle<()>,
}

impl<A> Service<A> {
    async fn stop(mut self) -> Result<()> {
        // This dummy task will be aborted by Drop.
        let fut = futures::future::ready(());
        let dummy_task = tokio::spawn(fut);
        let task = std::mem::replace(&mut self.task, dummy_task);

        task.abort();

        // Because we currently don't do graceful termination we expect a cancelled error.
        match task.await {
            Ok(()) => Ok(()),
            Err(err) if err.is_cancelled() => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}
