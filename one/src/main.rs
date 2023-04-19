//! Ceramic implements a single binary ceramic node.
#![deny(warnings)]
#![deny(missing_docs)]

use std::{path::PathBuf, str::FromStr, sync::Arc};

use anyhow::Result;
use ceramic_kubo_rpc::dag;
use clap::{Args, Parser, Subcommand};
use futures_util::{future, StreamExt};
use iroh_api::IpfsPath;
use iroh_api::Multiaddr;
use iroh_embed::{Iroh, IrohBuilder, Libp2pConfig, P2pService, RocksStoreService};
use iroh_metrics::{config::Config as MetricsConfig, MetricsHandle};
use libipld::json::DagJsonCodec;
use libp2p::metrics::Recorder;
use tokio::task;
use tracing::{debug, info, warn};

use crate::{metrics::Metrics, pubsub::Message};

mod metrics;
mod pubsub;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run a daemon process
    Daemon(DaemonOpts),
    /// Run a process that locally pins all stream tips
    Eye(EyeOpts),
}

#[derive(Args, Debug)]
struct DaemonOpts {
    /// Bind address of the RPC enpoint.
    #[arg(
        short,
        long,
        default_value = "127.0.0.1:5001",
        env = "CERAMIC_ONE_BIND_ADDRESS"
    )]
    bind_address: String,
    /// Listen address of the p2p swarm.
    #[arg(
        long,
        default_values_t = vec!["/ip4/0.0.0.0/tcp/0".to_string(), "/ip4/0.0.0.0/udp/0/quic-v1".to_string()],
        env = "CERAMIC_ONE_SWARM_ADDRESSES"
    )]
    swarm_addresses: Vec<String>,
    /// Address of bootstrap peers.
    /// There are no default address, use this arg or the API to connect to bootstrap peers as needed.
    #[arg(long, env = "CERAMIC_ONE_BOOTSTRAP_ADDRESSES")]
    bootstrap_addresses: Vec<String>,
    /// Path to storage directory
    #[arg(short, long, env = "CERAMIC_ONE_STORE_DIR")]
    store_dir: Option<PathBuf>,
    /// When true metrics will be exported
    #[arg(long, default_value_t = false, env = "CERAMIC_ONE_METRICS")]
    metrics: bool,
    /// When true traces will be exported
    #[arg(long, default_value_t = false, env = "CERAMIC_ONE_TRACING")]
    tracing: bool,
}

#[derive(Args, Debug)]
struct EyeOpts {
    #[command(flatten)]
    daemon: DaemonOpts,

    /// Topic to listen for tip updates
    #[arg(
        short,
        long,
        default_value = "/ceramic/testnet-clay",
        env = "CERAMIC_ONE_TOPIC"
    )]
    topic: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        Command::Daemon(opts) => {
            let daemon = Daemon::build(opts).await?;
            daemon.run().await?;
            daemon.shutdown().await
        }
        Command::Eye(opts) => eye(opts).await,
    }
}

struct Daemon {
    bind_address: String,
    iroh: Iroh,
    metrics_handle: MetricsHandle,
    metrics: Arc<Metrics>,
}

impl Daemon {
    async fn build(opts: DaemonOpts) -> Result<Self> {
        let mut metrics_config = MetricsConfig::default();
        metrics_config = metrics_config_with_compile_time_info(metrics_config);
        metrics_config.collect = opts.metrics;
        metrics_config.tracing = opts.tracing;
        let service_name = metrics_config.service_name.clone();
        let instance_id = metrics_config.instance_id.clone();
        let metrics = iroh_metrics::MetricsHandle::register(crate::metrics::Metrics::new);
        let metrics = Arc::new(metrics);

        let metrics_handle = iroh_metrics::MetricsHandle::new(metrics_config.clone())
            .await
            .expect("failed to initialize metrics");
        info!(service_name, instance_id);

        let dir = match opts.store_dir {
            Some(dir) => dir,
            None => match home::home_dir() {
                Some(home_dir) => home_dir.join(".ceramic-one"),
                None => PathBuf::from(".ceramic-one"),
            },
        };
        debug!("Using directory: {}", dir.display());

        let store = RocksStoreService::new(dir.join("store")).await?;
        let mut p2p_config = Libp2pConfig::default();
        p2p_config.mdns = false;

        p2p_config.bitswap_server = true;
        p2p_config.bitswap_client = true;
        p2p_config.kademlia = true;
        p2p_config.autonat = true;
        p2p_config.relay_server = true;
        p2p_config.relay_client = true;
        p2p_config.gossipsub = true;
        p2p_config.max_conns_out = 2000;
        p2p_config.max_conns_in = 2000;
        p2p_config.bootstrap_peers = opts
            .bootstrap_addresses
            .iter()
            .map(|addr| addr.parse())
            .collect::<Result<Vec<Multiaddr>, multiaddr::Error>>()?;

        p2p_config.listening_multiaddrs = opts
            .swarm_addresses
            .iter()
            .map(|addr| addr.parse())
            .collect::<Result<Vec<Multiaddr>, multiaddr::Error>>()?;
        let p2p = P2pService::new(p2p_config, dir, store.addr()).await?;

        // Note by default this is configured with an indexer, but not with http resolvers.
        let iroh = IrohBuilder::new().store(store).p2p(p2p).build().await?;

        Ok(Daemon {
            bind_address: opts.bind_address,
            iroh,
            metrics_handle,
            metrics,
        })
    }
    // Start the daemon, await does not return until the daemon is finished.
    async fn run(&self) -> Result<()> {
        // Run the HTTP server, this blocks until the HTTP server is shutdown via a unix signal.
        ceramic_kubo_rpc::http::serve(self.iroh.api().clone(), self.bind_address.clone()).await?;
        Ok(())
    }
    async fn shutdown(self) -> Result<()> {
        // Stop the system gracefully.
        self.iroh.stop().await?;
        self.metrics_handle.shutdown();
        Ok(())
    }
}

async fn eye(opts: EyeOpts) -> Result<()> {
    let daemon = Daemon::build(opts.daemon).await?;

    // Start subscription
    let subscription = daemon.iroh.api().p2p()?.subscribe(opts.topic).await?;

    let client = daemon.iroh.api().clone();
    let metrics = daemon.metrics.clone();

    let p2p_events_handle = task::spawn(subscription.for_each(move |event| {
        match event.expect("should be a message") {
            iroh_api::GossipsubEvent::Subscribed { .. } => {}
            iroh_api::GossipsubEvent::Unsubscribed { .. } => {}
            iroh_api::GossipsubEvent::Message {
                from,
                id: _,
                message,
            } => {
                let msg: Message = serde_json::from_slice(message.data.as_slice())
                    .expect("should be json message");
                info!(?msg);
                match &msg {
                    Message::Update {
                        stream: _,
                        tip,
                        model: _,
                    } => {
                        if let Ok(ipfs_path) = IpfsPath::from_str(tip) {
                            // Spawn task to get the data for a stream tip when we see one
                            let client = client.clone();
                            let metrics = metrics.clone();
                            task::spawn(async move {
                                let result = dag::get(client, &ipfs_path, DagJsonCodec).await;
                                metrics.record(&result);
                                match result {
                                    Ok(_) => info!("succeed in loading stream tip: {}", ipfs_path),
                                    Err(err) => warn!("failed to load stream tip: {}", err),
                                };
                            });
                        } else {
                            warn!("invalid update tip: {}", tip)
                        }
                    }
                    Message::Response { id: _, tips } => {
                        for tip in tips.values() {
                            if let Ok(ipfs_path) = IpfsPath::from_str(tip) {
                                // Spawn task to get the data for a stream tip when we see one
                                let client = client.clone();
                                let metrics = metrics.clone();
                                task::spawn(async move {
                                    let result = dag::get(client, &ipfs_path, DagJsonCodec).await;
                                    metrics.record(&result);
                                    match result {
                                        Ok(_) => {
                                            info!("succeed in loading stream tip: {}", ipfs_path)
                                        }
                                        Err(err) => warn!("failed to load stream tip: {}", err),
                                    };
                                });
                            } else {
                                warn!("invalid update tip: {}", tip)
                            }
                        }
                    }
                    _ => {}
                };
                metrics.record(&(from, msg));
            }
        }
        future::ready(())
    }));

    daemon.run().await?;
    daemon.shutdown().await?;

    p2p_events_handle.abort();
    p2p_events_handle.await.ok();
    Ok(())
}

fn metrics_config_with_compile_time_info(cfg: MetricsConfig) -> MetricsConfig {
    // compile time configuration
    cfg.with_service_name(env!("CARGO_PKG_NAME").to_string())
        .with_build(
            git_version::git_version!(
                prefix = "git:",
                cargo_prefix = "cargo:",
                fallback = "unknown"
            )
            .to_string(),
        )
        .with_version(env!("CARGO_PKG_VERSION").to_string())
}
