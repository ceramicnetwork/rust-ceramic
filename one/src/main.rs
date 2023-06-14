//! Ceramic implements a single binary ceramic node.
#![deny(missing_docs)]

mod metrics;
mod network;
mod pubsub;

use std::{path::PathBuf, str::FromStr, sync::Arc, time::Duration};

use anyhow::Result;
use ceramic_kubo_rpc::{dag, IpfsDep, IpfsPath, Multiaddr};
use ceramic_p2p::Libp2pConfig;
use clap::{Args, Parser, Subcommand};
use futures::StreamExt;
use futures_util::future;
use iroh_metrics::{config::Config as MetricsConfig, MetricsHandle};
use libipld::json::DagJsonCodec;
use libp2p::metrics::Recorder;
use tokio::{task, time::timeout};
use tracing::{debug, info, warn};

use crate::{
    metrics::{Metrics, TipLoadResult},
    network::Ipfs,
    pubsub::Message,
};

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
    /// Bind address of the RPC endpoint.
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
        use_value_delimiter = true,
        value_delimiter = ',',
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
    /// Bind address of the metrics endpoint.
    #[arg(
        short,
        long,
        default_value = "127.0.0.1:9090",
        env = "CERAMIC_ONE_METRICS_BIND_ADDRESS"
    )]
    metrics_bind_address: String,
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
    metrics_bind_address: String,
    ipfs: Ipfs,
    metrics_handle: MetricsHandle,
    metrics: Arc<Metrics>,
}

impl Daemon {
    async fn build(opts: DaemonOpts) -> Result<Self> {
        let mut metrics_config = MetricsConfig::default();
        metrics_config = metrics_config_with_compile_time_info(metrics_config);
        metrics_config.collect = opts.metrics;
        // Do not push metrics to any endpoint.
        metrics_config.export = false;
        metrics_config.tracing = opts.tracing;
        let service_name = metrics_config.service_name.clone();
        let instance_id = metrics_config.instance_id.clone();

        let metrics = iroh_metrics::MetricsHandle::register(crate::metrics::Metrics::new);
        let metrics = Arc::new(metrics);

        // Logging Tracing and metrics are initialized here,
        // debug,info etc will not work until after this line
        let metrics_handle = iroh_metrics::MetricsHandle::new(metrics_config.clone())
            .await
            .expect("failed to initialize metrics");
        info!(service_name, instance_id);
        debug!(?opts, "using daemon options");

        let dir = match opts.store_dir {
            Some(dir) => dir,
            None => match home::home_dir() {
                Some(home_dir) => home_dir.join(".ceramic-one"),
                None => PathBuf::from(".ceramic-one"),
            },
        };
        debug!("using directory: {}", dir.display());

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
        debug!(?p2p_config, "using p2p config");

        // Construct a recon implementation.
        let recon = Arc::new(std::sync::Mutex::new(recon::Recon::from_set([].into())));

        let ipfs = Ipfs::builder()
            .with_store(dir.join("store"))
            .await?
            .with_p2p(p2p_config, dir, Some(recon))
            .await?
            .build()
            .await?;

        Ok(Daemon {
            bind_address: opts.bind_address,
            metrics_bind_address: opts.metrics_bind_address,
            ipfs,
            metrics_handle,
            metrics,
        })
    }
    // Start the daemon, future does not return until the daemon is finished.
    async fn run(&self) -> Result<()> {
        // Start metrics server
        debug!(
            bind_addres = self.metrics_bind_address,
            "starting prometheus metrics server"
        );
        let srv = metrics::server(self.metrics_bind_address.as_str())?;
        let srv_handle = srv.handle();
        tokio::spawn(srv);

        // Run the Kubo RPC server, this blocks until the server is shutdown via a unix signal.
        debug!(
            bind_addres = self.bind_address,
            "starting Kubo RPC API server"
        );
        ceramic_kubo_rpc::http::serve(self.ipfs.api(), self.bind_address.as_str()).await?;

        // Shutdown metrics server
        srv_handle.stop(false).await;
        Ok(())
    }
    // Stop the system gracefully.
    async fn shutdown(self) -> Result<()> {
        // Stop IPFS before metrics
        let res = self.ipfs.stop().await;

        // Always shutdown metrics even if ipfs errors
        self.metrics_handle.shutdown();

        // Check ipfs shutdown error
        res?;

        Ok(())
    }
}

async fn eye(opts: EyeOpts) -> Result<()> {
    let daemon = Daemon::build(opts.daemon).await?;

    // Start subscription
    let subscription = daemon.ipfs.api().subscribe(opts.topic).await?;

    let client = daemon.ipfs.api();
    let metrics = daemon.metrics.clone();

    let p2p_events_handle = task::spawn(subscription.for_each(move |event| {
        match event.expect("should be a message") {
            ceramic_kubo_rpc::GossipsubEvent::Subscribed { .. } => {}
            ceramic_kubo_rpc::GossipsubEvent::Unsubscribed { .. } => {}
            ceramic_kubo_rpc::GossipsubEvent::Message {
                // From is the direct peer that forwarded the message
                from: _,
                id: _,
                message: pubsub_msg,
            } => {
                let ceramic_msg: Message = serde_json::from_slice(pubsub_msg.data.as_slice())
                    .expect("should be json message");
                info!(?ceramic_msg);
                match &ceramic_msg {
                    Message::Update {
                        stream: _,
                        tip,
                        model: _,
                    } => {
                        if let Ok(ipfs_path) = IpfsPath::from_str(tip) {
                            // Spawn task to get the data for a stream tip when we see one
                            let client = client.clone();
                            let metrics = metrics.clone();
                            task::spawn(async move { load_tip(client, metrics, &ipfs_path).await });
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
                                task::spawn(
                                    async move { load_tip(client, metrics, &ipfs_path).await },
                                );
                            } else {
                                warn!("invalid update tip: {}", tip)
                            }
                        }
                    }
                    _ => {}
                };
                metrics.record(&(pubsub_msg.source, ceramic_msg));
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

async fn load_tip<T: IpfsDep>(client: T, metrics: Arc<Metrics>, ipfs_path: &IpfsPath) {
    let result = timeout(
        Duration::from_secs(60 * 60),
        dag::get(client, ipfs_path, DagJsonCodec),
    )
    .await;
    let lr = match result {
        Ok(Ok(_)) => {
            info!("succeed in loading stream tip: {}", ipfs_path);
            TipLoadResult::Success
        }
        Ok(Err(err)) => {
            warn!("failed to load stream tip: {}", err);
            TipLoadResult::Failure
        }
        Err(_) => {
            warn!("timeout loading stream tip");
            TipLoadResult::Failure
        }
    };
    metrics.record(&lr);
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
