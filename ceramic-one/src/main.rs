//! Ceramic implements a single binary ceramic node.
#![deny(warnings)]
#![deny(missing_docs)]

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use iroh_api::Multiaddr;
use futures_util::{future, StreamExt};
use iroh_embed::{IrohBuilder, Libp2pConfig, P2pService, RocksStoreService};
use iroh_metrics::config::Config as MetricsConfig;
use libp2p::metrics::Recorder;
use tokio::task;
use tracing::{debug, info};

use crate::{ pubsub::Message};

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
    /// Path to storage directory
    #[arg(short, long, env = "CERAMIC_ONE_STORE_DIR")]
    store_dir: Option<PathBuf>,
    /// When true metrics will be exported
    #[arg(short, long, default_value_t = false, env = "CERAMIC_ONE_METRICS")]
    metrics: bool,
    /// When true traces will be exported
    #[arg(short, long, default_value_t = false, env = "CERAMIC_ONE_TRACING")]
    tracing: bool,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        Command::Daemon(opts) => daemon(opts).await,
    }
}

async fn daemon(opts: DaemonOpts) -> Result<()> {
    let mut metrics_config = MetricsConfig::default();
    metrics_config = metrics_config_with_compile_time_info(metrics_config);
    metrics_config.collect = opts.metrics;
    metrics_config.tracing = opts.tracing;
    let service_name = metrics_config.service_name.clone();
    let instance_id = metrics_config.instance_id.clone();
    let metrics = iroh_metrics::MetricsHandle::register(crate::metrics::Metrics::new);

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
    p2p_config.max_conns_out = 200;
    p2p_config.max_conns_in = 200;
    //p2p_config.max_conns_pending_out = 5;
    //p2p_config.max_conns_pending_in = 5;
    // Do not use any bootstrap_peers,
    // clients should initialize the bootstrap_peers.
    p2p_config.bootstrap_peers = vec![];
    p2p_config.listening_multiaddrs = opts
        .swarm_addresses
        .iter()
        .map(|addr| addr.parse())
        .collect::<Result<Vec<Multiaddr>, multiaddr::Error>>()?;
    let p2p = P2pService::new(p2p_config, dir, store.addr()).await?;

    // Register ceramic-one info
    //let local_peer_id = p2p.local_peer_id();
    //let info = MetricsInfo {
    //    config: metrics_config,
    //    local_peer_id,
    //};
    //iroh_metrics::MetricsHandle::register(crate::metrics::register_info(info));

    // Note by default this is configured with an indexer, but not with http resolvers.
    let iroh = IrohBuilder::new().store(store).p2p(p2p).build().await?;

    let subscription = iroh
        .api()
        .p2p()?
        .subscribe("/ceramic/testnet-clay".to_string())
        .await?;

    let p2p_events_handle = task::spawn(subscription.for_each(move |event| {
        match event.expect("should be a message") {
            iroh_api::GossipsubEvent::Subscribed { .. } => {}
            iroh_api::GossipsubEvent::Unsubscribed { .. } => {}
            iroh_api::GossipsubEvent::Message {
                from,
                id: _,
                message,
                topic: _,
            } => {
                info!(
                    "message data {}",
                    String::from_utf8(message.data.clone()).unwrap()
                );
                let msg: Message = serde_json::from_slice(message.data.as_slice())
                    .expect("should be json message");
                info!(?msg);
                metrics.record(&(from, msg));
            }
        }
        future::ready(())
    }));

    // Run the HTTP server
    ceramic_kubo_rpc::http::serve(iroh.api().clone(), opts.bind_address).await?;

    // Stop the system gracefully.
    iroh.stop().await?;

    p2p_events_handle.abort();
    p2p_events_handle.await.ok();

    metrics_handle.shutdown();
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
