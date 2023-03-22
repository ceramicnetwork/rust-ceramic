//! Ceramic implements a single binary ceramic node.
#![deny(warnings)]
#![deny(missing_docs)]

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use iroh_embed::{IrohBuilder, Libp2pConfig, P2pService, RocksStoreService};
use iroh_metrics::config::Config as MetricsConfig;
use tracing::{debug, info};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Daemon(DaemonOpts),
}

#[derive(Args, Debug)]
struct DaemonOpts {
    #[arg(short, long, default_value = "127.0.0.1:5001")]
    bind_address: String,
    #[arg(short, long)]
    store_dir: Option<PathBuf>,
    #[arg(short, long, default_value_t = false)]
    metrics: bool,
    #[arg(short, long, default_value_t = false)]
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
    let metrics_handle = iroh_metrics::MetricsHandle::new(metrics_config)
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
    p2p_config.bootstrap_peers = vec![
        "/dns4/go-ipfs-ceramic-private-mainnet-external.3boxlabs.com/tcp/4011/ws/p2p/QmXALVsXZwPWTUbsT8G6VVzzgTJaAWRUD7FWL5f7d5ubAL".parse().unwrap(),
        "/dns4/go-ipfs-ceramic-private-cas-mainnet-external.3boxlabs.com/tcp/4011/ws/p2p/QmUvEKXuorR7YksrVgA7yKGbfjWHuCRisw2cH9iqRVM9P8".parse().unwrap(),
        "/dns4/go-ipfs-ceramic-elp-1-1-external.3boxlabs.com/tcp/4011/ws/p2p/QmUiF8Au7wjhAF9BYYMNQRW5KhY7o8fq4RUozzkWvHXQrZ".parse().unwrap(),
        "/dns4/go-ipfs-ceramic-elp-1-2-external.3boxlabs.com/tcp/4011/ws/p2p/QmRNw9ZimjSwujzS3euqSYxDW9EHDU5LB3NbLQ5vJ13hwJ".parse().unwrap(),
    ];
    p2p_config.listening_multiaddrs = vec![
        "/ip4/0.0.0.0/tcp/0".parse().unwrap(),
        "/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap(),
    ];
    let p2p = P2pService::new(p2p_config, dir, store.addr()).await?;

    // Note by default this is configured with an indexer, but not with http resolvers.
    let iroh = IrohBuilder::new().store(store).p2p(p2p).build().await?;

    // Run the HTTP server
    ceramic_kubo_rpc::http::serve(iroh.api().clone(), opts.bind_address).await?;

    // Stop the system gracefully.
    iroh.stop().await?;

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
