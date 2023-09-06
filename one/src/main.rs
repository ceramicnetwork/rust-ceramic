//! Ceramic implements a single binary ceramic node.
#![warn(missing_docs)]

mod http;
mod metrics;
mod network;
mod pubsub;
mod sql;

use std::{path::PathBuf, str::FromStr, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use ceramic_core::{EventId, Interest, PeerId};
use ceramic_kubo_rpc::{dag, IpfsDep, IpfsPath, Multiaddr};

use ceramic_p2p::{load_identity, DiskStorage, Keychain, Libp2pConfig};
use clap::{Args, Parser, Subcommand, ValueEnum};
use futures::StreamExt;
use futures_util::future;
use iroh_metrics::{config::Config as MetricsConfig, MetricsHandle};
use libipld::json::DagJsonCodec;
use libp2p::metrics::Recorder;
use recon::{FullInterests, Recon, ReconInterestProvider, SQLiteStore, Server, Sha256a};
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;
use swagger::{auth::MakeAllowAllAuthenticator, EmptyContext};
use tokio::{sync::oneshot, task, time::timeout};
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
    /// Bind address of the API endpoint.
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
    /// Unique key used to find other Ceramic peers via the DHT
    #[arg(long, default_value = "testnet-clay", env = "CERAMIC_ONE_NETWORK")]
    network: Network,

    /// Unique key used to find other Ceramic peers via the DHT
    #[arg(long, env = "CERAMIC_ONE_LOCAL_NETWORK_ID")]
    local_network_id: Option<u32>,

    /// When true mdns will be used to discover peers.
    #[arg(long, default_value_t = false, env = "CERAMIC_ONE_MDNS")]
    mdns: bool,

    /// When true Recon will be used to synchronized events with peers.
    #[arg(long, default_value_t = false, env = "CERAMIC_ONE_RECON")]
    recon: bool,
}

#[derive(ValueEnum, Debug, Clone)]
enum Network {
    /// Production network
    Mainnet,
    /// Test network
    TestnetClay,
    /// Development network
    DevUnstable,
    /// Local network with unique id
    Local,
    /// Singleton network in memory
    InMemory,
}

impl Network {
    fn to_network(&self, local_id: &Option<u32>) -> Result<ceramic_core::Network> {
        Ok(match self {
            Network::Mainnet => ceramic_core::Network::Mainnet,
            Network::TestnetClay => ceramic_core::Network::TestnetClay,
            Network::DevUnstable => ceramic_core::Network::DevUnstable,
            Network::Local => ceramic_core::Network::Local(
                local_id.ok_or_else(|| anyhow!("must provide a local network id"))?,
            ),
            Network::InMemory => ceramic_core::Network::InMemory,
        })
    }
}

#[derive(Args, Debug)]
struct EyeOpts {
    #[command(flatten)]
    daemon: DaemonOpts,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let (non_blocking, _guard) = tracing_appender::non_blocking(std::io::stdout());

    let subscriber = tracing_subscriber::fmt().with_writer(non_blocking);

    tracing::subscriber::with_default(subscriber.finish(), || {
        tracing::event!(tracing::Level::INFO, "Ceramic One Server Running");
    });

    let args = Cli::parse();
    match args.command {
        Command::Daemon(opts) => {
            let daemon = Daemon::build(opts).await?;
            daemon.run().await
        }
        Command::Eye(opts) => eye(opts).await,
    }
}

type InterestStore = SQLiteStore<Interest, Sha256a>;
type InterestInterest = FullInterests<Interest>;
type ReconInterest = Server<Interest, Sha256a, InterestStore, InterestInterest>;

type ModelStore = SQLiteStore<EventId, Sha256a>;
type ModelInterest = ReconInterestProvider<Sha256a>;
type ReconModel = Server<EventId, Sha256a, ModelStore, ModelInterest>;

struct Daemon {
    peer_id: PeerId,
    network: ceramic_core::Network,
    bind_address: String,
    metrics_bind_address: String,
    ipfs: Ipfs,
    metrics_handle: MetricsHandle,
    metrics: Arc<Metrics>,
    recon_interest: ReconInterest,
    recon_model: ReconModel,
}

impl Daemon {
    async fn build(opts: DaemonOpts) -> Result<Self> {
        let network = opts.network.to_network(&opts.local_network_id)?;

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
        p2p_config.mdns = opts.mdns;
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

        let sql_db_path = dir.join("db.sqlite3");

        // Load p2p identity
        let mut kc = Keychain::<DiskStorage>::new(dir.clone()).await?;
        let keypair = load_identity(&mut kc).await?;
        let peer_id = keypair.public().to_peer_id();

        // Connect to sqlite
        let sql_pool = sql::connect(&sql_db_path).await?;

        // Create recon store for interests.
        let interest_store = InterestStore::new(sql_pool.clone(), "interest".to_string()).await?;

        // Create second recon store for models.
        let model_store = ModelStore::new(sql_pool.clone(), "model".to_string()).await?;

        // Construct a recon implementation for interests.
        let mut recon_interest =
            Server::new(Recon::new(interest_store, InterestInterest::default()));

        // Construct a recon implementation for models.
        let mut recon_model = Server::new(Recon::new(
            model_store,
            // Use recon interests as the InterestProvider for recon_model
            ModelInterest::new(peer_id, recon_interest.client()),
        ));

        let recons = if opts.recon {
            Some((recon_interest.client(), recon_model.client()))
        } else {
            None
        };
        let ipfs = Ipfs::builder()
            .with_store(dir.join("store"))
            .await?
            .with_p2p(p2p_config, keypair, recons, &network.name())
            .await?
            .build()
            .await?;

        Ok(Daemon {
            peer_id,
            network,
            bind_address: opts.bind_address,
            metrics_bind_address: opts.metrics_bind_address,
            ipfs,
            metrics_handle,
            metrics,
            recon_interest,
            recon_model,
        })
    }
    // Start the daemon, future does not return until the daemon is finished.
    async fn run(mut self) -> Result<()> {
        // Start metrics server
        debug!(
            bind_address = self.metrics_bind_address,
            "starting prometheus metrics server"
        );
        let (tx_metrics_server_shutdown, metrics_server_handle) =
            metrics::start(&self.metrics_bind_address.parse()?);

        // Build HTTP server
        let network = self.network.clone();
        let ceramic_server = ceramic_api::Server::new(
            self.peer_id,
            network,
            self.recon_interest.client(),
            self.recon_model.client(),
        );
        let ceramic_service = ceramic_api_server::server::MakeService::new(ceramic_server);
        let ceramic_service = MakeAllowAllAuthenticator::new(ceramic_service, "");
        let ceramic_service =
            ceramic_api_server::context::MakeAddContext::<_, EmptyContext>::new(ceramic_service);

        let kubo_rpc_server = ceramic_kubo_rpc::http::Server::new(self.ipfs.api());
        let kubo_rpc_service = ceramic_kubo_rpc_server::server::MakeService::new(kubo_rpc_server);
        let kubo_rpc_service = MakeAllowAllAuthenticator::new(kubo_rpc_service, "");
        let kubo_rpc_service =
            ceramic_kubo_rpc_server::context::MakeAddContext::<_, EmptyContext>::new(
                kubo_rpc_service,
            );

        // Compose both services
        let service = http::MakePrefixService::new(
            ("/ceramic/".to_string(), ceramic_service),
            ("/api/v0/".to_string(), kubo_rpc_service),
        );

        // Start recon tasks
        let recon_interest_handle = tokio::spawn(self.recon_interest.run());
        let recon_model_handle = tokio::spawn(self.recon_model.run());

        // Start HTTP server with a graceful shutdown
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let signals = Signals::new([SIGHUP, SIGTERM, SIGINT, SIGQUIT])?;
        let handle = signals.handle();

        debug!("starting signal handler task");
        let signals_handle = tokio::spawn(handle_signals(signals, tx));

        // The server task blocks until we are ready to start shutdown
        debug!("starting api server");
        hyper::server::Server::bind(&self.bind_address.parse()?)
            .serve(service)
            .with_graceful_shutdown(async {
                rx.await.ok();
            })
            .await?;
        debug!("api server finished, starting shutdown...");

        // Stop IPFS.
        if let Err(err) = self.ipfs.stop().await {
            warn!(%err,"ipfs task error");
        }
        debug!("ipfs stopped");

        // Drop recon_model first as it contains a client into recon_interest.
        if let Err(err) = recon_model_handle.await {
            warn!(%err, "recon models task error");
        }
        debug!("recon models server stopped");
        if let Err(err) = recon_interest_handle.await {
            warn!(%err, "recon interest task error");
        }
        debug!("recon interests server stopped");

        // Shutdown metrics server and collection handler
        tx_metrics_server_shutdown
            .send(())
            .expect("should be able to send metrics shutdown message");
        if let Err(err) = metrics_server_handle.await? {
            warn!(%err, "metrics server task error")
        }
        self.metrics_handle.shutdown();
        debug!("metrics server stopped");

        // Wait for signal handler to finish
        handle.close();
        signals_handle.await?;
        debug!("signal handler stopped");

        Ok(())
    }
}
async fn handle_signals(mut signals: Signals, shutdown: oneshot::Sender<()>) {
    let mut shutdown = Some(shutdown);
    while let Some(signal) = signals.next().await {
        debug!(?signal, "signal received");
        if let Some(shutdown) = shutdown.take() {
            info!("sending shutdown message");
            shutdown
                .send(())
                .expect("should be able to send shutdown message");
        }
    }
}

async fn eye(opts: EyeOpts) -> Result<()> {
    let daemon = Daemon::build(opts.daemon).await?;

    // Start subscription
    let subscription = daemon
        .ipfs
        .api()
        .subscribe(daemon.network.name().clone())
        .await?;

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
