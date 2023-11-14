//! Ceramic implements a single binary ceramic node.
#![warn(missing_docs)]

mod http;
mod metrics;
mod network;
mod pubsub;
mod sql;

use std::{env, num::NonZeroUsize, path::PathBuf, str::FromStr, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use ceramic_core::{EventId, Interest, PeerId};
use ceramic_kubo_rpc::{dag, IpfsDep, IpfsPath, Multiaddr};

use ceramic_metrics::{config::Config as MetricsConfig, MetricsHandle, Recorder};
use ceramic_p2p::{load_identity, DiskStorage, Keychain, Libp2pConfig};
use clap::{Args, Parser, Subcommand, ValueEnum};
use futures::StreamExt;
use futures_util::future;
use libipld::json::DagJsonCodec;
use multibase::Base;
use multihash::{Code, Hasher, Multihash, MultihashDigest};
use recon::{FullInterests, Recon, ReconInterestProvider, SQLiteStore, Server, Sha256a};
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;
use swagger::{auth::MakeAllowAllAuthenticator, EmptyContext};
use tokio::{io::AsyncReadExt, sync::oneshot, task, time::timeout};
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
        default_values_t = vec!["/ip4/0.0.0.0/tcp/4001".to_string(), "/ip4/0.0.0.0/udp/4001/quic-v1".to_string()],
        use_value_delimiter = true,
        value_delimiter = ',',
        env = "CERAMIC_ONE_SWARM_ADDRESSES"
    )]
    swarm_addresses: Vec<String>,

    /// Path to storage directory
    #[arg(short, long, env = "CERAMIC_ONE_STORE_DIR")]
    store_dir: Option<PathBuf>,

    /// Bind address of the metrics endpoint.
    #[arg(
        short,
        long,
        default_value = "127.0.0.1:9464",
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

    /// Unique id when the network type is 'local'.
    #[arg(long, env = "CERAMIC_ONE_LOCAL_NETWORK_ID")]
    local_network_id: Option<u32>,

    /// When set mdns will be used to discover peers.
    #[arg(long, default_value_t = false, env = "CERAMIC_ONE_MDNS")]
    mdns: bool,

    /// When set Recon will be used to synchronized events with peers.
    #[arg(long, default_value_t = false, env = "CERAMIC_ONE_RECON")]
    recon: bool,

    /// When set autonat will not be used to discover external address or allow other peers
    /// to directly dial the local peer.
    #[arg(long, default_value_t = false, env = "CERAMIC_ONE_DISABLE_AUTONAT")]
    disable_autonat: bool,

    /// Specify the format of log events.
    #[arg(long, default_value = "multi-line", env = "CERAMIC_ONE_LOG_FORMAT")]
    log_format: LogFormat,

    /// Specify maximum established outgoing connections.
    #[arg(long, default_value_t = 2_000, env = "CERAMIC_ONE_MAX_CONNS_OUT")]
    max_conns_out: u32,

    /// Specify maximum established incoming connections.
    #[arg(long, default_value_t = 2_000, env = "CERAMIC_ONE_MAX_CONNS_IN")]
    max_conns_in: u32,

    /// Specify maximum pending outgoing connections.
    #[arg(long, default_value_t = 256, env = "CERAMIC_ONE_MAX_CONNS_PENDING_OUT")]
    max_conns_pending_out: u32,

    /// Specify maximum pending incoming connections.
    #[arg(long, default_value_t = 256, env = "CERAMIC_ONE_MAX_CONNS_PENDING_IN")]
    max_conns_pending_in: u32,

    /// Specify maximum established connections per peer regardless of direction (incoming or
    /// outgoing).
    #[arg(long, default_value_t = 8, env = "CERAMIC_ONE_MAX_CONNS_PER_PEER")]
    max_conns_per_peer: u32,

    /// Specify idle connection timeout in milliseconds.
    #[arg(
        long,
        default_value_t = 30_000,
        env = "CERAMIC_ONE_IDLE_CONNS_TIMEOUT_MS"
    )]
    idle_conns_timeout_ms: u64,

    /// Sets to how many closest peers a record is replicated.
    #[arg(long, default_value_t = NonZeroUsize::new(20).expect("> 0"), env = "CERAMIC_ONE_KADEMLIA_REPLICATION")]
    kademlia_replication: NonZeroUsize,

    /// Sets the allowed level of parallelism for iterative queries.
    #[arg(long, default_value_t = NonZeroUsize::new(16).expect("> 0"), env = "CERAMIC_ONE_KADEMLIA_PARALLELISM")]
    kademlia_parallelism: NonZeroUsize,

    /// Sets the timeout in seconds for a single query.
    ///
    /// **Note**: A single query usually comprises at least as many requests
    /// as the replication factor, i.e. this is not a request timeout.
    #[arg(
        long,
        default_value_t = 60,
        env = "CERAMIC_ONE_KADEMLIA_QUERY_TIMEOUT_SECS"
    )]
    kademlia_query_timeout_secs: u64,

    /// Sets the interval in seconds at which provider records for keys provided
    /// by the local node are re-published.
    ///
    /// `0` means that stored provider records are never automatically
    /// re-published.
    ///
    /// Must be significantly less than the provider record TTL.
    #[arg(
        long,
        default_value_t = 12 * 60 * 60,
        env = "CERAMIC_ONE_KADEMLIA_PROVIDER_PUBLICATION_INTERVAL_SECS"
    )]
    kademlia_provider_publication_interval_secs: u64,

    /// Sets the TTL in seconds for provider records.
    ///
    /// `0` means that stored provider records never expire.
    ///
    /// Must be significantly larger than the provider publication interval.
    #[arg(
        long,
        default_value_t = 24 * 60 * 60,
        env = "CERAMIC_ONE_KADEMLIA_PROVIDER_RECORD_TTL_SECS"
    )]
    kademlia_provider_record_ttl_secs: u64,
}

#[derive(ValueEnum, Debug, Clone, Default)]
enum LogFormat {
    /// Format log events on multiple lines using ANSI colors.
    #[default]
    MultiLine,
    /// Format log events on a single line using ANSI colors.
    SingleLine,
    /// Format log events newline delimited JSON objects.
    /// No ANSI colors are used.
    Json,
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

    /// bootstrap peers for Mainnet, TestnetClay, and DevUnstable
    /// should be kept in sync with js-ceramic.
    /// https://github.com/ceramicnetwork/js-ceramic/blob/develop/packages/ipfs-topology/src/ipfs-topology.ts
    fn bootstrap_addresses(&self) -> Vec<Multiaddr> {
        match self {
            Network::Mainnet => vec![
                "/dns4/go-ipfs-ceramic-private-mainnet-external.3boxlabs.com/tcp/4011/ws/p2p/QmXALVsXZwPWTUbsT8G6VVzzgTJaAWRUD7FWL5f7d5ubAL".to_string(), // cspell:disable-line
                "/dns4/go-ipfs-ceramic-private-cas-mainnet-external.3boxlabs.com/tcp/4011/ws/p2p/QmUvEKXuorR7YksrVgA7yKGbfjWHuCRisw2cH9iqRVM9P8".to_string(), // cspell:disable-line
            ],
            Network::TestnetClay => vec![
                "/dns4/go-ipfs-ceramic-public-clay-external.3boxlabs.com/tcp/4011/ws/p2p/QmWiY3CbNawZjWnHXx3p3DXsg21pZYTj4CRY1iwMkhP8r3".to_string(), // cspell:disable-line
                "/dns4/go-ipfs-ceramic-private-clay-external.3boxlabs.com/tcp/4011/ws/p2p/QmQotCKxiMWt935TyCBFTN23jaivxwrZ3uD58wNxeg5npi".to_string(), // cspell:disable-line
                "/dns4/go-ipfs-ceramic-private-cas-clay-external.3boxlabs.com/tcp/4011/ws/p2p/QmbeBTzSccH8xYottaYeyVX8QsKyox1ExfRx7T1iBqRyCd".to_string(), // cspell:disable-line
            ],
            Network::DevUnstable => vec![
                "/dns4/go-ipfs-ceramic-public-qa-external.3boxlabs.com/tcp/4011/ws/p2p/QmPP3RdaSWDkhcxZReGo591FWanLw9ucvgmUZhtSLt9t6D".to_string(),  // cspell:disable-line
                "/dns4/go-ipfs-ceramic-private-qa-external.3boxlabs.com/tcp/4011/ws/p2p/QmXcmXfLkkaGbQdj98cgGvHr5gkwJp4r79j9xbJajsoYHr".to_string(),  // cspell:disable-line
                "/dns4/go-ipfs-ceramic-private-cas-qa-external.3boxlabs.com/tcp/4011/ws/p2p/QmRvJ4HX4N6H26NgtqjoJEUyaDyDRUhGESP1aoyCJE1X1b".to_string(),  // cspell:disable-line
            ],
            Network::Local => vec![],
            Network::InMemory => vec![],
        }
        .iter()
        .map(|addr| addr.parse())
        .collect::<Result<Vec<Multiaddr>, multiaddr::Error>>()
        .expect("hard coded bootstrap addresses should parse")
    }
}

#[derive(Args, Debug)]
struct EyeOpts {
    #[command(flatten)]
    daemon: DaemonOpts,
}

/// Run the ceramic one binary process
pub async fn run() -> Result<()> {
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

        let info = Info::new().await?;

        let mut metrics_config = MetricsConfig {
            collect: opts.metrics,
            // Do not push metrics to any endpoint.
            export: false,
            tracing: opts.tracing,
            log_format: match opts.log_format {
                LogFormat::SingleLine => ceramic_metrics::config::LogFormat::SingleLine,
                LogFormat::MultiLine => ceramic_metrics::config::LogFormat::MultiLine,
                LogFormat::Json => ceramic_metrics::config::LogFormat::Json,
            },
            ..Default::default()
        };
        info.apply_to_metrics_config(&mut metrics_config);

        let metrics = ceramic_metrics::MetricsHandle::register(|registry| {
            crate::metrics::Metrics::register(info.clone(), registry)
        });
        let metrics = Arc::new(metrics);

        // Logging Tracing and metrics are initialized here,
        // debug,info etc will not work until after this line
        let metrics_handle = ceramic_metrics::MetricsHandle::new(metrics_config.clone())
            .await
            .expect("failed to initialize metrics");
        info!(
            service__name = info.service_name,
            version = info.version,
            build = info.build,
            instance_id = info.instance_id,
            exe_hash = info.exe_hash,
        );
        debug!(?opts, "using daemon options");

        // 1 path from options
        // 2 path $HOME/.ceramic-one
        // 3 pwd/.ceramic-one
        let dir = match opts.store_dir {
            Some(dir) => dir,
            None => match home::home_dir() {
                Some(home_dir) => home_dir.join(".ceramic-one"),
                None => PathBuf::from(".ceramic-one"),
            },
        };
        debug!("using directory: {}", dir.display());

        let p2p_config = Libp2pConfig {
            mdns: opts.mdns,
            bitswap_server: true,
            bitswap_client: true,
            kademlia: true,
            autonat: !opts.disable_autonat,
            relay_server: true,
            relay_client: true,
            gossipsub: true,
            max_conns_out: opts.max_conns_out,
            max_conns_in: opts.max_conns_in,
            max_conns_pending_out: opts.max_conns_pending_out,
            max_conns_pending_in: opts.max_conns_pending_in,
            max_conns_per_peer: opts.max_conns_per_peer,
            idle_connection_timeout: Duration::from_millis(opts.idle_conns_timeout_ms),
            bootstrap_peers: opts.network.bootstrap_addresses(),
            listening_multiaddrs: opts
                .swarm_addresses
                .iter()
                .map(|addr| addr.parse())
                .collect::<Result<Vec<Multiaddr>, multiaddr::Error>>()?,
            kademlia_replication_factor: opts.kademlia_replication,
            kademlia_parallelism: opts.kademlia_parallelism,
            kademlia_query_timeout: Duration::from_secs(opts.kademlia_query_timeout_secs),
            kademlia_provider_publication_interval: if opts
                .kademlia_provider_publication_interval_secs
                == 0
            {
                None
            } else {
                Some(Duration::from_secs(
                    opts.kademlia_provider_publication_interval_secs,
                ))
            },
            kademlia_provider_record_ttl: if opts.kademlia_provider_record_ttl_secs == 0 {
                None
            } else {
                Some(Duration::from_secs(opts.kademlia_provider_record_ttl_secs))
            },
            ..Default::default()
        };
        debug!(?p2p_config, "using p2p config");

        // Load p2p identity
        let mut kc = Keychain::<DiskStorage>::new(dir.clone()).await?;
        let keypair = load_identity(&mut kc).await?;
        let peer_id = keypair.public().to_peer_id();

        // Connect to sqlite
        let sql_db_path: PathBuf = dir.join("db.sqlite3");
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
        let ipfs_metrics =
            ceramic_metrics::MetricsHandle::register(ceramic_kubo_rpc::IpfsMetrics::register);
        let p2p_metrics = ceramic_metrics::MetricsHandle::register(ceramic_p2p::Metrics::register);
        let ipfs = Ipfs::builder()
            .with_p2p(p2p_config, keypair, recons, sql_pool.clone(), p2p_metrics)
            .await?
            .build(sql_pool.clone(), ipfs_metrics)
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
        let kubo_rpc_metrics =
            ceramic_metrics::MetricsHandle::register(ceramic_kubo_rpc::http::Metrics::register);
        // Wrap server in metrics middleware
        let kubo_rpc_server =
            ceramic_kubo_rpc::http::MetricsMiddleware::new(kubo_rpc_server, kubo_rpc_metrics);
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

/// Static information about the current process.
#[derive(Debug, Clone)]
pub struct Info {
    /// Name of the service.
    pub service_name: String,
    /// Semantic version of the build.
    pub version: String,
    /// Description of git commit.
    pub build: String,
    /// Unique name generated for this invocation of the process.
    pub instance_id: String,
    /// Multibase encoded multihash of the current running executable.
    pub exe_hash: String,
}

impl Info {
    async fn new() -> Result<Self> {
        let exe_hash = multibase::encode(Base::Base64Url, current_exe_hash().await?.to_bytes());
        Ok(Self {
            service_name: env!("CARGO_PKG_NAME").to_string(),
            build: git_version::git_version!(
                prefix = "git:",
                cargo_prefix = "cargo:",
                fallback = "unknown"
            )
            .to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            instance_id: names::Generator::default().next().unwrap(),
            exe_hash,
        })
    }
    fn apply_to_metrics_config(&self, cfg: &mut MetricsConfig) {
        cfg.service_name = self.service_name.clone();
        cfg.version = self.version.clone();
        cfg.build = self.build.clone();
        cfg.instance_id = self.instance_id.clone();
    }
}

async fn current_exe_hash() -> Result<Multihash> {
    if cfg!(debug_assertions) {
        // Debug builds can be 1GB+, so do we not want to spend the time to hash them.
        // Return a fake hash.
        let mut hash = multihash::Identity256::default();
        // Spells debugg when base64 url encoded with some leading padding.
        hash.update(&[00, 117, 230, 238, 130]);
        Ok(Code::Identity.wrap(hash.finalize())?)
    } else {
        let exe_path = env::current_exe()?;
        let mut hasher = multihash::Sha2_256::default();
        let mut f = tokio::fs::File::open(exe_path).await?;
        let mut buffer = vec![0; 4096];

        loop {
            let bytes_read = f.read(&mut buffer[..]).await?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }
        let hash = hasher.finalize();
        Ok(Code::Sha2_256.wrap(hash)?)
    }
}
