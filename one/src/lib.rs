//! Ceramic implements a single binary ceramic node.
#![warn(missing_docs)]

mod http;
mod http_metrics;
mod metrics;
mod migrations;
mod network;

use std::{env, path::PathBuf, time::Duration};

use anyhow::{anyhow, Context, Result};
use ceramic_api::{EventStore, InterestStore};
use ceramic_core::{EventId, Interest};
use ceramic_kubo_rpc::Multiaddr;
use ceramic_metrics::{config::Config as MetricsConfig, MetricsHandle};
use ceramic_p2p::{load_identity, DiskStorage, Keychain, Libp2pConfig};
use ceramic_service::{CeramicEventService, CeramicInterestService, CeramicService};
use clap::{Args, Parser, Subcommand, ValueEnum};
use futures::StreamExt;
use multibase::Base;
use multihash::Multihash;
use multihash_codetable::Code;
use multihash_derive::Hasher;
use recon::{FullInterests, Recon, ReconInterestProvider, Server, Sha256a};
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;
use std::sync::Arc;
use swagger::{auth::MakeAllowAllAuthenticator, EmptyContext};
use tokio::{io::AsyncReadExt, sync::oneshot};
use tracing::{debug, error, info, warn};

use crate::network::Ipfs;

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
    /// Perform various migrations
    #[command(subcommand)]
    Migrations(migrations::EventsCommand),
}

#[derive(Args, Debug)]
struct DaemonOpts {
    #[command(flatten)]
    db_opts: DBOpts,

    /// Path to libp2p private key directory
    #[arg(short, long, default_value = ".", env = "CERAMIC_ONE_P2P_KEY_DIR")]
    p2p_key_dir: PathBuf,

    /// Bind address of the API endpoint.
    #[arg(
        short,
        long,
        default_value = "127.0.0.1:5101",
        env = "CERAMIC_ONE_BIND_ADDRESS"
    )]
    bind_address: String,

    /// Listen address of the p2p swarm.
    #[arg(
        long,
        default_values_t = vec!["/ip4/0.0.0.0/tcp/4101".to_string(), "/ip4/0.0.0.0/udp/4101/quic-v1".to_string()],
        use_value_delimiter = true,
        value_delimiter = ',',
        env = "CERAMIC_ONE_SWARM_ADDRESSES"
    )]
    swarm_addresses: Vec<String>,

    /// External address of the p2p swarm.
    /// These addressed are advertised to remote peers in order to dial the local peer.
    #[arg(
        long,
        use_value_delimiter = true,
        value_delimiter = ',',
        env = "CERAMIC_ONE_EXTERNAL_SWARM_ADDRESSES"
    )]
    external_swarm_addresses: Vec<String>,

    /// Extra addresses of peers that participate in the Ceramic network.
    /// A best-effort attempt will be made to maintain a connection to these addresses.
    #[arg(
        long,
        use_value_delimiter = true,
        value_delimiter = ',',
        env = "CERAMIC_ONE_EXTRA_CERAMIC_PEER_ADDRESSES"
    )]
    extra_ceramic_peer_addresses: Vec<String>,

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

    /// When true the tokio console will be exposed
    #[cfg(feature = "tokio-console")]
    #[arg(long, default_value_t = false, env = "CERAMIC_ONE_TOKIO_CONSOLE")]
    tokio_console: bool,

    /// Unique key used to find other Ceramic peers via the DHT
    #[arg(long, default_value = "testnet-clay", env = "CERAMIC_ONE_NETWORK")]
    network: Network,

    /// Unique id when the network type is 'local'.
    #[arg(long, env = "CERAMIC_ONE_LOCAL_NETWORK_ID")]
    local_network_id: Option<u32>,

    #[command(flatten)]
    log_opts: LogOpts,

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

    /// Allowed CORS origins. Should include the transport e.g. https:// or http://.
    #[arg(
            long,
            default_values_t = vec!["*".to_string()],
            use_value_delimiter = true,
            value_delimiter = ',',
            env = "CERAMIC_ONE_CORS_ALLOW_ORIGINS"
        )]
    cors_allow_origins: Vec<String>,
}

#[derive(Args, Debug)]
struct DBOpts {
    /// Path to storage directory
    #[arg(short, long, default_value = ".", env = "CERAMIC_ONE_STORE_DIR")]
    store_dir: PathBuf,
}

#[derive(Args, Debug)]
struct LogOpts {
    /// Specify the format of log events.
    #[arg(long, default_value = "multi-line", env = "CERAMIC_ONE_LOG_FORMAT")]
    log_format: LogFormat,
}

impl LogOpts {
    fn format(&self) -> ceramic_metrics::config::LogFormat {
        match self.log_format {
            LogFormat::SingleLine => ceramic_metrics::config::LogFormat::SingleLine,
            LogFormat::MultiLine => ceramic_metrics::config::LogFormat::MultiLine,
            LogFormat::Json => ceramic_metrics::config::LogFormat::Json,
        }
    }
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
    #[clap(alias = "inmemory")]
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
                "/dns4/bootstrap-mainnet-rust-ceramic-1.3box.io/tcp/4101/p2p/12D3KooWJC1yR4KiCnocV9kuAEwtsMNh7Xmu2vzqpBvk2o3MrYd6",
                "/dns4/bootstrap-mainnet-rust-ceramic-2.3box.io/tcp/4101/p2p/12D3KooWCuS388c1im7KkmdrpsLMziihF8mbcv2w6HPCp4Qmww6m",
            ],
            Network::TestnetClay => vec![
                "/dns4/bootstrap-tnet-rust-ceramic-1.3box.io/tcp/4101/p2p/12D3KooWMqCFj5bnwuNi6D6KLhYiK4C8Eh9xSUKv2E6Jozs4nWEE",
                "/dns4/bootstrap-tnet-rust-ceramic-2.3box.io/tcp/4101/p2p/12D3KooWPFGbRHWfDaWt5MFFeqAHBBq3v5BqeJ4X7pmn2V1t6uNs",
            ],
            Network::DevUnstable => vec![
                "/dns4/bootstrap-devqa-rust-ceramic-1.3box.io/tcp/4101/p2p/12D3KooWJmYPnXgst4gW5GoyAYzRB3upLgLVR1oDVGwjiS9Ce7sA",
                "/dns4/bootstrap-devqa-rust-ceramic-2.3box.io/tcp/4101/p2p/12D3KooWFCf7sKeW8NHoT35EutjJX5vCpPekYqa4hB4tTUpYrcam",
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

/// Run the ceramic one binary process
pub async fn run() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        Command::Daemon(opts) => Daemon::run(opts).await,
        Command::Migrations(opts) => migrations::migrate(opts).await,
    }
}

type InterestInterest = FullInterests<Interest>;
type ModelInterest = ReconInterestProvider<Sha256a>;

impl DBOpts {
    /// This function will create the database directory if it does not exist.
    async fn get_database(&self, process_undelivered: bool) -> Result<Databases> {
        match tokio::fs::create_dir_all(&self.store_dir).await {
            Ok(_) => {}
            Err(err) => match err.kind() {
                std::io::ErrorKind::AlreadyExists => {}
                _ => {
                    error!(
                        dir = %self.store_dir.display(),
                        %err, "failed to create required directory"
                    );
                    anyhow::bail!(err);
                }
            },
        }
        let sql_db_path = self.store_dir.join("db.sqlite3").display().to_string();
        Self::build_sqlite_dbs(&sql_db_path, process_undelivered).await
    }

    async fn build_sqlite_dbs(path: &str, process_undelivered: bool) -> Result<Databases> {
        let sql_pool =
            ceramic_store::SqlitePool::connect(path, ceramic_store::Migrations::Apply).await?;
        let ceramic_service = CeramicService::try_new(sql_pool).await?;
        let interest_store = ceramic_service.interest_service().to_owned();
        let event_store = ceramic_service.event_service().to_owned();
        if process_undelivered {
            event_store.process_all_undelivered_events().await?;
        }
        info!(path, "connected to sqlite db");

        Ok(Databases::Sqlite(SqliteBackend {
            event_store,
            interest_store,
        }))
    }
}

enum Databases {
    Sqlite(SqliteBackend),
}
struct SqliteBackend {
    interest_store: Arc<CeramicInterestService>,
    event_store: Arc<CeramicEventService>,
}

struct Daemon;

impl Daemon {
    async fn run(opts: DaemonOpts) -> Result<()> {
        let info = Info::new().await?;

        let mut metrics_config = MetricsConfig {
            export: opts.metrics,
            tracing: opts.tracing,
            log_format: opts.log_opts.format(),
            #[cfg(feature = "tokio-console")]
            tokio_console: opts.tokio_console,
            ..Default::default()
        };
        info.apply_to_metrics_config(&mut metrics_config);

        // Currently only an info metric is recorded so we do not need to keep the handle to the
        // Metrics struct. That will change once we add more metrics.
        let _metrics = ceramic_metrics::MetricsHandle::register(|registry| {
            crate::metrics::Metrics::register(info.clone(), registry)
        });

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
        let db = opts.db_opts.get_database(true).await?;

        // we should be able to consolidate the Store traits now that they all rely on &self, but for now we use
        // static dispatch and require compile-time type information, so we pass all the types we need in, even
        // though they are currently all implemented by a single struct and we're just cloning Arcs.
        match db {
            Databases::Sqlite(db) => {
                Daemon::run_internal(
                    opts,
                    db.interest_store.clone(),
                    db.interest_store,
                    db.event_store.clone(),
                    db.event_store.clone(),
                    db.event_store.clone(),
                    db.event_store,
                    metrics_handle,
                )
                .await
            }
        }
    }

    async fn run_internal<I1, I2, E1, E2, E3, E4>(
        opts: DaemonOpts,
        interest_api_store: Arc<I1>,
        interest_recon_store: Arc<I2>,
        model_api_store: Arc<E1>,
        model_recon_store: Arc<E2>,
        model_rpc_store: Arc<E3>,
        bitswap_block_store: Arc<E4>,
        metrics_handle: MetricsHandle,
    ) -> Result<()>
    where
        I1: InterestStore + Send + Sync + 'static,
        I2: recon::Store<Key = Interest, Hash = Sha256a> + Send + Sync + 'static,
        E1: EventStore + Send + Sync + 'static,
        E2: recon::Store<Key = EventId, Hash = Sha256a> + Send + Sync + 'static,
        E3: ceramic_rpc::EventStore + Send + Sync + 'static,
        E4: iroh_bitswap::Store + Send + Sync + 'static,
    {
        let network = opts.network.to_network(&opts.local_network_id)?;

        let store_dir = opts.db_opts.store_dir;
        debug!(dir = %store_dir.display(), "using store directory");
        debug!(dir = %opts.p2p_key_dir.display(), "using p2p key directory");

        // Setup tokio-metrics
        MetricsHandle::register(|registry| {
            let handle = tokio::runtime::Handle::current();
            let runtime_monitor = tokio_metrics::RuntimeMonitor::new(&handle);
            tokio_prometheus_client::register(
                runtime_monitor,
                registry.sub_registry_with_prefix("tokio"),
            );
        });

        let p2p_config = Libp2pConfig {
            mdns: false,
            bitswap_server: false,
            bitswap_client: false,
            kademlia: false,
            autonat: false,
            relay_server: false,
            relay_client: false,
            max_conns_out: opts.max_conns_out,
            max_conns_in: opts.max_conns_in,
            max_conns_pending_out: opts.max_conns_pending_out,
            max_conns_pending_in: opts.max_conns_pending_in,
            max_conns_per_peer: opts.max_conns_per_peer,
            idle_connection_timeout: Duration::from_millis(opts.idle_conns_timeout_ms),
            // Add extra ceramic peer addresses to the list of official ceramic peer addresses.
            ceramic_peers: opts
                .network
                .bootstrap_addresses()
                .into_iter()
                .chain(
                    opts.extra_ceramic_peer_addresses
                        .iter()
                        .map(|addr| addr.parse())
                        .collect::<Result<Vec<Multiaddr>, multiaddr::Error>>()?,
                )
                .collect(),
            external_multiaddrs: opts
                .external_swarm_addresses
                .iter()
                .map(|addr| addr.parse())
                .collect::<Result<Vec<Multiaddr>, multiaddr::Error>>()?,
            listening_multiaddrs: opts
                .swarm_addresses
                .iter()
                .map(|addr| addr.parse())
                .collect::<Result<Vec<Multiaddr>, multiaddr::Error>>()?,
            ..Default::default()
        };
        debug!(?p2p_config, "using p2p config");

        // Load p2p identity
        let mut kc = Keychain::<DiskStorage>::new(opts.p2p_key_dir.clone())
            .await
            .context(format!(
                "initializing p2p key: using p2p_key_dir={}",
                opts.p2p_key_dir.display()
            ))?;
        let keypair = load_identity(&mut kc).await?;
        let peer_id = keypair.public().to_peer_id();

        // Register metrics for all components
        let recon_metrics = MetricsHandle::register(recon::Metrics::register);
        let store_metrics = MetricsHandle::register(ceramic_store::Metrics::register);
        let http_metrics = Arc::new(ceramic_metrics::MetricsHandle::register(
            http_metrics::Metrics::register,
        ));

        // Create recon store for interests.
        let interest_store = ceramic_store::StoreMetricsMiddleware::new(
            interest_recon_store.clone(),
            store_metrics.clone(),
        );

        let interest_api_store =
            ceramic_store::StoreMetricsMiddleware::new(interest_api_store, store_metrics.clone());

        // Create second recon store for models.
        let model_store = ceramic_store::StoreMetricsMiddleware::new(
            model_recon_store.clone(),
            store_metrics.clone(),
        );

        let model_api_store =
            ceramic_store::StoreMetricsMiddleware::new(model_api_store, store_metrics);

        // Construct a recon implementation for interests.
        let mut recon_interest_svr = Server::new(Recon::new(
            interest_store.clone(),
            InterestInterest::default(),
            recon_metrics.clone(),
        ));

        // Construct a recon implementation for models.
        let mut recon_model_svr = Server::new(Recon::new(
            model_store.clone(),
            // Use recon interests as the InterestProvider for recon_model
            ModelInterest::new(peer_id, recon_interest_svr.client()),
            recon_metrics,
        ));

        let recons = Some((recon_interest_svr.client(), recon_model_svr.client()));
        let ipfs_metrics =
            ceramic_metrics::MetricsHandle::register(ceramic_kubo_rpc::IpfsMetrics::register);
        let p2p_metrics = MetricsHandle::register(ceramic_p2p::Metrics::register);
        let ipfs = Ipfs::<E3>::builder()
            .with_p2p(
                p2p_config,
                keypair,
                recons,
                bitswap_block_store.clone(),
                p2p_metrics,
            )
            .await?
            .build(bitswap_block_store, ipfs_metrics)
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to start libp2p server using addresses: {}. {}",
                    opts.swarm_addresses.join(", "),
                    e
                )
            })?;

        // Start metrics server
        debug!(
            bind_address = opts.metrics_bind_address,
            "starting prometheus metrics server"
        );
        let (tx_metrics_server_shutdown, metrics_server_handle) =
            metrics::start(&opts.metrics_bind_address.parse()?).map_err(|e| {
                anyhow!(
                    "Failed to start metrics server using address: {}. {}",
                    opts.metrics_bind_address,
                    e
                )
            })?;

        // Build HTTP server
        let ceramic_server = ceramic_api::Server::new(
            peer_id,
            network,
            interest_api_store,
            Arc::new(model_api_store),
        );
        let ceramic_service = ceramic_api_server::server::MakeService::new(ceramic_server);
        let ceramic_service = MakeAllowAllAuthenticator::new(ceramic_service, "");
        let ceramic_service =
            ceramic_api_server::context::MakeAddContext::<_, EmptyContext>::new(ceramic_service);

        let kubo_rpc_server = ceramic_kubo_rpc::http::Server::new(ipfs.api());
        let kubo_rpc_service = ceramic_kubo_rpc_server::server::MakeService::new(kubo_rpc_server);
        let kubo_rpc_service = MakeAllowAllAuthenticator::new(kubo_rpc_service, "");
        let kubo_rpc_service =
            ceramic_kubo_rpc_server::context::MakeAddContext::<_, EmptyContext>::new(
                kubo_rpc_service,
            );
        let kubo_rpc_service =
            http::MakeMetricsService::new(kubo_rpc_service, http_metrics.clone());
        let kubo_rpc_service =
            http::MakeCorsService::new(kubo_rpc_service, opts.cors_allow_origins.clone());
        let ceramic_service = http::MakeCorsService::new(ceramic_service, opts.cors_allow_origins);
        let ceramic_service = http::MakeMetricsService::new(ceramic_service, http_metrics);

        // Compose both services
        let service = http::MakePrefixService::new(
            ("/ceramic/".to_string(), ceramic_service),
            ("/api/v0/".to_string(), kubo_rpc_service),
        );

        let recon_interest_handle = tokio::spawn(recon_interest_svr.run());
        let recon_model_handle = tokio::spawn(recon_model_svr.run());

        debug!("staring rpc server");
        tokio::spawn(async move {
            ceramic_rpc::run(model_rpc_store)
                .await
                .expect("should be able to run rpc server")
        });

        // Start HTTP server with a graceful shutdown
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let signals = Signals::new([SIGHUP, SIGTERM, SIGINT, SIGQUIT])?;
        let handle = signals.handle();

        debug!("starting signal handler task");
        let signals_handle = tokio::spawn(handle_signals(signals, tx));

        // The server task blocks until we are ready to start shutdown
        info!("starting api server at address {}", opts.bind_address);
        hyper::server::Server::try_bind(&opts.bind_address.parse()?)
            .map_err(|e| anyhow!("Failed to bind address: {}. {}", opts.bind_address, e))?
            .serve(service)
            .with_graceful_shutdown(async {
                rx.await.ok();
            })
            .await?;
        debug!("api server finished, starting shutdown...");

        // Stop IPFS.
        if let Err(err) = ipfs.stop().await {
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
        metrics_handle.shutdown();
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
        cfg.service_name.clone_from(&self.service_name);
        cfg.version.clone_from(&self.version);
        cfg.build.clone_from(&self.build);
        cfg.instance_id.clone_from(&self.instance_id);
    }
}

async fn current_exe_hash() -> Result<Multihash<32>> {
    if cfg!(debug_assertions) {
        // Debug builds can be 1GB+, so do we not want to spend the time to hash them.
        // Return a fake hash.
        Ok(Multihash::<32>::wrap(
            // Identity hash code
            0,
            // Spells debug when base64 url encoded with some leading padding.
            &[00, 117, 230, 238, 130],
        )
        .expect("hardcoded digest should fit in 32 bytes"))
    } else {
        let exe_path = env::current_exe()?;
        let mut hasher = multihash_codetable::Sha2_256::default();
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
        Ok(Multihash::<32>::wrap(Code::Sha2_256.into(), hash)?)
    }
}
