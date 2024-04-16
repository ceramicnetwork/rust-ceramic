//! Ceramic implements a single binary ceramic node.
#![warn(missing_docs)]

mod cbor_value;
mod ethereum_rpc;
mod events;
mod http;
mod metrics;
mod network;

use std::{env, num::NonZeroUsize, path::PathBuf, time::Duration};

use anyhow::{anyhow, Result};
use ceramic_api::{AccessInterestStore, AccessModelStore};
use ceramic_core::{EventId, Interest};
use ceramic_kubo_rpc::Multiaddr;

use ceramic_metrics::{config::Config as MetricsConfig, MetricsHandle};
use ceramic_p2p::{load_identity, DiskStorage, Keychain, Libp2pConfig};
use clap::{Args, Parser, Subcommand, ValueEnum};
use futures::StreamExt;
use multibase::Base;
use multihash::{Code, Hasher, Multihash, MultihashDigest};
use recon::{FullInterests, Recon, ReconInterestProvider, Server, Sha256a};
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;
use std::sync::Arc;
use swagger::{auth::MakeAllowAllAuthenticator, EmptyContext};
use tokio::{io::AsyncReadExt, sync::oneshot};
use tracing::{debug, info, warn};

use crate::network::Ipfs;
pub use cbor_value::CborValue;
use ceramic_store::{
    EventStorePostgres, EventStoreSqlite, InterestStorePostgres, InterestStoreSqlite,
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
    /// Event store tools
    #[command(subcommand)]
    Events(events::EventsCommand),
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

    /// Extra addresses of peers that participate in the Ceramic network.
    /// A best-effort attempt will be made to maintain a connection to these addresses.
    #[arg(
        long,
        use_value_delimiter = true,
        value_delimiter = ',',
        env = "CERAMIC_ONE_EXTRA_CERAMIC_PEER_ADDRESSES"
    )]
    extra_ceramic_peer_addresses: Vec<String>,

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

    /// When set mdns will be used to discover peers.
    #[arg(long, default_value_t = false, env = "CERAMIC_ONE_MDNS")]
    mdns: bool,

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
    /// The database to connect to e.g.
    ///
    /// `sqlite:///path/to/file.sqlite3` or `sqlite://:memory:` or `sqlite://~/.ceramic-one/db.sqlite3`
    /// `postgres://user:password@host:port/dbname`
    ///
    /// The default is to use `db.sqlite3` in the store directory.
    #[arg(long, env = "CERAMIC_ONE_DATABASE_URL")]
    database_url: Option<String>,
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
                "/dns/rust-ceramic-v4-tnet-0.3box.io/tcp/4001/p2p/12D3KooWNYomhBwgoCZ5sbhkCfEmmHV8m3bvESm6PL1bjDU7H3ja/p2p/12D3KooWNYomhBwgoCZ5sbhkCfEmmHV8m3bvESm6PL1bjDU7H3ja".to_string(),// cspell:disable-line
                "/dns/rust-ceramic-v4-tnet-1.3box.io/tcp/4001/p2p/12D3KooWPYNbc6VBfPuJQ84sNb4xNXysD383ZXZuLYbG8xTmHmfj/p2p/12D3KooWPYNbc6VBfPuJQ84sNb4xNXysD383ZXZuLYbG8xTmHmfj".to_string(), // cspell:disable-line
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

/// Run the ceramic one binary process
pub async fn run() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        Command::Daemon(opts) => Daemon::run(opts).await,
        Command::Events(opts) => events::events(opts).await,
    }
}

type InterestInterest = FullInterests<Interest>;
type ModelInterest = ReconInterestProvider<Sha256a>;

impl DaemonOpts {
    fn default_directory(&self) -> PathBuf {
        // 1 path from options
        // 2 path $HOME/.ceramic-one
        // 3 pwd/.ceramic-one
        match self.store_dir.clone() {
            Some(dir) => dir,
            None => match home::home_dir() {
                Some(home_dir) => home_dir.join(".ceramic-one"),
                None => PathBuf::from(".ceramic-one"),
            },
        }
    }
    async fn get_database(&self) -> Result<Databases> {
        // this is called before tracing is initialized so we use stdout/stderr
        match self.database_url.as_ref() {
            Some(url) if url.starts_with("sqlite://") => Self::build_sqlite_dbs(url).await,
            Some(url) if url.starts_with("postgres://") => {
                let sql_pool =
                    ceramic_store::PostgresPool::connect(url, ceramic_store::Migrations::Apply)
                        .await?;
                let interest_store = Arc::new(InterestStorePostgres::new(sql_pool.clone()).await?);
                let event_store = Arc::new(EventStorePostgres::new(sql_pool.clone()).await?);

                println!("Connected to postgres database");
                Ok(Databases::Postgres(PgBackend {
                    event_store,
                    interest_store,
                }))
            }
            Some(unknown) => {
                let message = format!("Database URL is not supported: {}", unknown);
                eprintln!("{}", message);
                anyhow::bail!(message)
            }
            None => {
                let sql_db_path = self
                    .default_directory()
                    .join("db.sqlite3")
                    .display()
                    .to_string();
                Self::build_sqlite_dbs(&sql_db_path).await
            }
        }
    }

    async fn build_sqlite_dbs(path: &str) -> Result<Databases> {
        let sql_pool =
            ceramic_store::SqlitePool::connect(path, ceramic_store::Migrations::Apply).await?;
        let interest_store = Arc::new(InterestStoreSqlite::new(sql_pool.clone()).await?);
        let event_store = Arc::new(EventStoreSqlite::new(sql_pool.clone()).await?);
        println!("Connected to sqlite database: {}", path);

        Ok(Databases::Sqlite(SqliteBackend {
            event_store,
            interest_store,
        }))
    }
}

enum Databases {
    Postgres(PgBackend),
    Sqlite(SqliteBackend),
}
struct SqliteBackend {
    interest_store: Arc<InterestStoreSqlite>,
    event_store: Arc<EventStoreSqlite>,
}

struct PgBackend {
    interest_store: Arc<InterestStorePostgres>,
    event_store: Arc<EventStorePostgres>,
}

struct Daemon;

impl Daemon {
    async fn run(opts: DaemonOpts) -> Result<()> {
        let db = opts.get_database().await?;

        // we should be able to consolidate the Store traits now that they all rely on &self, but for now we use
        // static dispatch and require compile-time type information, so we pass all the types we need in, even
        // though they are currently all implemented by a single struct and we're just cloning Arcs.
        match db {
            Databases::Postgres(db) => {
                Daemon::run_int(
                    opts,
                    db.interest_store.clone(),
                    db.interest_store,
                    db.event_store.clone(),
                    db.event_store.clone(),
                    db.event_store,
                )
                .await
            }
            Databases::Sqlite(db) => {
                Daemon::run_int(
                    opts,
                    db.interest_store.clone(),
                    db.interest_store,
                    db.event_store.clone(),
                    db.event_store.clone(),
                    db.event_store,
                )
                .await
            }
        }
    }

    async fn run_int<I1, I2, E1, E2, E3>(
        opts: DaemonOpts,
        interest_api_store: Arc<I1>,
        interest_recon_store: Arc<I2>,
        model_api_store: Arc<E1>,
        model_recon_store: Arc<E2>,
        bitswap_block_store: Arc<E3>,
    ) -> Result<()>
    where
        I1: AccessInterestStore + Send + Sync + 'static,
        I2: recon::Store<Key = Interest, Hash = Sha256a> + Send + Sync + 'static,
        E1: AccessModelStore + Send + Sync + 'static,
        E2: recon::Store<Key = EventId, Hash = Sha256a> + Send + Sync + 'static,
        E3: iroh_bitswap::Store + Send + Sync + 'static,
    {
        let network = opts.network.to_network(&opts.local_network_id)?;

        let info = Info::new().await?;

        let mut metrics_config = MetricsConfig {
            export: opts.metrics,
            tracing: opts.tracing,
            log_format: match opts.log_format {
                LogFormat::SingleLine => ceramic_metrics::config::LogFormat::SingleLine,
                LogFormat::MultiLine => ceramic_metrics::config::LogFormat::MultiLine,
                LogFormat::Json => ceramic_metrics::config::LogFormat::Json,
            },
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

        let dir = opts.default_directory();
        debug!("using directory: {}", dir.display());

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
            mdns: opts.mdns,
            bitswap_server: true,
            bitswap_client: true,
            kademlia: true,
            autonat: !opts.disable_autonat,
            relay_server: true,
            relay_client: true,
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
        let mut kc = Keychain::<DiskStorage>::new(dir).await?;
        let keypair = load_identity(&mut kc).await?;
        let peer_id = keypair.public().to_peer_id();

        // Create recon metrics
        let recon_metrics = MetricsHandle::register(recon::Metrics::register);
        let store_metrics = MetricsHandle::register(ceramic_store::Metrics::register);

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
            .await?;

        // Start metrics server
        debug!(
            bind_address = opts.metrics_bind_address,
            "starting prometheus metrics server"
        );
        let (tx_metrics_server_shutdown, metrics_server_handle) =
            metrics::start(&opts.metrics_bind_address.parse()?);

        // Build HTTP server
        let network = network.clone();
        let ceramic_server =
            ceramic_api::Server::new(peer_id, network, interest_api_store, model_api_store);
        let ceramic_metrics = MetricsHandle::register(ceramic_api::Metrics::register);
        // Wrap server in metrics middleware
        let ceramic_server = ceramic_api::MetricsMiddleware::new(ceramic_server, ceramic_metrics);
        let ceramic_service = ceramic_api_server::server::MakeService::new(ceramic_server);
        let ceramic_service = MakeAllowAllAuthenticator::new(ceramic_service, "");
        let ceramic_service =
            ceramic_api_server::context::MakeAddContext::<_, EmptyContext>::new(ceramic_service);

        let kubo_rpc_server = ceramic_kubo_rpc::http::Server::new(ipfs.api());
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

        let recon_interest_handle = tokio::spawn(recon_interest_svr.run());
        let recon_model_handle = tokio::spawn(recon_model_svr.run());

        // Start HTTP server with a graceful shutdown
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let signals = Signals::new([SIGHUP, SIGTERM, SIGINT, SIGQUIT])?;
        let handle = signals.handle();

        debug!("starting signal handler task");
        let signals_handle = tokio::spawn(handle_signals(signals, tx));

        // The server task blocks until we are ready to start shutdown
        debug!("starting api server");
        hyper::server::Server::bind(&opts.bind_address.parse()?)
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
        // Spells debug when base64 url encoded with some leading padding.
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
