//! Ceramic implements a single binary ceramic node.
#![warn(missing_docs)]

mod daemon;
mod http;
mod http_metrics;
mod metrics;
mod migrations;
mod network;
mod query;

use anyhow::{anyhow, bail, Result};
use ceramic_core::ssi::caip2::ChainId;
use ceramic_event_svc::{eth_rpc::HttpEthRpc, ChainInclusionProvider};
use ceramic_metrics::config::Config as MetricsConfig;
use ceramic_sql::sqlite::{SqliteOpts, SqlitePool};
use clap::{Args, Parser, Subcommand, ValueEnum};
use futures::StreamExt;
use libp2p::Multiaddr;
use multibase::Base;
use multihash::Multihash;
use multihash_codetable::Code;
use multihash_derive::Hasher;
use shutdown::Shutdown;
use signal_hook_tokio::Signals;
use std::{env, path::PathBuf, str::FromStr, sync::Arc};
use tokio::io::AsyncReadExt;
use tracing::{debug, error, info, warn};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run a daemon process
    Daemon(Box<daemon::DaemonOpts>),
    /// Perform various migrations
    #[command(subcommand)]
    Migrations(migrations::EventsCommand),
    /// Run an interactive SQL REPL to inspect local data.
    Query(query::QueryOpts),
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
/// Value for Sqlite `pragma temp_store`
enum SqliteTempStore {
    /// Default database was compiled with
    Default,
    /// Use the filesystem
    File,
    /// Temporary tables and indices are kept as if they were in pure in-memory databases
    Memory,
}

impl From<SqliteTempStore> for ceramic_sql::sqlite::SqliteTempStore {
    fn from(value: SqliteTempStore) -> Self {
        match value {
            SqliteTempStore::Default => Self::Default,
            SqliteTempStore::File => Self::File,
            SqliteTempStore::Memory => Self::Memory,
        }
    }
}

#[derive(ValueEnum, Debug, Clone, PartialEq, Eq)]
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

    /// Return the default ethereum rpc providers for each network.
    fn default_rpc_urls(&self) -> Result<Vec<String>> {
        match self {
            Network::Mainnet => {
                anyhow::bail!("no Ethereum RPC URLs specified for Mainnet")
            }
            Network::TestnetClay => {
                info!("no Ethereum RPC URLs specified for Clay Testnet, defaulting to https://gnosis-mainnet.public.blastapi.io");
                Ok(vec!["https://gnosis-mainnet.public.blastapi.io".to_string()])
            }
            Network::DevUnstable => {
                info!("no Ethereum RPC URLs specified for dev-unstable network, defaulting to https://ethereum-sepolia-rpc.publicnode.com");
                Ok(vec![
                    "https://ethereum-sepolia-rpc.publicnode.com".to_string()
                ])
            }
            Network::Local => {
                info!(
                "using default Ganache Ethereum RPC URL for Local network: http://localhost:7545"
            );
                // Default Ganache port
                Ok(vec!["http://localhost:8545".to_string()])
            }
            Network::InMemory => {
                info!("no Ethereum RPC URLs specified");
                Ok(vec![])
            }
        }
    }

    /// return the allowed chain ids for this network. or None for any
    fn supported_chain_ids(&self) -> Option<Vec<ChainId>> {
        match self {
            Network::Mainnet => Some(vec![
                ChainId::from_str("eip155:1").expect("eip155:1 is a valid chain")
            ]), // Ethereum mainnet
            Network::TestnetClay => Some(vec![
                ChainId::from_str("eip155:100").expect("eip155:100 is a valid chain")
            ]), // Gnosis
            Network::DevUnstable => Some(vec![
                ChainId::from_str("eip155:11155111").expect("eip155:11155111 is a valid chain")
            ]), // Sepolia
            Network::Local => None,
            Network::InMemory => None,
        }
    }

    /// Get the network as a unique name.
    fn name(&self) -> String {
        match self {
            Network::Mainnet => "mainnet".to_owned(),
            Network::TestnetClay => "testnet-clay".to_owned(),
            Network::DevUnstable => "dev-unstable".to_owned(),
            Network::Local => "local".to_owned(),
            Network::InMemory => "inmemory".to_owned(),
        }
    }

    pub async fn get_eth_rpc_providers(
        &self,
        ethereum_rpc_urls: Vec<String>,
    ) -> Result<Vec<ChainInclusionProvider>> {
        let ethereum_rpc_urls = if ethereum_rpc_urls.is_empty() {
            self.default_rpc_urls()?
        } else {
            ethereum_rpc_urls
        };

        let mut providers = Vec::new();
        for url in ethereum_rpc_urls {
            match HttpEthRpc::try_new(&url).await {
                Ok(provider) => {
                    let provider_chain = provider.chain_id();
                    if self
                        .supported_chain_ids()
                        .is_none_or(|ids| ids.contains(provider_chain))
                    {
                        info!(
                            "Using ethereum rpc provider for chain: {} with url: {}",
                            provider.chain_id(),
                            provider.url()
                        );
                        let provider: ChainInclusionProvider = Arc::new(provider);
                        providers.push(provider);
                    } else {
                        warn!("Eth RPC provider {} uses chainid {} which isn't supported by Ceramic network {:?}", url, provider_chain, self);
                    }
                }
                Err(err) => {
                    warn!("failed to create RPC client with url: '{url}': {:?}", err);
                }
            }
        }

        if providers.is_empty() {
            if *self == Network::Local || *self == Network::InMemory {
                warn!("No usable ethereum RPC provided for network {}. All TimeEvent validation will fail", self.name());
            } else {
                bail!(
                    "No usable ethereum RPC configured for network {}",
                    self.name()
                );
            }
        }

        Ok(providers)
    }
}

/// The default storage directory to use if none is provided. In order:
///     - `$HOME/.ceramic-one`
///     -  `./.ceramic-one`
fn default_directory() -> PathBuf {
    home::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".ceramic-one")
}

// Shared options for configuring where data is stored.
#[derive(Args, Debug)]
struct DBOpts {
    /// Path to storage directory.
    #[arg(short, long, default_value=default_directory().into_os_string(), env = "CERAMIC_ONE_STORE_DIR")]
    store_dir: PathBuf,
}

#[derive(Args, Debug)]
/// Experimental options related to tuning database performance
struct DBOptsExperimental {
    #[arg(
        long,
        env = "CERAMIC_ONE_DB_CACHE_SIZE",
        requires = "experimental_features"
    )]
    /// Value to use for the sqlite cache_size pragma
    /// Use the negative version, which represents Kib e.g. -20000 = 20 Mb
    /// Or the postive version, representing pages
    /// None means the default is used.
    /// Requires using the experimental-features flag
    db_cache_size: Option<i64>,
    /// Used for pragma mmap_size
    /// 10737418240: 10 GB of memory mapped IO
    /// if this is slightly bigger than your db file it can improve read performance
    /// Set to 0 to disable. None is the default
    /// Requires using the experimental-features flag
    #[arg(
        long,
        env = "CERAMIC_ONE_DB_MMAP_SIZE",
        requires = "experimental_features"
    )]
    db_mmap_size: Option<u64>,
    /// The maximum number of read only connections in the pool
    /// Requires using the experimental-features flag
    #[arg(
        long,
        default_value = "8",
        env = "CERAMIC_ONE_DB_MAX_CONNECTIONS",
        requires = "experimental_features"
    )]
    db_max_connections: u32,
    /// The sqlite temp_store value to use
    /// Requires using the experimental-features flag
    #[arg(
        long,
        env = "CERAMIC_ONE_DB_TEMP_STORE",
        requires = "experimental_features"
    )]
    db_temp_store: Option<SqliteTempStore>,
    /// The sqlite analysis_limit to use for optimize.
    /// Values between 100 and 1000 are recommended, with lower values doing less work.
    /// Or, to disable the analysis limit, causing ANALYZE to do a complete scan of each index, set the analysis limit to 0.
    /// This MAY take extemely long (minutes to hours) on very large databases.
    /// Requires using the experimental-features flag
    #[arg(
        long,
        default_value = "1000",
        env = "CERAMIC_ONE_DB_ANALYSIS_LIMIT",
        requires = "experimental_features"
    )]
    db_analysis_limit: u32,

    /// Whether or not `pragma optimize` should be run.
    /// If it's set, optimize is run immediately and daily in the background.
    /// Will use the `analysis_limit` to control how much work is done.
    /// It shouldn't, but it's possible this may take a long time on very large databases.
    /// Requires using the experimental-features flag
    #[arg(
        long,
        default_value = "true",
        env = "CERAMIC_ONE_DB_OPTIMIZE",
        requires = "experimental_features"
    )]
    db_optimize: bool,
}

impl From<DBOptsExperimental> for SqliteOpts {
    fn from(value: DBOptsExperimental) -> Self {
        Self {
            cache_size: value.db_cache_size,
            mmap_size: value.db_mmap_size,
            max_ro_connections: value.db_max_connections,
            temp_store: value.db_temp_store.map(|v| v.into()),
            analysis_limit: value.db_analysis_limit,
            optimize: value.db_optimize,
        }
    }
}

// Shared options for how logging is configured.
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

/// Run the ceramic one binary process
pub async fn run() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        Command::Daemon(opts) => daemon::run(*opts).await,
        Command::Migrations(opts) => migrations::migrate(opts).await,
        Command::Query(opts) => query::run(opts).await,
    }
}

impl DBOpts {
    /// This function will create the database directory if it does not exist.
    async fn get_sqlite_pool(&self, opts: SqliteOpts) -> Result<SqlitePool> {
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
        Ok(ceramic_sql::sqlite::SqlitePool::connect(
            &sql_db_path,
            opts,
            ceramic_sql::sqlite::Migrations::Apply,
        )
        .await?)
    }
}

async fn handle_signals(mut signals: Signals, shutdown: Shutdown) {
    let mut shutdown = Some(shutdown);
    while let Some(signal) = signals.next().await {
        debug!(?signal, "signal received");
        if let Some(shutdown) = shutdown.take() {
            info!("sending shutdown message");
            shutdown.shutdown();
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
