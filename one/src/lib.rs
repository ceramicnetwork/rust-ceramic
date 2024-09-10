//! Ceramic implements a single binary ceramic node.
#![warn(missing_docs)]

mod daemon;
mod http;
mod http_metrics;
mod metrics;
mod migrations;
mod network;

use std::{env, path::PathBuf};

use anyhow::{anyhow, Result};
use ceramic_metrics::config::Config as MetricsConfig;
use ceramic_sql::sqlite::SqlitePool;
use clap::{Args, Parser, Subcommand, ValueEnum};
use futures::StreamExt;
use libp2p::Multiaddr;
use multibase::Base;
use multihash::Multihash;
use multihash_codetable::Code;
use multihash_derive::Hasher;
use signal_hook_tokio::Signals;
use tokio::{io::AsyncReadExt, sync::oneshot};
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
    }
}

impl DBOpts {
    /// This function will create the database directory if it does not exist.
    async fn get_sqlite_pool(&self) -> Result<SqlitePool> {
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
            ceramic_sql::sqlite::Migrations::Apply,
        )
        .await?)
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
