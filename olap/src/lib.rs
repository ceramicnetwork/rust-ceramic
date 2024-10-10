//! OLAP Aggregator process for aggregating Ceramic Model Instance Document streams.
#![warn(missing_docs)]

mod aggregator;
mod metrics;

use anyhow::{anyhow, Result};
use ceramic_metrics::config::Config as MetricsConfig;
use clap::{Args, Parser, Subcommand, ValueEnum};
use futures::StreamExt as _;
use multihash::Multihash;
use multihash_codetable::Code;
use multihash_derive::Hasher as _;
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;
use tokio::{io::AsyncReadExt as _, sync::oneshot};
use tracing::{debug, info, warn};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run a daemon process
    Daemon(Box<DaemonOpts>),
}

#[derive(Args, Debug)]
struct DaemonOpts {
    /// Endpoint of a Flight SQL server for the conclusion feed.
    #[arg(
        short,
        long,
        default_value = "http://127.0.0.1:5102",
        env = "CERAMIC_OLAP_FLIGHT_SQL_ENDPOINT"
    )]
    flight_sql_endpoint: String,

    /// Bind address of the metrics endpoint.
    #[arg(
        short,
        long,
        default_value = "127.0.0.1:9465",
        env = "CERAMIC_OLAP_METRICS_BIND_ADDRESS"
    )]
    metrics_bind_address: String,

    /// When true metrics will be exported
    #[arg(long, default_value_t = false, env = "CERAMIC_OLAP_METRICS")]
    metrics: bool,

    /// When true traces will be exported
    #[arg(long, default_value_t = false, env = "CERAMIC_OLAP_TRACING")]
    tracing: bool,

    /// AWS S3 bucket name.
    /// When configured the aggregator will support storing data in S3 compatible object stores.
    ///
    /// Credentials are read from the environment:
    ///
    ///   * AWS_ACCESS_KEY_ID -> access_key_id
    ///   * AWS_SECRET_ACCESS_KEY -> secret_access_key
    ///   * AWS_DEFAULT_REGION -> region
    ///   * AWS_ENDPOINT -> endpoint
    ///   * AWS_SESSION_TOKEN -> token
    ///   * AWS_ALLOW_HTTP -> set to "true" to permit HTTP connections without TLS
    ///
    #[arg(long, env = "CERAMIC_OLAP_AWS_BUCKET")]
    aws_bucket: String,

    #[command(flatten)]
    log_opts: LogOpts,
}

#[derive(Args, Debug)]
struct LogOpts {
    /// Specify the format of log events.
    #[arg(long, default_value = "multi-line", env = "CERAMIC_OLAP_LOG_FORMAT")]
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

impl From<&DaemonOpts> for aggregator::Config {
    fn from(value: &DaemonOpts) -> Self {
        Self {
            flight_sql_endpoint: value.flight_sql_endpoint.clone(),
            aws_s3_bucket: value.aws_bucket.clone(),
        }
    }
}

/// Run the ceramic one binary process
pub async fn run() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        Command::Daemon(opts) => daemon(*opts).await,
    }
}

async fn daemon(opts: DaemonOpts) -> Result<()> {
    let info = Info::new().await?;

    let mut metrics_config = MetricsConfig {
        export: opts.metrics,
        tracing: opts.tracing,
        log_format: opts.log_opts.format(),
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

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let signals = Signals::new([SIGHUP, SIGTERM, SIGINT, SIGQUIT])?;
    let handle = signals.handle();
    debug!("starting signal handler task");
    let signals_handle = tokio::spawn(handle_signals(signals, tx));

    // Start aggregator
    aggregator::run(&opts, async move {
        let _ = rx.await;
    })
    .await?;

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
        let exe_hash = multibase::encode(
            multibase::Base::Base64Url,
            current_exe_hash().await?.to_bytes(),
        );
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
        let exe_path = std::env::current_exe()?;
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
