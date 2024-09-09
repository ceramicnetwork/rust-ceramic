use std::{path::PathBuf, time::Duration};

use anyhow::{anyhow, Context, Result};
use ceramic_event_svc::EventService;
use ceramic_interest_svc::InterestService;
use ceramic_kubo_rpc::Multiaddr;
use ceramic_metrics::{config::Config as MetricsConfig, MetricsHandle};
use ceramic_p2p::{load_identity, DiskStorage, Keychain, Libp2pConfig};
use clap::Args;
use recon::{FullInterests, Recon, ReconInterestProvider};
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;
use std::sync::Arc;
use swagger::{auth::MakeAllowAllAuthenticator, EmptyContext};
use tracing::{debug, info, warn};

use crate::{
    default_directory, feature_flags::*, handle_signals, http, http_metrics, metrics,
    network::Ipfs, DBOpts, Info, LogOpts, Network,
};

#[derive(Args, Debug)]
pub struct DaemonOpts {
    #[command(flatten)]
    db_opts: DBOpts,

    /// Path to libp2p private key directory
    #[arg(short, long, default_value=default_directory().into_os_string(), env = "CERAMIC_ONE_P2P_KEY_DIR")]
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

    /// Enable experimental feature flags
    #[arg(
        long,
        use_value_delimiter = true,
        value_delimiter = ',',
        env = "CERAMIC_ONE_EXPERIMENTAL_FEATURE_FLAGS"
    )]
    experimental_feature_flags: Vec<ExperimentalFeatureFlags>,

    /// Enable feature flags
    #[arg(
        long,
        use_value_delimiter = true,
        value_delimiter = ',',
        env = "CERAMIC_ONE_FEATURE_FLAGS"
    )]
    feature_flags: Vec<FeatureFlags>,
}

// Start the daemon process
pub async fn run(opts: DaemonOpts) -> Result<()> {
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

    let store_dir = &opts.db_opts.store_dir;
    debug!(dir = %store_dir.display(), "using store directory");
    debug!(dir = %opts.p2p_key_dir.display(), "using p2p key directory");

    // Construct sqlite_pool
    let sqlite_pool = opts.db_opts.get_sqlite_pool().await?;

    // Construct services from pool
    let interest_svc = Arc::new(InterestService::new(sqlite_pool.clone()));
    let event_svc = Arc::new(
        EventService::try_new(
            sqlite_pool.clone(),
            true,
            opts.experimental_feature_flags
                .contains(&ExperimentalFeatureFlags::EventValidation),
        )
        .await?,
    );

    let network = opts.network.to_network(&opts.local_network_id)?;

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
    let interest_svc_store_metrics =
        MetricsHandle::register(ceramic_interest_svc::store::Metrics::register);
    let event_svc_store_metrics =
        MetricsHandle::register(ceramic_event_svc::store::Metrics::register);
    let http_metrics = Arc::new(ceramic_metrics::MetricsHandle::register(
        http_metrics::Metrics::register,
    ));

    // Create recon store for interests.
    let interest_store = ceramic_interest_svc::store::StoreMetricsMiddleware::new(
        interest_svc.clone(),
        interest_svc_store_metrics.clone(),
    );

    let interest_api_store = ceramic_interest_svc::store::StoreMetricsMiddleware::new(
        interest_svc.clone(),
        interest_svc_store_metrics.clone(),
    );

    // Create second recon store for models.
    let model_store = ceramic_event_svc::store::StoreMetricsMiddleware::new(
        event_svc.clone(),
        event_svc_store_metrics.clone(),
    );

    let model_api_store = ceramic_event_svc::store::StoreMetricsMiddleware::new(
        event_svc.clone(),
        event_svc_store_metrics,
    );

    // Construct a recon implementation for interests.
    let recon_interest_svr = Recon::new(
        interest_store.clone(),
        FullInterests::default(),
        recon_metrics.clone(),
    );

    // Construct a recon implementation for models.
    let recon_model_svr = Recon::new(
        model_store.clone(),
        // Use recon interests as the InterestProvider for recon_model
        ReconInterestProvider::new(peer_id, interest_store.clone()),
        recon_metrics,
    );

    let recons = Some((recon_interest_svr, recon_model_svr));
    let ipfs_metrics =
        ceramic_metrics::MetricsHandle::register(ceramic_kubo_rpc::IpfsMetrics::register);
    let p2p_metrics = MetricsHandle::register(ceramic_p2p::Metrics::register);
    let ipfs = Ipfs::<EventService>::builder()
        .with_p2p(p2p_config, keypair, recons, event_svc.clone(), p2p_metrics)
        .await?
        .build(event_svc.clone(), ipfs_metrics)
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
    let mut ceramic_server = ceramic_api::Server::new(
        peer_id,
        network,
        interest_api_store,
        Arc::new(model_api_store),
    );
    if opts
        .experimental_feature_flags
        .contains(&ExperimentalFeatureFlags::Authentication)
    {
        ceramic_server.with_authentication(true);
    }
    let ceramic_service = ceramic_api_server::server::MakeService::new(ceramic_server);
    let ceramic_service = MakeAllowAllAuthenticator::new(ceramic_service, "");
    let ceramic_service =
        ceramic_api_server::context::MakeAddContext::<_, EmptyContext>::new(ceramic_service);

    let kubo_rpc_server = ceramic_kubo_rpc::http::Server::new(ipfs.api());
    let kubo_rpc_service = ceramic_kubo_rpc_server::server::MakeService::new(kubo_rpc_server);
    let kubo_rpc_service = MakeAllowAllAuthenticator::new(kubo_rpc_service, "");
    let kubo_rpc_service =
        ceramic_kubo_rpc_server::context::MakeAddContext::<_, EmptyContext>::new(kubo_rpc_service);
    let kubo_rpc_service = http::MakeMetricsService::new(kubo_rpc_service, http_metrics.clone());
    let kubo_rpc_service =
        http::MakeCorsService::new(kubo_rpc_service, opts.cors_allow_origins.clone());
    let ceramic_service = http::MakeCorsService::new(ceramic_service, opts.cors_allow_origins);
    let ceramic_service = http::MakeMetricsService::new(ceramic_service, http_metrics);

    // Compose both services
    let service = http::MakePrefixService::new(
        ("/ceramic/".to_string(), ceramic_service),
        ("/api/v0/".to_string(), kubo_rpc_service),
    );

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