#[macro_use]
mod macros;
#[cfg(feature = "bitswap")]
pub mod bitswap;
pub mod config;
pub mod core;
#[cfg(feature = "p2p")]
pub mod p2p;
#[cfg(feature = "store")]
pub mod store;

#[macro_use]
extern crate lazy_static;

use crate::config::Config;
use crate::core::HistogramType;
use crate::core::MetricType;
#[cfg(any(
    feature = "bitswap",
    feature = "gateway",
    feature = "resolver",
    feature = "store",
    feature = "p2p"
))]
#[allow(unused_imports)]
use crate::core::MetricsRecorder;
use crate::core::CORE;
use opentelemetry::{
    global,
    sdk::{propagation::TraceContextPropagator, trace, Resource},
    trace::{TraceContextExt, TraceId},
};
use opentelemetry_otlp::WithExportConfig;
use prometheus_client::registry::Registry;
use std::env::consts::{ARCH, OS};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, metadata::LevelFilter, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{
    fmt::{
        self,
        format::{Compact, Json, Pretty},
        time::SystemTime,
        FormatEvent,
    },
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter, Layer,
};

#[derive(Debug)]
pub struct MetricsHandle {
    metrics_task: Option<JoinHandle<()>>,
}

impl MetricsHandle {
    /// Shutdown the tracing and metrics subsystems.
    pub fn shutdown(&self) {
        opentelemetry::global::shutdown_tracer_provider();
        if let Some(mt) = &self.metrics_task {
            mt.abort();
        }
    }

    /// Initialize the tracing and metrics subsystems.
    pub async fn new(cfg: Config) -> Result<Self, Box<dyn std::error::Error>> {
        init_tracer(cfg.clone())?;
        let metrics_task = init_metrics(cfg).await;
        Ok(MetricsHandle { metrics_task })
    }

    pub fn register<T, F>(f: F) -> T
    where
        F: FnOnce(&mut Registry) -> T,
    {
        CORE.register(f)
    }

    pub fn encode() -> Vec<u8> {
        CORE.encode()
    }
}

/// Initialize the metrics subsystem.
async fn init_metrics(cfg: Config) -> Option<JoinHandle<()>> {
    if cfg.collect {
        CORE.set_enabled(true);
        if cfg.export {
            let prom_gateway_uri = format!(
                "{}/metrics/job/{}/instance/{}",
                cfg.prom_gateway_endpoint, cfg.service_name, cfg.instance_id
            );
            let push_client = reqwest::Client::new();
            return Some(tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let buff = CORE.encode();
                    let res = match push_client.post(&prom_gateway_uri).body(buff).send().await {
                        Ok(res) => res,
                        Err(e) => {
                            warn!("failed to push metrics: {}", e);
                            continue;
                        }
                    };
                    match res.status() {
                        reqwest::StatusCode::OK => {
                            debug!("pushed metrics to gateway");
                        }
                        _ => {
                            warn!("failed to push metrics to gateway: {:?}", res);
                            let body = res.text().await.unwrap();
                            warn!("error body: {}", body);
                        }
                    }
                }
            }));
        }
    }
    None
}

struct Format {
    kind: config::LogFormat,
    single: tracing_subscriber::fmt::format::Format<Compact, SystemTime>,
    multi: tracing_subscriber::fmt::format::Format<Pretty, SystemTime>,
    json: tracing_subscriber::fmt::format::Format<Json, SystemTime>,
}
impl<S, N> FormatEvent<S, N> for Format
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> fmt::FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &fmt::FmtContext<'_, S, N>,
        writer: fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        match self.kind {
            config::LogFormat::SingleLine => self.single.format_event(ctx, writer, event),
            config::LogFormat::MultiLine => self.multi.format_event(ctx, writer, event),
            config::LogFormat::Json => self.json.format_event(ctx, writer, event),
        }
    }
}

/// Initialize the tracing subsystem.
fn init_tracer(cfg: Config) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "tokio-console")]
    let console_subscriber = cfg.tokio_console.then(console_subscriber::spawn);

    // Default to INFO if no env is specified
    let filter_builder = EnvFilter::builder().with_default_directive(LevelFilter::INFO.into());

    let log_filter = filter_builder.from_env()?;
    let otlp_filter = filter_builder.from_env()?;

    let format = Format {
        kind: cfg.log_format,
        single: fmt::format().with_ansi(true).compact(),
        multi: fmt::format().with_ansi(true).pretty(),
        json: fmt::format().with_ansi(false).json(),
    };

    let log_subscriber = fmt::layer().event_format(format).with_filter(log_filter);

    let opentelemetry_subscriber = if cfg.tracing {
        global::set_text_map_propagator(TraceContextPropagator::new());
        let exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(cfg.collector_endpoint)
            .with_timeout(std::time::Duration::from_secs(5));

        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(exporter)
            .with_trace_config(trace::config().with_resource(Resource::new(vec![
                opentelemetry::KeyValue::new("instance.id", cfg.instance_id),
                opentelemetry::KeyValue::new("service.name", cfg.service_name),
                opentelemetry::KeyValue::new("service.version", cfg.version),
                opentelemetry::KeyValue::new("service.build", cfg.build),
                opentelemetry::KeyValue::new("service.os", OS),
                opentelemetry::KeyValue::new("service.ARCH", ARCH),
                opentelemetry::KeyValue::new("service.environment", cfg.service_env),
            ])))
            .install_batch(opentelemetry::runtime::Tokio)?;

        Some(
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(otlp_filter),
        )
    } else {
        None
    };

    #[cfg(feature = "tokio-console")]
    let registry = tracing_subscriber::registry().with(console_subscriber);
    #[cfg(not(feature = "tokio-console"))]
    let registry = tracing_subscriber::registry();

    registry
        .with(log_subscriber)
        .with(opentelemetry_subscriber)
        .try_init()?;

    Ok(())
}

pub fn get_current_trace_id() -> TraceId {
    tracing::Span::current()
        .context()
        .span()
        .span_context()
        .trace_id()
}

#[derive(Debug, PartialEq, Eq)]
pub enum Collector {
    #[cfg(feature = "gateway")]
    Gateway,
    #[cfg(feature = "resolver")]
    Resolver,
    #[cfg(feature = "bitswap")]
    Bitswap,
    #[cfg(feature = "store")]
    Store,
    #[cfg(feature = "p2p")]
    P2P,
}

#[allow(unused_variables, unreachable_patterns)]
pub fn record<M>(c: Collector, m: M, v: u64)
where
    M: MetricType + std::fmt::Display,
{
    if CORE.enabled() {
        match c {
            #[cfg(feature = "gateway")]
            Collector::Gateway => CORE.gateway_metrics().record(m, v),
            #[cfg(feature = "resolver")]
            Collector::Resolver => CORE.resolver_metrics().record(m, v),
            #[cfg(feature = "bitswap")]
            Collector::Bitswap => CORE.bitswap_metrics().record(m, v),
            #[cfg(feature = "store")]
            Collector::Store => CORE.store_metrics().record(m, v),
            #[cfg(feature = "p2p")]
            Collector::P2P => CORE.p2p_metrics().record(m, v),
            _ => panic!("not enabled/implemented"),
        };
    }
}

#[allow(unused_variables, unreachable_patterns)]
pub fn observe<M>(c: Collector, m: M, v: f64)
where
    M: HistogramType + std::fmt::Display,
{
    if CORE.enabled() {
        match c {
            #[cfg(feature = "gateway")]
            Collector::Gateway => CORE.gateway_metrics().observe(m, v),
            #[cfg(feature = "resolver")]
            Collector::Resolver => CORE.resolver_metrics().observe(m, v),
            #[cfg(feature = "bitswap")]
            Collector::Bitswap => CORE.bitswap_metrics().observe(m, v),
            #[cfg(feature = "store")]
            Collector::Store => CORE.store_metrics().observe(m, v),
            #[cfg(feature = "p2p")]
            Collector::P2P => CORE.p2p_metrics().observe(m, v),
            _ => panic!("not enabled/implemented"),
        };
    }
}

#[cfg(feature = "p2p")]
pub fn libp2p_metrics() -> &'static p2p::Libp2pMetrics {
    CORE.libp2p_metrics()
}
