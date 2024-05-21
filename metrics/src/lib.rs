// Allow use of deprecated traits until we remove them.
#![allow(deprecated)]

#[macro_use]
mod macros;
pub mod bitswap;
pub mod config;
pub mod core;

#[macro_use]
extern crate lazy_static;

use crate::core::HistogramType;
use crate::core::MetricType;
use crate::core::CORE;
use crate::{config::Config, core::MetricsRecorder};
use opentelemetry::{
    global,
    trace::{TraceContextExt, TraceId},
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace, Resource};
use prometheus_client::registry::Registry;
use std::env::consts::{ARCH, OS};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, metadata::LevelFilter, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{
    fmt::{
        self,
        format::{Compact, DefaultFields, Json, JsonFields, Pretty},
        time::SystemTime,
        FormatEvent, FormatFields,
    },
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter, Layer,
};

/// Recorder that can record metrics about an event.
pub trait Recorder<Event> {
    /// Record the given event.
    fn record(&self, event: &Event);
}

impl<S, Event> Recorder<Event> for std::sync::Arc<S>
where
    S: Recorder<Event>,
{
    fn record(&self, event: &Event) {
        self.as_ref().record(event);
    }
}

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
    None
}

// Implement a FormatEvent type that can be configured to one of a set of log formats.
struct EventFormat {
    kind: config::LogFormat,
    single: tracing_subscriber::fmt::format::Format<Compact, SystemTime>,
    multi: tracing_subscriber::fmt::format::Format<Pretty, SystemTime>,
    json: tracing_subscriber::fmt::format::Format<Json, SystemTime>,
}

impl EventFormat {
    fn new(kind: config::LogFormat) -> Self {
        Self {
            kind,
            single: fmt::format().compact(),
            multi: fmt::format().pretty(),
            json: fmt::format().json(),
        }
    }
}

impl<S, N> FormatEvent<S, N> for EventFormat
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

// Implement a FormatFields type that can be configured to one of a set of log formats.
pub struct FieldsFormat {
    kind: config::LogFormat,
    default_fields: DefaultFields,
    json_fields: JsonFields,
}

impl FieldsFormat {
    pub fn new(kind: config::LogFormat) -> Self {
        Self {
            kind,
            default_fields: DefaultFields::new(),
            json_fields: JsonFields::new(),
        }
    }
}

impl<'writer> FormatFields<'writer> for FieldsFormat {
    fn format_fields<R: tracing_subscriber::prelude::__tracing_subscriber_field_RecordFields>(
        &self,
        writer: fmt::format::Writer<'writer>,
        fields: R,
    ) -> std::fmt::Result {
        match self.kind {
            config::LogFormat::SingleLine => self.default_fields.format_fields(writer, fields),
            config::LogFormat::MultiLine => self.default_fields.format_fields(writer, fields),
            config::LogFormat::Json => self.json_fields.format_fields(writer, fields),
        }
    }
}

/// For use in CLI tools that are writing to stdout without metrics
/// Will start a tokio console subscriber if the feature is enabled
pub fn init_local_tracing() -> Result<(), Box<dyn std::error::Error>> {
    let filter_builder = EnvFilter::builder().with_default_directive(LevelFilter::INFO.into());

    let log_filter = filter_builder.from_env()?;

    // Configure both the fields and event formats.
    let fields_format = FieldsFormat::new(config::LogFormat::MultiLine);
    let event_format = EventFormat::new(config::LogFormat::MultiLine);

    let log_subscriber = fmt::layer()
        // The JSON format ignore the ansi setting and always format without colors.
        .with_ansi(true)
        .event_format(event_format)
        .fmt_fields(fields_format)
        .with_filter(log_filter);

    #[cfg(feature = "tokio-console")]
    let registry = tracing_subscriber::registry().with(console_subscriber::spawn());
    #[cfg(not(feature = "tokio-console"))]
    let registry = tracing_subscriber::registry();

    registry.with(log_subscriber).try_init()?;

    Ok(())
}

/// Initialize the tracing subsystem.
fn init_tracer(cfg: Config) -> Result<(), Box<dyn std::error::Error>> {
    // Default to INFO if no env is specified
    let filter_builder = EnvFilter::builder().with_default_directive(LevelFilter::INFO.into());

    let log_filter = filter_builder.from_env()?;
    let otlp_filter = filter_builder.from_env()?;

    // Configure both the fields and event formats.
    let fields_format = FieldsFormat::new(cfg.log_format.clone());
    let event_format = EventFormat::new(cfg.log_format);

    let log_subscriber = fmt::layer()
        // The JSON format ignore the ansi setting and always format without colors.
        .with_ansi(true)
        .event_format(event_format)
        .fmt_fields(fields_format)
        .with_filter(log_filter);

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
            .install_batch(opentelemetry_sdk::runtime::Tokio)?;

        Some(
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(otlp_filter),
        )
    } else {
        None
    };

    #[cfg(feature = "tokio-console")]
    let registry =
        tracing_subscriber::registry().with(cfg.tokio_console.then(console_subscriber::spawn));
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
    Bitswap,
}

#[allow(unused_variables, unreachable_patterns)]
pub fn record<M>(c: Collector, m: M, v: u64)
where
    M: MetricType + std::fmt::Display,
{
    match c {
        Collector::Bitswap => CORE.bitswap_metrics().record(m, v),
        _ => panic!("not enabled/implemented"),
    };
}

#[allow(unused_variables, unreachable_patterns)]
pub fn observe<M>(c: Collector, m: M, v: f64)
where
    M: HistogramType + std::fmt::Display,
{
    match c {
        Collector::Bitswap => CORE.bitswap_metrics().observe(m, v),
        _ => panic!("not enabled/implemented"),
    };
}

pub fn libp2p_metrics() -> &'static libp2p::metrics::Metrics {
    CORE.libp2p_metrics()
}
