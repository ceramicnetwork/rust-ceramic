use std::time::Duration;

use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{
        counter::Counter,
        family::Family,
        histogram::{exponential_buckets, Histogram},
        info::Info,
    },
    registry::Registry,
};

/// Metrics for Kubo RPC API
#[derive(Clone)]
pub struct Metrics {
    requests: Family<RequestLabels, Counter>,
    request_durations: Family<RequestLabels, Histogram>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct InfoLabels {
    api_version: &'static str,
    kubo_api_version: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RequestLabels {
    method: String,
    path: String,
    status: u16,
}

impl From<&Event> for RequestLabels {
    fn from(value: &Event) -> Self {
        Self {
            method: value.method.clone(),
            path: value.path.clone(),
            status: value.status_code,
        }
    }
}

impl Metrics {
    /// Register and construct Metrics
    pub fn register(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry_with_prefix("http_api");

        let requests = Family::<RequestLabels, Counter>::default();
        sub_registry.register("requests", "Number of HTTP requests", requests.clone());

        let request_durations = Family::<RequestLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(exponential_buckets(0.005, 2.0, 20))
        });
        sub_registry.register(
            "request_durations",
            "Duration of HTTP requests",
            request_durations.clone(),
        );

        let info: Info<InfoLabels> = Info::new(InfoLabels {
            api_version: ceramic_api_server::API_VERSION,
            kubo_api_version: ceramic_kubo_rpc_server::API_VERSION,
        });
        sub_registry.register(
            "http_api",
            "Information about the Ceramic and Kubo APIs",
            info,
        );

        Self {
            requests,
            request_durations,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Event {
    pub(crate) path: String,
    pub(crate) method: String,
    pub(crate) status_code: u16,
    pub(crate) duration: Duration,
}

impl ceramic_metrics::Recorder<Event> for Metrics {
    fn record(&self, event: &Event) {
        let labels = event.into();
        self.requests.get_or_create(&labels).inc();
        self.request_durations
            .get_or_create(&labels)
            .observe(event.duration.as_secs_f64());
    }
}
