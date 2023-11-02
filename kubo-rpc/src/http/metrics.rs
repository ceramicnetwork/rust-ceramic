pub mod api;

use std::time::Duration;

use ceramic_kubo_rpc_server::{API_VERSION, BASE_PATH};
use ceramic_metrics::Recorder;
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RequestLabels {
    path: &'static str,
}

impl From<&Event> for RequestLabels {
    fn from(value: &Event) -> Self {
        Self { path: value.path }
    }
}

/// Metrics for Kubo RPC API
#[derive(Clone)]
pub struct Metrics {
    requests: Family<RequestLabels, Counter>,
    request_durations: Family<RequestLabels, Histogram>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct InfoLabels {
    base_path: &'static str,
    version: &'static str,
}

impl Metrics {
    /// Register and construct Metrics
    pub fn register(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry_with_prefix("kubo_rpc");

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
            base_path: BASE_PATH,
            version: API_VERSION,
        });
        sub_registry.register("api", "Information about the Kubo RPC API", info);

        Self {
            requests,
            request_durations,
        }
    }
}

pub struct Event {
    pub(crate) path: &'static str,
    pub(crate) duration: Duration,
}

impl Recorder<Event> for Metrics {
    fn record(&self, event: &Event) {
        let labels: RequestLabels = event.into();
        self.requests.get_or_create(&labels).inc();
        self.request_durations
            .get_or_create(&labels)
            .observe(event.duration.as_secs_f64());
    }
}
