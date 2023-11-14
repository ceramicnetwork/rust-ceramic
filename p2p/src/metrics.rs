use ceramic_metrics::Recorder;
use prometheus_client::{
    encoding::{EncodeLabelSet, EncodeLabelValue},
    metrics::{counter::Counter, family::Family},
    registry::Registry,
};

/// Metrics for Ceramic P2P events
#[derive(Clone)]
pub struct Metrics {
    publish_results: Family<PublishResultsLabels, Counter>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PublishResultsLabels {
    result: PublishResult,
}

impl From<&PublishResult> for PublishResultsLabels {
    fn from(value: &PublishResult) -> Self {
        Self { result: *value }
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum PublishResult {
    Success,
    Failed,
}

impl Metrics {
    /// Register and construct Metrics
    pub fn register(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry_with_prefix("p2p");

        let publish_results = Family::<PublishResultsLabels, Counter>::default();
        sub_registry.register(
            "publish_results",
            "Number of provider records results",
            publish_results.clone(),
        );

        Self { publish_results }
    }
}

pub enum Event {
    PublishResult(PublishResult),
}

impl Recorder<Option<Event>> for Metrics {
    fn record(&self, event: &Option<Event>) {
        match event {
            Some(Event::PublishResult(result)) => {
                let labels = result.into();
                self.publish_results.get_or_create(&labels).inc();
            }
            None => {}
        }
    }
}
