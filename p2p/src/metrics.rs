use ceramic_metrics::Recorder;
use prometheus_client::{
    encoding::{EncodeLabelSet, EncodeLabelValue},
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};

/// Metrics for Ceramic P2P events
#[derive(Clone)]
pub struct Metrics {
    publish_results: Family<PublishResultsLabels, Counter>,

    publisher_batch_new_count: Gauge,
    publisher_batch_repeat_count: Gauge,
    publisher_batch_max_retry_count: Gauge,

    publisher_batches_finished: Counter,
    publisher_lag_ratio: Gauge<f64, std::sync::atomic::AtomicU64>,
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

        let publisher_batch_new_count = Gauge::default();
        sub_registry.register(
            "publisher_batch_new_count",
            "Number of records in the batch that are new",
            publisher_batch_new_count.clone(),
        );
        let publisher_batch_repeat_count = Gauge::default();
        sub_registry.register(
            "publisher_batch_repeat_count",
            "Number of records in the batch that are repeated from the previous batch",
            publisher_batch_repeat_count.clone(),
        );

        let publisher_batch_max_retry_count = Gauge::default();
        sub_registry.register(
            "publisher_batch_max_retry_count",
            "Maximum retry count for any record in the batch",
            publisher_batch_max_retry_count.clone(),
        );

        let publisher_batches_finished = Counter::default();
        sub_registry.register(
            "publisher_batches_finished",
            "Number of batches proccessed",
            publisher_batches_finished.clone(),
        );
        let publisher_lag_ratio = Gauge::default();
        sub_registry.register(
            "publisher_lag_ratio",
            "Ratio of estimated_needed_time / remaining_time",
            publish_results.clone(),
        );

        Self {
            publish_results,
            publisher_batch_new_count,
            publisher_batch_repeat_count,
            publisher_batch_max_retry_count,
            publisher_lag_ratio,
            publisher_batches_finished,
        }
    }
}

pub enum PublisherEvent {
    Result(PublishResult),
    BatchStarted {
        new_count: i64,
        repeat_count: i64,
        max_retry_count: i64,
    },
    BatchFinished {
        lag_ratio: f64,
    },
}

impl Recorder<Option<PublisherEvent>> for Metrics {
    fn record(&self, event: &Option<PublisherEvent>) {
        match event {
            Some(PublisherEvent::Result(result)) => {
                let labels = result.into();
                self.publish_results.get_or_create(&labels).inc();
            }
            Some(PublisherEvent::BatchStarted {
                new_count,
                repeat_count,
                max_retry_count,
            }) => {
                self.publisher_batch_new_count.set(*new_count);
                self.publisher_batch_repeat_count.set(*repeat_count);
                self.publisher_batch_max_retry_count.set(*max_retry_count);
            }
            Some(PublisherEvent::BatchFinished { lag_ratio }) => {
                self.publisher_batches_finished.inc();
                self.publisher_lag_ratio.set(*lag_ratio);
            }
            None => {}
        }
    }
}
