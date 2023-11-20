use ceramic_metrics::{register, Recorder};
use prometheus_client::{
    encoding::{EncodeLabelSet, EncodeLabelValue},
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};

/// Metrics for Ceramic P2P events
#[derive(Clone)]
pub struct Metrics {
    publish_results: Family<PublishResultsLabels, Counter>,

    publisher_result_send_err_count: Counter,
    publisher_batch_send_err_count: Counter,

    publisher_batch_new_count: Gauge,
    publisher_batch_repeat_count: Gauge,
    publisher_batch_max_retry_count: Gauge,

    publisher_batches_finished: Counter,
    publisher_lag_ratio: Gauge<f64, std::sync::atomic::AtomicU64>,
    publisher_batch_size: Gauge<i64>,

    peering_connected_count: Counter,
    peering_disconnected_count: Counter,
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

        register!(
            publish_results,
            "Number of provider records results",
            Family::<PublishResultsLabels, Counter>::default(),
            sub_registry
        );

        register!(
            publisher_result_send_err_count,
            "Number of errors sending a result over the internal channel",
            Counter::default(),
            sub_registry
        );
        register!(
            publisher_batch_send_err_count,
            "Number of errors sending a batch over the internal channel",
            Counter::default(),
            sub_registry
        );

        register!(
            publisher_batch_new_count,
            "Number of records in the batch that are new",
            Gauge::default(),
            sub_registry
        );
        register!(
            publisher_batch_repeat_count,
            "Number of records in the batch that are repeated from the previous batch",
            Gauge::default(),
            sub_registry
        );

        register!(
            publisher_batch_max_retry_count,
            "Maximum retry count for any record in the batch",
            Gauge::default(),
            sub_registry
        );

        register!(
            publisher_batches_finished,
            "Number of batches processed",
            Counter::default(),
            sub_registry
        );
        register!(
            publisher_lag_ratio,
            "Ratio of estimated_needed_time / remaining_time",
            Gauge::default(),
            sub_registry
        );
        register!(
            publisher_batch_size,
            "Number of records in the finished batch",
            Gauge::default(),
            sub_registry
        );

        register!(
            peering_connected_count,
            "Number of peering connections established",
            Counter::default(),
            sub_registry
        );
        register!(
            peering_disconnected_count,
            "Number of peering connections closed",
            Counter::default(),
            sub_registry
        );

        Self {
            publish_results,
            publisher_result_send_err_count,
            publisher_batch_send_err_count,
            publisher_batch_new_count,
            publisher_batch_repeat_count,
            publisher_batch_max_retry_count,
            publisher_batches_finished,
            publisher_lag_ratio,
            publisher_batch_size,
            peering_connected_count,
            peering_disconnected_count,
        }
    }
}

pub enum PublisherEvent {
    Result(PublishResult),
    ResultSendErr,
    BatchStarted {
        new_count: i64,
        repeat_count: i64,
        max_retry_count: i64,
    },
    BatchSendErr,
    BatchFinished {
        batch_size: usize,
        lag_ratio: f64,
    },
}

impl Recorder<PublisherEvent> for Metrics {
    fn record(&self, event: &PublisherEvent) {
        match event {
            PublisherEvent::Result(result) => {
                let labels = result.into();
                self.publish_results.get_or_create(&labels).inc();
            }
            PublisherEvent::ResultSendErr => {
                self.publisher_result_send_err_count.inc();
            }
            PublisherEvent::BatchStarted {
                new_count,
                repeat_count,
                max_retry_count,
            } => {
                self.publisher_batch_new_count.set(*new_count);
                self.publisher_batch_repeat_count.set(*repeat_count);
                self.publisher_batch_max_retry_count.set(*max_retry_count);
            }
            PublisherEvent::BatchFinished {
                batch_size,
                lag_ratio,
            } => {
                self.publisher_batches_finished.inc();
                self.publisher_lag_ratio.set(*lag_ratio);
                self.publisher_batch_size.set(*batch_size as i64);
            }
            PublisherEvent::BatchSendErr => {
                self.publisher_batch_send_err_count.inc();
            }
        }
    }
}

pub enum PeeringEvent {
    Connected,
    Disconnected,
}

impl Recorder<PeeringEvent> for Metrics {
    fn record(&self, event: &PeeringEvent) {
        match event {
            PeeringEvent::Connected => {
                self.peering_connected_count.inc();
            }
            PeeringEvent::Disconnected => {
                self.peering_disconnected_count.inc();
            }
        }
    }
}
