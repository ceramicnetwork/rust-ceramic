use std::time::Duration;

use ceramic_metrics::{register, Recorder};
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{
        counter::Counter,
        family::Family,
        histogram::{exponential_buckets, Histogram},
    },
    registry::Registry,
};

/// Metrics for Recon P2P events
#[derive(Debug, Clone)]
pub struct Metrics {
    key_insert_count: Counter,

    store_query_durations: Family<QueryLabels, Histogram>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct QueryLabels {
    name: &'static str,
}

impl From<&StoreQuery> for QueryLabels {
    fn from(value: &StoreQuery) -> Self {
        Self { name: value.name }
    }
}

impl Metrics {
    /// Register and construct Metrics
    pub fn register(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry_with_prefix("recon");

        register!(
            key_insert_count,
            "Number times a new key is inserted into the datastore",
            Counter::default(),
            sub_registry
        );

        register!(
            store_query_durations,
            "Durations of store queries in seconds",
            Family::<QueryLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.005, 2.0, 20))
            }),
            sub_registry
        );

        Self {
            key_insert_count,
            store_query_durations,
        }
    }
}

#[derive(Debug)]
pub struct KeyInsertEvent;

impl Recorder<KeyInsertEvent> for Metrics {
    fn record(&self, _event: &KeyInsertEvent) {
        self.key_insert_count.inc();
    }
}

pub struct StoreQuery {
    pub(crate) name: &'static str,
    pub(crate) duration: Duration,
}

impl Recorder<StoreQuery> for Metrics {
    fn record(&self, event: &StoreQuery) {
        let labels: QueryLabels = event.into();
        self.store_query_durations
            .get_or_create(&labels)
            .observe(event.duration.as_secs_f64());
    }
}
