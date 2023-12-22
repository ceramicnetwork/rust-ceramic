use ceramic_metrics::{register, Recorder};
use prometheus_client::{metrics::counter::Counter, registry::Registry};

/// Metrics for Recon P2P events
#[derive(Debug, Clone)]
pub struct Metrics {
    key_insert_count: Counter,
    value_insert_count: Counter,
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
            value_insert_count,
            "Number times a new value is inserted into the datastore",
            Counter::default(),
            sub_registry
        );

        Self {
            key_insert_count,
            value_insert_count,
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

#[derive(Debug)]
pub struct ValueInsertEvent;

impl Recorder<ValueInsertEvent> for Metrics {
    fn record(&self, _event: &ValueInsertEvent) {
        self.value_insert_count.inc();
    }
}
