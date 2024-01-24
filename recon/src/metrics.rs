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

use crate::{
    protocol::{InitiatorMessage, ResponderMessage},
    AssociativeHash, Key,
};

/// Metrics for Recon P2P events
#[derive(Debug, Clone)]
pub struct Metrics {
    key_insert_count: Counter,
    value_insert_count: Counter,

    store_query_durations: Family<QueryLabels, Histogram>,

    protocol_message_received_count: Family<MessageLabels, Counter>,
    protocol_message_sent_count: Family<MessageLabels, Counter>,

    protocol_want_enqueue_failed_count: Counter,
    protocol_want_enqueued_count: Counter,
    protocol_want_dequeued_count: Counter,

    protocol_range_enqueue_failed_count: Counter,
    protocol_range_enqueued_count: Counter,
    protocol_range_dequeued_count: Counter,

    protocol_loop_count: Counter,
    protocol_run_count: Counter,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct MessageLabels {
    message_type: &'static str,
}

impl<K: Key, H: AssociativeHash> From<&InitiatorMessage<K, H>> for MessageLabels {
    fn from(value: &InitiatorMessage<K, H>) -> Self {
        match value {
            InitiatorMessage::ValueRequest(_) => Self {
                message_type: "ValueRequest",
            },
            InitiatorMessage::ValueResponse(_) => Self {
                message_type: "ValueResponse",
            },
            InitiatorMessage::ListenOnly => Self {
                message_type: "ListenOnly",
            },
            InitiatorMessage::Finished => Self {
                message_type: "Finished",
            },
            InitiatorMessage::InterestRequest(_) => Self {
                message_type: "InterestRequest",
            },
            InitiatorMessage::RangeRequest(_) => Self {
                message_type: "RangeRequest",
            },
        }
    }
}

impl<K: Key, H: AssociativeHash> From<&ResponderMessage<K, H>> for MessageLabels {
    fn from(value: &ResponderMessage<K, H>) -> Self {
        match value {
            ResponderMessage::ValueRequest(_) => Self {
                message_type: "ValueRequest",
            },
            ResponderMessage::ValueResponse(_) => Self {
                message_type: "ValueResponse",
            },
            ResponderMessage::ListenOnly => Self {
                message_type: "ListenOnly",
            },
            ResponderMessage::InterestResponse(_) => Self {
                message_type: "InterestResponse",
            },
            ResponderMessage::RangeResponse(_) => Self {
                message_type: "RangeResponse",
            },
        }
    }
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
            value_insert_count,
            "Number times a new value is inserted into the datastore",
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

        register!(
            protocol_message_received_count,
            "Number times a message is received",
            Family::<MessageLabels, Counter>::default(),
            sub_registry
        );

        register!(
            protocol_message_sent_count,
            "Number times a message is sent",
            Family::<MessageLabels, Counter>::default(),
            sub_registry
        );

        register!(
            protocol_want_enqueue_failed_count,
            "Number times key is dropped when enqueing into the want_values queue",
            Counter::default(),
            sub_registry
        );

        register!(
            protocol_want_enqueued_count,
            "Number times key is enqued into the want_values queue",
            Counter::default(),
            sub_registry
        );

        register!(
            protocol_want_dequeued_count,
            "Number times key is dequed from the want_values queue",
            Counter::default(),
            sub_registry
        );

        register!(
            protocol_range_enqueue_failed_count,
            "Number times a range is dropped when enqueing into the ranges queue",
            Counter::default(),
            sub_registry
        );

        register!(
            protocol_range_enqueued_count,
            "Number times a range is enqued into the ranges queue",
            Counter::default(),
            sub_registry
        );

        register!(
            protocol_range_dequeued_count,
            "Number times a range is dequed from the ranges queue",
            Counter::default(),
            sub_registry
        );

        register!(
            protocol_loop_count,
            "Number times the protocol loop has iterated",
            Counter::default(),
            sub_registry
        );
        register!(
            protocol_run_count,
            "Number times the protocol has run to completion",
            Counter::default(),
            sub_registry
        );

        Self {
            key_insert_count,
            value_insert_count,
            store_query_durations,
            protocol_message_received_count,
            protocol_message_sent_count,
            protocol_want_enqueue_failed_count,
            protocol_want_enqueued_count,
            protocol_want_dequeued_count,
            protocol_range_enqueue_failed_count,
            protocol_range_enqueued_count,
            protocol_range_dequeued_count,
            protocol_loop_count,
            protocol_run_count,
        }
    }
}

pub(crate) struct KeyInsertEvent {
    pub(crate) cnt: u64,
}
impl Recorder<KeyInsertEvent> for Metrics {
    fn record(&self, event: &KeyInsertEvent) {
        self.key_insert_count.inc_by(event.cnt);
    }
}

pub(crate) struct StoreQuery {
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

pub(crate) struct ValueInsertEvent {
    pub(crate) cnt: u64,
}
impl Recorder<ValueInsertEvent> for Metrics {
    fn record(&self, event: &ValueInsertEvent) {
        self.value_insert_count.inc_by(event.cnt);
    }
}

pub(crate) struct MessageRecv<'a, T>(pub &'a T);
pub(crate) struct MessageSent<'a, T>(pub &'a T);

impl<'a, T> Recorder<MessageRecv<'a, T>> for Metrics
where
    &'a T: Into<MessageLabels>,
{
    fn record(&self, event: &MessageRecv<'a, T>) {
        let labels = event.0.into();
        self.protocol_message_received_count
            .get_or_create(&labels)
            .inc();
    }
}
impl<'a, T> Recorder<MessageSent<'a, T>> for Metrics
where
    &'a T: Into<MessageLabels>,
{
    fn record(&self, event: &MessageSent<'a, T>) {
        let labels = event.0.into();
        self.protocol_message_sent_count
            .get_or_create(&labels)
            .inc();
    }
}

pub(crate) struct WantEnqueueFailed;
impl Recorder<WantEnqueueFailed> for Metrics {
    fn record(&self, _event: &WantEnqueueFailed) {
        self.protocol_want_enqueue_failed_count.inc();
    }
}

pub(crate) struct WantEnqueued;
impl Recorder<WantEnqueued> for Metrics {
    fn record(&self, _event: &WantEnqueued) {
        self.protocol_want_enqueued_count.inc();
    }
}

pub(crate) struct WantDequeued;
impl Recorder<WantDequeued> for Metrics {
    fn record(&self, _event: &WantDequeued) {
        self.protocol_want_dequeued_count.inc();
    }
}

pub(crate) struct RangeEnqueueFailed;
impl Recorder<RangeEnqueueFailed> for Metrics {
    fn record(&self, _event: &RangeEnqueueFailed) {
        self.protocol_range_enqueue_failed_count.inc();
    }
}

pub(crate) struct RangeEnqueued;
impl Recorder<RangeEnqueued> for Metrics {
    fn record(&self, _event: &RangeEnqueued) {
        self.protocol_range_enqueued_count.inc();
    }
}

pub(crate) struct RangeDequeued;
impl Recorder<RangeDequeued> for Metrics {
    fn record(&self, _event: &RangeDequeued) {
        self.protocol_range_dequeued_count.inc();
    }
}

pub(crate) struct ProtocolLoop;

impl Recorder<ProtocolLoop> for Metrics {
    fn record(&self, _event: &ProtocolLoop) {
        self.protocol_loop_count.inc();
    }
}
pub(crate) struct ProtocolRun;
impl Recorder<ProtocolRun> for Metrics {
    fn record(&self, _event: &ProtocolRun) {
        self.protocol_run_count.inc();
    }
}
