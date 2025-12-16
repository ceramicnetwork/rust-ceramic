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
    AssociativeHash, InvalidItem, Key,
};

/// Metrics for Recon P2P events
#[derive(Debug, Clone)]
pub struct Metrics {
    protocol_message_received_count: Family<MessageLabels, Counter>,
    protocol_message_sent_count: Family<MessageLabels, Counter>,

    protocol_write_loop_count: Counter,
    protocol_run_duration: Histogram,
    protocol_run_new_keys: Histogram,

    protocol_pending_items: Counter,
    protocol_invalid_items: Family<InvalidItemLabels, Counter>,

    /// Number of connections denied due to peer being blocked
    blocked_connection_count: Counter,
    /// Number of inbound syncs rejected due to backoff
    inbound_sync_rejected_count: Counter,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct MessageLabels {
    pub(crate) message_type: &'static str,
}

impl<K: Key, H: AssociativeHash> From<&InitiatorMessage<K, H>> for MessageLabels {
    fn from(value: &InitiatorMessage<K, H>) -> Self {
        match value {
            InitiatorMessage::Value(_) => Self {
                message_type: "Value",
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
            ResponderMessage::Value(_) => Self {
                message_type: "Value",
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

impl Metrics {
    /// Register and construct Metrics
    pub fn register(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry_with_prefix("recon");

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
            protocol_write_loop_count,
            "Number times the protocol write loop has iterated",
            Counter::default(),
            sub_registry
        );
        register!(
            protocol_run_duration,
            "Duration of protocol runs to completion",
            Histogram::new(exponential_buckets(0.005, 2.0, 20)),
            sub_registry
        );
        register!(
            protocol_run_new_keys,
            "Number of new keys discovered for each protocol run",
            Histogram::new(exponential_buckets(1.0, 2.0, 20)),
            sub_registry
        );

        register!(
            protocol_pending_items,
            "Number of items received that depend on undiscovered events",
            Counter::default(),
            sub_registry
        );

        register!(
            protocol_invalid_items,
            "Number of items received that were considered invalid",
            Family::<InvalidItemLabels, Counter>::default(),
            sub_registry
        );

        register!(
            blocked_connection_count,
            "Number of connections denied due to peer being blocked",
            Counter::default(),
            sub_registry
        );

        register!(
            inbound_sync_rejected_count,
            "Number of inbound syncs rejected due to backoff",
            Counter::default(),
            sub_registry
        );

        Self {
            protocol_message_received_count,
            protocol_message_sent_count,
            protocol_write_loop_count,
            protocol_run_duration,
            protocol_run_new_keys,
            protocol_pending_items,
            protocol_invalid_items,
            blocked_connection_count,
            inbound_sync_rejected_count,
        }
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

pub(crate) struct ProtocolWriteLoop;
impl Recorder<ProtocolWriteLoop> for Metrics {
    fn record(&self, _event: &ProtocolWriteLoop) {
        self.protocol_write_loop_count.inc();
    }
}

pub(crate) struct ProtocolRun(pub Duration, pub f64);
impl Recorder<ProtocolRun> for Metrics {
    fn record(&self, event: &ProtocolRun) {
        self.protocol_run_duration.observe(event.0.as_secs_f64());
        self.protocol_run_new_keys.observe(event.1);
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct InvalidItemLabels {
    pub(crate) reason: &'static str,
}

impl<K: Key> Recorder<InvalidItem<K>> for Metrics {
    fn record(&self, event: &InvalidItem<K>) {
        let labels = event.into();
        self.protocol_invalid_items.get_or_create(&labels).inc();
    }
}

impl<K: Key> From<&InvalidItem<K>> for InvalidItemLabels {
    fn from(value: &InvalidItem<K>) -> Self {
        match value {
            InvalidItem::InvalidFormat { .. } => InvalidItemLabels {
                reason: "InvalidFormat",
            },
            InvalidItem::InvalidSignature { .. } => InvalidItemLabels {
                reason: "InvalidSignature",
            },
        }
    }
}

pub(crate) struct PendingEvents(pub u64);
impl Recorder<PendingEvents> for Metrics {
    fn record(&self, event: &PendingEvents) {
        self.protocol_pending_items.inc_by(event.0);
    }
}

/// Event for when a connection is blocked
#[derive(Debug)]
pub struct BlockedConnection;
impl Recorder<BlockedConnection> for Metrics {
    fn record(&self, _event: &BlockedConnection) {
        self.blocked_connection_count.inc();
    }
}

/// Event for when an inbound sync is rejected due to backoff
#[derive(Debug)]
pub struct InboundSyncRejected;
impl Recorder<InboundSyncRejected> for Metrics {
    fn record(&self, _event: &InboundSyncRejected) {
        self.inbound_sync_rejected_count.inc();
    }
}
