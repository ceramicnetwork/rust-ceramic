use ceramic_metrics::{register, Recorder};
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family},
    registry::Registry,
};

use crate::{libp2p::protocol::Envelope, AssociativeHash, Key};

/// Metrics for Recon P2P events
#[derive(Debug, Clone)]
pub struct Metrics {
    envelope_received_count: Family<EnvelopeLabels, Counter>,
    envelope_sent_count: Family<EnvelopeLabels, Counter>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct EnvelopeLabels {
    envelope_type: &'static str,
}

impl<K: Key, H: AssociativeHash> From<&Envelope<K, H>> for EnvelopeLabels {
    fn from(value: &Envelope<K, H>) -> Self {
        match value {
            Envelope::Synchronize(_, _) => Self {
                envelope_type: "Synchronize",
            },
            Envelope::ValueRequest(_) => Self {
                envelope_type: "ValueRequest",
            },
            Envelope::ValueResponse(_, _) => Self {
                envelope_type: "ValueResponse",
            },
            Envelope::HangUp => Self {
                envelope_type: "HangUp",
            },
        }
    }
}

impl Metrics {
    /// Register and construct Metrics
    pub fn register(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry_with_prefix("recon_p2p");

        register!(
            envelope_received_count,
            "Number times an envelope messages is received",
            Family::<EnvelopeLabels, Counter>::default(),
            sub_registry
        );

        register!(
            envelope_sent_count,
            "Number times an envelope messages is sent",
            Family::<EnvelopeLabels, Counter>::default(),
            sub_registry
        );

        Self {
            envelope_received_count,
            envelope_sent_count,
        }
    }
}

pub(crate) struct EnvelopeRecv<'a, K: Key, H: AssociativeHash>(pub &'a Envelope<K, H>);
pub(crate) struct EnvelopeSent<'a, K: Key, H: AssociativeHash>(pub &'a Envelope<K, H>);

impl<'a, K: Key, H: AssociativeHash> Recorder<EnvelopeRecv<'a, K, H>> for Metrics {
    fn record(&self, event: &EnvelopeRecv<'a, K, H>) {
        let labels = event.0.into();
        self.envelope_received_count.get_or_create(&labels).inc();
    }
}
impl<'a, K: Key, H: AssociativeHash> Recorder<EnvelopeSent<'a, K, H>> for Metrics {
    fn record(&self, event: &EnvelopeSent<'a, K, H>) {
        let labels = event.0.into();
        self.envelope_sent_count.get_or_create(&labels).inc();
    }
}
