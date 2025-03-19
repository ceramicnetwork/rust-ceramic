use ceramic_actor::MessageEvent;
use ceramic_metrics::register;
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family},
    registry::Registry,
};

/// Metrics for Ceramic Pipeline events
#[derive(Debug, Clone)]
pub struct Metrics {
    pub(crate) message_count: Family<MessageLabels, Counter>,

    pub(crate) concluder_poll_new_events_loop_count: Counter,

    pub(crate) aggregator_new_conclusion_events_count: Counter,

    pub(crate) resolver_new_event_states_count: Counter,
}
impl Metrics {
    /// Register and construct Metrics
    pub fn register(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry_with_prefix("pipeline");

        register!(
            concluder_poll_new_events_loop_count,
            "Number of times the loop to poll new conclusion events has run",
            Counter::default(),
            sub_registry
        );

        register!(
            message_count,
            "Number of messages delivered to actors",
            Family::<MessageLabels, Counter>::default(),
            sub_registry
        );
        register!(
            aggregator_new_conclusion_events_count,
            "Number of new conclusion events delivered to the aggregator",
            Counter::default(),
            sub_registry
        );
        register!(
            resolver_new_event_states_count,
            "Number of new event states delivered to the resolver",
            Counter::default(),
            sub_registry
        );

        Self {
            message_count,
            concluder_poll_new_events_loop_count,
            aggregator_new_conclusion_events_count,
            resolver_new_event_states_count,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct MessageLabels {
    actor: &'static str,
    message: &'static str,
}

impl<T> From<&MessageEvent<T>> for MessageLabels {
    fn from(value: &MessageEvent<T>) -> Self {
        Self {
            actor: value.actor_type,
            message: value.message_type,
        }
    }
}
