use std::borrow::Cow;

use libp2p::metrics::Recorder;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::registry::Registry;

use crate::pubsub;

pub struct Metrics {
    update_messages: Counter,
    query_messages: Counter,
    response_messages: Counter,
    keepalive_messages: Counter,
}

impl Metrics {
    pub fn new(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry_with_prefix("pubsub");

        let update_messages = Counter::default();
        sub_registry
            .sub_registry_with_label((Cow::Borrowed("msg_type"), Cow::Borrowed("update")))
            .register(
                "query",
                "Number of response messages received",
                Box::new(update_messages.clone()),
            );

        let query_messages = Counter::default();
        sub_registry
            .sub_registry_with_label((Cow::Borrowed("msg_type"), Cow::Borrowed("query")))
            .register(
                "query",
                "Number of query messages received",
                Box::new(query_messages.clone()),
            );

        let response_messages = Counter::default();
        sub_registry
            .sub_registry_with_label((Cow::Borrowed("msg_type"), Cow::Borrowed("response")))
            .register(
                "query",
                "Number of response messages received",
                Box::new(response_messages.clone()),
            );

        let keepalive_messages = Counter::default();
        sub_registry
            .sub_registry_with_label((Cow::Borrowed("msg_type"), Cow::Borrowed("keepalive")))
            .register(
                "query",
                "Number of keepalive messages received",
                Box::new(keepalive_messages.clone()),
            );

        Self {
            query_messages,
            response_messages,
            update_messages,
            keepalive_messages,
        }
    }
}

impl Recorder<pubsub::Message> for Metrics {
    fn record(&self, event: &pubsub::Message) {
        match event {
            pubsub::Message::Update { .. } => self.update_messages.inc(),
            pubsub::Message::Query { .. } => self.query_messages.inc(),
            pubsub::Message::Response { .. } => self.response_messages.inc(),
            pubsub::Message::Keepalive { .. } => self.keepalive_messages.inc(),
        };
    }
}
