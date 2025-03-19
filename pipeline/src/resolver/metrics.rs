use ceramic_actor::MessageEvent;
use ceramic_metrics::Recorder;

use crate::metrics::{MessageLabels, Metrics};

use super::{NewEventStatesMsg, ResolverRecorder, StreamStateMsg};

impl Recorder<MessageEvent<NewEventStatesMsg>> for Metrics {
    fn record(&self, event: &MessageEvent<NewEventStatesMsg>) {
        self.message_count
            .get_or_create(&MessageLabels::from(event))
            .inc();
        self.resolver_new_event_states_count
            .inc_by(event.message.events.num_rows() as u64);
    }
}
impl Recorder<MessageEvent<StreamStateMsg>> for Metrics {
    fn record(&self, event: &MessageEvent<StreamStateMsg>) {
        self.message_count
            .get_or_create(&MessageLabels::from(event))
            .inc();
    }
}
impl ResolverRecorder for Metrics {}
