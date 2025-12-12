use ceramic_actor::MessageEvent;
use ceramic_metrics::Recorder;

use crate::metrics::{MessageLabels, Metrics};

use super::{AggregatorRecorder, EventValidationStatusMsg, NewConclusionEventsMsg, StreamStateMsg};

impl Recorder<MessageEvent<NewConclusionEventsMsg>> for Metrics {
    fn record(&self, event: &MessageEvent<NewConclusionEventsMsg>) {
        self.message_count
            .get_or_create(&MessageLabels::from(event))
            .inc();
        self.aggregator_new_conclusion_events_count
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
impl Recorder<MessageEvent<EventValidationStatusMsg>> for Metrics {
    fn record(&self, event: &MessageEvent<EventValidationStatusMsg>) {
        self.message_count
            .get_or_create(&MessageLabels::from(event))
            .inc();
    }
}
impl AggregatorRecorder for Metrics {}
