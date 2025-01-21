use ceramic_actor::MessageEvent;
use ceramic_metrics::Recorder;

use crate::metrics::{MessageLabels, Metrics};

use super::{ConcluderRecorder, EventsSinceMsg, NewEventsMsg};

impl Recorder<MessageEvent<NewEventsMsg>> for Metrics {
    fn record(&self, event: &MessageEvent<NewEventsMsg>) {
        self.message_count
            .get_or_create(&MessageLabels::from(event))
            .inc();
    }
}
impl Recorder<MessageEvent<EventsSinceMsg>> for Metrics {
    fn record(&self, event: &MessageEvent<EventsSinceMsg>) {
        self.message_count
            .get_or_create(&MessageLabels::from(event))
            .inc();
    }
}
impl ConcluderRecorder for Metrics {}
