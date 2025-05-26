use ceramic_actor::MessageEvent;
use ceramic_metrics::Recorder;

use crate::metrics::{MessageLabels, Metrics};

use super::{ConcluderRecorder, NewEventsMsg};

impl Recorder<MessageEvent<NewEventsMsg>> for Metrics {
    fn record(&self, event: &MessageEvent<NewEventsMsg>) {
        self.message_count
            .get_or_create(&MessageLabels::from(event))
            .inc();
    }
}
impl ConcluderRecorder for Metrics {}
