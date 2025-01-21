use ceramic_actor::MessageEvent;
use ceramic_metrics::Recorder;

use crate::metrics::{MessageLabels, Metrics};

use super::SubscribeSinceMsg;

impl Recorder<MessageEvent<SubscribeSinceMsg>> for Metrics {
    fn record(&self, event: &MessageEvent<SubscribeSinceMsg>) {
        self.message_count
            .get_or_create(&MessageLabels::from(event))
            .inc();
    }
}
