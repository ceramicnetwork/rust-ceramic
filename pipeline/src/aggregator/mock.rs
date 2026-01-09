//! Provides a mock implmentation of the aggregator actor.
use async_trait::async_trait;
use ceramic_actor::{Actor, Handler, Message, Shutdown};
use mockall::mock;
use prometheus_client::registry::Registry;

use crate::metrics::Metrics;

use super::{
    Aggregator, AggregatorActor, AggregatorEnvelope, AggregatorHandle, EventValidationStatusMsg,
    NewConclusionEventsMsg, StreamStateMsg, SubscribeSinceMsg,
};

mock! {
    // mockall does not support multiple methods on the struct with the same name.
    // This arises when implementing multiple traits that have methods with the same name as is
    // the case with the [`ceramic_actor::Handler`] trait.
    //
    // We add a layer of indirection to get around this limitation.
    pub Aggregator {
        #[allow(missing_docs)]
        pub fn handle_subscribe_since(
            &mut self,
            message: SubscribeSinceMsg,
        ) -> <SubscribeSinceMsg as Message>::Result;
        #[allow(missing_docs)]
        pub fn handle_new_conclusion_events(
            &mut self,
            message: NewConclusionEventsMsg,
        ) -> <NewConclusionEventsMsg as Message>::Result;
        #[allow(missing_docs)]
        pub fn handle_stream_state(
            &mut self,
            message: StreamStateMsg,
        ) -> <StreamStateMsg
            as Message>::Result;
        #[allow(missing_docs)]
        pub fn handle_event_validation_status(
            &mut self,
            message: EventValidationStatusMsg,
        ) -> <EventValidationStatusMsg as Message>::Result;
    }
}

#[async_trait]
impl Handler<SubscribeSinceMsg> for MockAggregator {
    async fn handle(
        &mut self,
        message: SubscribeSinceMsg,
    ) -> <SubscribeSinceMsg as Message>::Result {
        self.handle_subscribe_since(message)
    }
}

#[async_trait]
impl Handler<NewConclusionEventsMsg> for MockAggregator {
    async fn handle(
        &mut self,
        message: NewConclusionEventsMsg,
    ) -> <NewConclusionEventsMsg as Message>::Result {
        self.handle_new_conclusion_events(message)
    }
}

#[async_trait]
impl Handler<StreamStateMsg> for MockAggregator {
    async fn handle(&mut self, message: StreamStateMsg) -> <StreamStateMsg as Message>::Result {
        self.handle_stream_state(message)
    }
}

#[async_trait]
impl Handler<EventValidationStatusMsg> for MockAggregator {
    async fn handle(
        &mut self,
        message: EventValidationStatusMsg,
    ) -> <EventValidationStatusMsg as Message>::Result {
        self.handle_event_validation_status(message)
    }
}

impl Actor for MockAggregator {
    type Envelope = AggregatorEnvelope;
}
impl AggregatorActor for MockAggregator {}

#[async_trait]
impl Shutdown for MockAggregator {
    async fn shutdown(&mut self) {
        // Mock implementation - no cleanup needed
    }
}

impl MockAggregator {
    /// Spawn a mock aggregator actor.
    pub fn spawn(mock_actor: MockAggregator) -> AggregatorHandle {
        let metrics = Metrics::register(&mut Registry::default());
        let (handle, _task_handle) =
            Aggregator::spawn(1_000, mock_actor, metrics, std::future::pending());
        handle
    }
}
