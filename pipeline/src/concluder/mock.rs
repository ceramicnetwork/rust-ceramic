//! Provides a mock implmentation of the concluder actor.
use async_trait::async_trait;
use ceramic_actor::{Actor, Handler, Message};
use mockall::mock;
use prometheus_client::registry::Registry;

use crate::metrics::Metrics;

use super::{
    Concluder, ConcluderActor, ConcluderEnvelope, ConcluderHandle, NewEventsMsg, SubscribeSinceMsg,
};

mock! {
    // mockall does not support multiple methods on the struct with the same name.
    // This arises when implementing multiple traits that have methods with the same name as is
    // the case with the [`ceramic_actor::Handler`] trait.
    //
    // We add a layer of indirection to get around this limitation.
    pub Concluder {
        #[allow(missing_docs)]
        pub fn handle_new_events(
            &mut self,
            message: NewEventsMsg,
        ) -> <NewEventsMsg as Message>::Result;
        #[allow(missing_docs)]
        pub fn handle_subscribe_since(
            &mut self,
            message: SubscribeSinceMsg,
        ) -> <SubscribeSinceMsg as Message>::Result;
    }
}

#[async_trait]
impl Handler<NewEventsMsg> for MockConcluder {
    async fn handle(&mut self, message: NewEventsMsg) -> <NewEventsMsg as Message>::Result {
        self.handle_new_events(message)
    }
}

#[async_trait]
impl Handler<SubscribeSinceMsg> for MockConcluder {
    async fn handle(
        &mut self,
        message: SubscribeSinceMsg,
    ) -> <SubscribeSinceMsg as Message>::Result {
        self.handle_subscribe_since(message)
    }
}

impl Actor for MockConcluder {
    type Envelope = ConcluderEnvelope;
}
impl ConcluderActor for MockConcluder {}

impl MockConcluder {
    /// Spawn a mock concluder actor.
    pub fn spawn(mock_actor: MockConcluder) -> ConcluderHandle {
        let metrics = Metrics::register(&mut Registry::default());
        let (handle, _task_handle) =
            Concluder::spawn(1_000, mock_actor, metrics, std::future::pending());
        handle
    }
}
