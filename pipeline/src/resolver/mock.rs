//! Provides a mock implmentation of the aggregator actor.
use async_trait::async_trait;
use ceramic_actor::{Actor, Handler, Message};
use mockall::mock;
use prometheus_client::registry::Registry;

use crate::metrics::Metrics;

use super::{
    NewEventStatesMsg, Resolver, ResolverActor, ResolverEnvelope, ResolverHandle, StreamStateMsg,
    SubscribeSinceMsg,
};

mock! {
    // mockall does not support multiple methods on the struct with the same name.
    // This arises when implementing multiple traits that have methods with the same name as is
    // the case with the [`ceramic_actor::Handler`] trait.
    //
    // We add a layer of indirection to get around this limitation.
    pub Resolver {
        #[allow(missing_docs)]
        pub fn handle_subscribe_since(
            &mut self,
            message: SubscribeSinceMsg,
        ) -> <SubscribeSinceMsg as Message>::Result;
        #[allow(missing_docs)]
        pub fn handle_new_event_states(
            &mut self,
            message: NewEventStatesMsg,
        ) -> <NewEventStatesMsg as Message>::Result;
        #[allow(missing_docs)]
        pub fn handle_stream_state(
            &mut self,
            message: StreamStateMsg,
        ) -> <StreamStateMsg
            as Message>::Result;
    }
}

#[async_trait]
impl Handler<SubscribeSinceMsg> for MockResolver {
    async fn handle(
        &mut self,
        message: SubscribeSinceMsg,
    ) -> <SubscribeSinceMsg as Message>::Result {
        self.handle_subscribe_since(message)
    }
}

#[async_trait]
impl Handler<NewEventStatesMsg> for MockResolver {
    async fn handle(
        &mut self,
        message: NewEventStatesMsg,
    ) -> <NewEventStatesMsg as Message>::Result {
        self.handle_new_event_states(message)
    }
}

#[async_trait]
impl Handler<StreamStateMsg> for MockResolver {
    async fn handle(&mut self, message: StreamStateMsg) -> <StreamStateMsg as Message>::Result {
        self.handle_stream_state(message)
    }
}

impl Actor for MockResolver {
    type Envelope = ResolverEnvelope;
}
impl ResolverActor for MockResolver {}

impl MockResolver {
    /// Spawn a mock aggregator actor.
    pub fn spawn(mock_actor: MockResolver) -> ResolverHandle {
        let metrics = Metrics::register(&mut Registry::default());
        let (handle, _task_handle) =
            Resolver::spawn(1_000, mock_actor, metrics, std::future::pending());
        handle
    }
}
