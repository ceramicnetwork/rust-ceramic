//! Actor provides a lightweight actor framework based on tokio.
//!
#![warn(missing_docs)]
pub mod macros;

use std::{future::Future, pin::Pin};

use tokio::sync::{mpsc, oneshot};

use async_trait::async_trait;
use tracing::{debug_span, Span};

pub trait Message: 'static + Sync + Send + Sized {
    type Result: 'static + Sync + Send;

    /// Returns the messages' type name string
    fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }
}

#[async_trait]
pub trait Handler<M: Message>
where
    Self: Actor,
{
    async fn handle(&mut self, message: M) -> M::Result;
}

pub trait Actor: 'static + Send + Sync {
    type Envelope: Send + Sync + 'static;

    fn receiver(&mut self) -> mpsc::Receiver<TracedMessage<Self::Envelope>>;

    /// Returns the actor's type name string
    fn type_name() -> &'static str
    where
        Self: Sized,
    {
        std::any::type_name::<Self>()
    }
}

#[async_trait]
pub trait SendActor<A, Msg>: Send + Sync + 'static
where
    A: Handler<Msg>,
    Msg: Message,
{
    /// Send a message to the actor and wait for the response.
    async fn send(&self, msg: Msg) -> Result<Msg::Result, ()>;
    /// Create a span context for the message
    fn span(&self) -> Span {
        debug_span!(
            "send",
            actor_type = A::type_name(),
            message_type = Msg::type_name()
        )
    }
}

#[async_trait]
pub trait NotifyActor<A, Msg>: Send + Sync + 'static
where
    A: Handler<Msg>,
    Msg: Message,
{
    /// Notify the actor with a message and do not wait for the response.
    async fn notify(&self, msg: Msg) -> Result<(), ()>;
    /// Create a span context for the message
    fn span(&self) -> Span {
        debug_span!(
            "notify",
            actor_type = A::type_name(),
            message_type = Msg::type_name()
        )
    }
}

// TODO hide behind mailbox type
pub struct TracedMessage<M> {
    pub msg: M,
    pub span: Span,
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

pub trait ActorRef<A>: Send + Sync + 'static
where
    A: Actor,
{
    fn sender(&self) -> mpsc::Sender<TracedMessage<A::Envelope>>;

    fn notify<Msg>(&self, msg: Msg) -> BoxFuture<Result<(), ()>>
    where
        Msg: Message + Send + 'static,
        Msg: Into<A::Envelope>,
        A: Handler<Msg>,
    {
        let sender = self.sender();
        let span = self.notify_span::<Msg>();
        Box::pin(async move {
            sender
                .send(TracedMessage {
                    msg: msg.into(),
                    span,
                })
                .await
                .unwrap();
            Ok(())
        })
    }

    fn send<Msg>(&self, msg: Msg) -> BoxFuture<Result<Msg::Result, ()>>
    where
        Msg: Message + Send + 'static,
        A::Envelope: From<(Msg, oneshot::Sender<Msg::Result>)>,
        A: Handler<Msg>,
    {
        let sender = self.sender();
        let span = self.send_span::<Msg>();
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();
            sender
                .send(TracedMessage {
                    msg: (msg, tx).into(),
                    span,
                })
                .await
                .unwrap();
            Ok(rx.await.unwrap())
        })
    }

    fn notify_span<Msg>(&self) -> Span
    where
        Msg: Message,
    {
        debug_span!(
            "notify",
            actor_type = A::type_name(),
            message_type = Msg::type_name()
        )
    }
    fn send_span<Msg>(&self) -> Span
    where
        Msg: Message,
    {
        debug_span!(
            "notify",
            actor_type = A::type_name(),
            message_type = Msg::type_name()
        )
    }
}
