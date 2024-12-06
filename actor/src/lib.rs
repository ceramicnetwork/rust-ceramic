//! Actor provides a lightweight actor framework based on tokio.
//!
#![warn(missing_docs)]

mod macros;
pub use ceramic_actor_macros::*;

pub use tracing;

use tokio::sync::{mpsc, oneshot};

use async_trait::async_trait;
use tracing::{debug_span, Span};

/// A message that can be sent to an actor.
pub trait Message: 'static + Sync + Send + Sized {
    /// The type of results from this message.
    type Result: 'static + Sync + Send;

    /// Returns the messages' type name string
    fn type_name() -> &'static str {
        std::any::type_name::<Self>()
    }
}

/// Handle a type of [`Message`] for an [`Actor`].
#[async_trait]
pub trait Handler<M: Message>
where
    Self: Actor,
{
    /// Handle the message producing its result.
    async fn handle(&mut self, message: M) -> M::Result;
}

/// Actors handle messages and produce their results.
pub trait Actor: 'static + Send + Sync {
    /// Enumeration of all possible messages the actor can handle.
    type Envelope: Send + Sync + 'static;

    /// Returns the actor's type name string
    fn type_name() -> &'static str
    where
        Self: Sized,
    {
        std::any::type_name::<Self>()
    }
}

/// Wrapper of any T with its tracing span context.
pub struct Traced<T> {
    /// Wrapped value.
    pub value: T,
    /// Originating span.
    pub span: Span,
}

/// A handle to a running [`Actor`].
/// Used to send/notify the actor of messages.
#[async_trait]
pub trait ActorHandle<A>: Send + Sync + 'static
where
    A: Actor,
{
    /// Produce a channel on which messages can be sent.
    fn sender(&self) -> Sender<A::Envelope>;

    /// Notify the actor of the message. Do not wait for result of the message.
    async fn notify<Msg>(&self, msg: Msg) -> Result<(), ()>
    where
        Msg: Message + Send + 'static,
        A::Envelope: From<(Msg, oneshot::Sender<Msg::Result>)>,
        A: Handler<Msg>,
    {
        let span = debug_span!(
            "notify",
            actor_type = A::type_name(),
            message_type = Msg::type_name()
        );
        let sender = self.sender();
        let (tx, _rx) = oneshot::channel();
        sender
            .send(DeliverOp::Notify(Traced {
                value: (msg, tx).into(),
                span,
            }))
            .await
            .unwrap();
        Ok(())
    }

    /// Send the actor a message waiting for the result.
    async fn send<Msg>(&self, msg: Msg) -> Result<Msg::Result, ()>
    where
        Msg: Message + Send + 'static,
        A::Envelope: From<(Msg, oneshot::Sender<Msg::Result>)>,
        A: Handler<Msg>,
    {
        let span = debug_span!(
            "send",
            actor_type = A::type_name(),
            message_type = Msg::type_name()
        );
        let sender = self.sender();
        let (tx, rx) = oneshot::channel();
        sender
            .send(DeliverOp::Send(Traced {
                value: (msg, tx).into(),
                span,
            }))
            .await
            .unwrap();
        Ok(rx.await.unwrap())
    }
}

/// Operation used to deliver a message to an actor.
pub enum DeliverOp<E> {
    /// Send the message and wait for the result.
    Send(E),
    /// Send the message and do not wait for the result
    Notify(E),
}

/// Receiver of actor messages.
pub struct Receiver<E>(mpsc::Receiver<DeliverOp<Traced<E>>>);
/// Sender of actor messages.
pub struct Sender<E>(mpsc::Sender<DeliverOp<Traced<E>>>);

/// Construct a buffered channel of actor messages.
pub fn channel<E>(buffer: usize) -> (Sender<E>, Receiver<E>) {
    let (tx, rx) = mpsc::channel(buffer);
    (Sender(tx), Receiver(rx))
}

impl<E> Clone for Sender<E> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<E> Receiver<E> {
    /// Receives the next value from this receiver.
    /// See [`tokio::mpsc::Receiver::recv`] for more details.
    pub async fn recv(&mut self) -> Option<DeliverOp<Traced<E>>> {
        self.0.recv().await
    }
}
impl<E> Sender<E> {
    async fn send(
        &self,
        value: DeliverOp<Traced<E>>,
    ) -> Result<(), mpsc::error::SendError<DeliverOp<Traced<E>>>> {
        self.0.send(value).await
    }
}
