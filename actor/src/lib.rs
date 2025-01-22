//! Actor provides a lightweight actor framework based on tokio.
//!
//! Features:
//!  - In memory message passing
//!  - Strongly typed
//!  - tracing spans are preserved from caller to actor
//!  - first class support for recording metrics about messages
//!
//! # Example
//! ```
#![doc = include_str!("../examples/game/main.rs")]
#![doc = "```"]
#![warn(missing_docs)]

mod macros;
use std::sync::Arc;

pub use ceramic_actor_macros::*;

use ceramic_metrics::Recorder;
pub use tracing;

use snafu::prelude::*;
use tokio::sync::{mpsc, oneshot};

use async_trait::async_trait;
use tracing::{debug_span, Span};

/// A message that can be sent to an actor.
pub trait Message: 'static + Send + Sized {
    /// The type of results from this message.
    type Result: 'static + Send;

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
pub trait Actor {
    /// Enumeration of all possible messages the actor can handle.
    type Envelope: Send + 'static;

    /// Returns the actor's type name string
    fn type_name() -> &'static str
    where
        Self: Sized,
    {
        std::any::type_name::<Self>()
    }
}

/// Wrapper of any T with its tracing span context.
#[derive(Debug)]
pub struct Traced<T> {
    /// Wrapped value.
    pub value: T,
    /// Originating span.
    pub span: Span,
}

impl<T> Traced<T> {
    fn into_inner(self) -> T {
        self.value
    }
}

/// A handle to a running [`Actor`].
/// Used to send/notify the actor of messages.
#[async_trait]
pub trait ActorHandle: Clone + Send + Sync + 'static {
    /// The actor to which self is a handle.
    type Actor: Actor;

    /// Produce a channel on which messages can be sent.
    fn sender(&self) -> Sender<<Self::Actor as Actor>::Envelope>;

    /// Notify the actor of the message. Do not wait for result of the message.
    async fn notify<Msg, R>(&self, msg: Msg, recorder: Arc<R>) -> Result<(), Error<Msg>>
    where
        Msg: Message + TryFrom<<Self::Actor as Actor>::Envelope> + std::fmt::Debug + Send + 'static,
        <Msg as TryFrom<<Self::Actor as Actor>::Envelope>>::Error: std::fmt::Debug,
        <Self::Actor as Actor>::Envelope: From<(Msg, oneshot::Sender<Msg::Result>)>,
        R: Recorder<MessageEvent<Msg>> + Send + Sync + ?Sized + 'static,
    {
        let event = MessageEvent {
            message: msg,
            actor_type: Self::Actor::type_name(),
            message_type: Msg::type_name(),
        };
        recorder.record(&event);
        let span = debug_span!(
            "notify",
            actor_type = Self::Actor::type_name(),
            message_type = Msg::type_name()
        );
        let sender = self.sender();
        let (tx, _rx) = oneshot::channel();
        sender
            .send(DeliverOp::Notify(Traced {
                value: <Self::Actor as Actor>::Envelope::from((event.message, tx)),
                span,
            }))
            .await
            .map_err(|err| {
                mpsc::error::SendError(
                    Msg::try_from(err.0.into_inner().into_inner())
                        .expect("should be able to extract the message from the envelope"),
                )
            })
            .context(SendSnafu)?;
        Ok(())
    }

    /// Send the actor a message waiting for the result.
    /// Additionally record the message event.
    async fn send<Msg, R>(&self, msg: Msg, recorder: Arc<R>) -> Result<Msg::Result, Error<Msg>>
    where
        Msg: Message + TryFrom<<Self::Actor as Actor>::Envelope> + std::fmt::Debug + Send + 'static,
        <Msg as TryFrom<<Self::Actor as Actor>::Envelope>>::Error: std::fmt::Debug,
        <Self::Actor as Actor>::Envelope: From<(Msg, oneshot::Sender<Msg::Result>)>,
        R: Recorder<MessageEvent<Msg>> + Send + Sync + ?Sized + 'static,
    {
        let event = MessageEvent {
            message: msg,
            actor_type: Self::Actor::type_name(),
            message_type: Msg::type_name(),
        };
        recorder.record(&event);
        let span = debug_span!(
            "send",
            actor_type = Self::Actor::type_name(),
            message_type = Msg::type_name()
        );
        let sender = self.sender();
        let (tx, rx) = oneshot::channel();
        sender
            .send(DeliverOp::Send(Traced {
                value: <Self::Actor as Actor>::Envelope::from((event.message, tx)),
                span,
            }))
            .await
            .map_err(|err| {
                mpsc::error::SendError(
                    Msg::try_from(err.0.into_inner().into_inner())
                        .expect("should be able to extract message from the envelope"),
                )
            })
            .context(SendSnafu)?;
        let result = rx.await.context(RecvSnafu)?;
        Ok(result)
    }
}

/// Operation used to deliver a message to an actor.
#[derive(Debug)]
pub enum DeliverOp<E> {
    /// Send the message and wait for the result.
    Send(E),
    /// Send the message and do not wait for the result
    Notify(E),
}

impl<E> DeliverOp<E> {
    fn into_inner(self) -> E {
        match self {
            DeliverOp::Send(e) => e,
            DeliverOp::Notify(e) => e,
        }
    }
}

/// Receiver of actor messages.
#[derive(Debug)]
pub struct Receiver<E>(mpsc::Receiver<DeliverOp<Traced<E>>>);
/// Sender of actor messages.
#[derive(Debug)]
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
    /// See [`tokio::sync::mpsc::Receiver::recv`] for more details.
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

/// Errors encountered when communicating with actors.
#[derive(Debug, Snafu)]
pub enum Error<T: 'static> {
    /// Failed to send message to actor
    Send {
        /// Contains the original message that failed to send.
        #[snafu(source)]
        message: mpsc::error::SendError<T>,
    },
    /// Failed to receive message from actor
    Recv {
        /// The underlying receive error
        source: oneshot::error::RecvError,
    },
}

/// Metadata about a message used in recording metrics about the occurrence of a message.
pub struct MessageEvent<M> {
    /// The message itself.
    pub message: M,
    /// The name of the type actor handling the message.
    pub actor_type: &'static str,
    /// The type of the message.
    pub message_type: &'static str,
}
