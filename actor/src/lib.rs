//! Actor provides a lightweight actor framework based on tokio.
//!
//#![warn(missing_docs)]

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

    fn receiver(&mut self) -> Receiver<Self::Envelope>;

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

pub struct Traced<T> {
    pub value: T,
    pub span: Span,
}

#[async_trait]
pub trait ActorRef<A>: Send + Sync + 'static
where
    A: Actor,
{
    fn sender(&self) -> Sender<A::Envelope>;

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
        //TODO can we avoid creating this at all?
        let (tx, rx) = oneshot::channel();
        sender
            .send(DeliverOp::Notify(Traced {
                value: (msg, tx).into(),
                span,
            }))
            .await
            .unwrap();
        Ok(())
    }

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

pub enum DeliverOp<E> {
    Send(E),
    Notify(E),
}

pub struct Receiver<E>(mpsc::Receiver<DeliverOp<Traced<E>>>);
pub struct Sender<E>(mpsc::Sender<DeliverOp<Traced<E>>>);

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
