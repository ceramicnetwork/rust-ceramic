//! Protocol provides a means to synchronize two Recon peers.
//! Synchronization is pairwise, always between two nodes, where one is the initiator and the other the responder.
//!
//! Generally the initiator makes requests and the responder responds.
//! However its possible the responder requests values if it learns about new keys.
//!
//! The API of this module is a type that implements Sink + Stream in order to create a full duplex
//! channel of communication between two peers. See [`initiate_synchronize`] and
//! [`respond_synchronize`] for details.
//!
//! Encoding and framing of messages is outside the scope of this crate.
//! However the message types do implement serde::Serialize and serde::Deserialize.
use std::pin::Pin;

use anyhow::{Context, Result};
use async_trait::async_trait;
use ceramic_core::RangeOpen;
use ceramic_metrics::Recorder;
use futures::{Sink, SinkExt, Stream, TryStreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::mpsc::{channel, Receiver, Sender},
};
use tracing::{trace, Level};

use crate::{
    metrics::{
        MessageLabels, MessageRecv, MessageSent, Metrics, ProtocolLoop, ProtocolRun, RangeDequeued,
        RangeEnqueueFailed, RangeEnqueued, WantDequeued, WantEnqueueFailed, WantEnqueued,
    },
    recon::{Range, SyncState},
    AssociativeHash, Client, Key,
};

// Number of want value requests to buffer.
const WANT_VALUES_BUFFER: usize = 10000;
// Number of message to buffer on the sink before flushing.
const SINK_BUFFER_COUNT: usize = 100;
// Limit to the number of pending range requests.
// Range requets grow logistically, meaning they grow
// exponentially while splitting and then decay
// exponentially when they discover in sync sections.
//
// In order to ensure we do not have too much pending work in
// progress at any moment we place a limit.
// A higher limit means more concurrent work is getting done between peers.
// Too high of a limit means peers can deadlock because each peer can be
// trying to write to the network while neither is reading.
//
// Even a small limit will quickly mean that both peers have work to do.
const PENDING_RANGES_LIMIT: usize = 20;

/// Intiate Recon synchronization with a peer over a stream.
#[tracing::instrument(skip(recon, stream), ret(level = Level::DEBUG))]
pub async fn initiate_synchronize<S, R, E>(recon: R, stream: S) -> Result<()>
where
    R: Recon,
    S: Stream<Item = Result<ResponderMessage<R::Key, R::Hash>, E>>
        + Sink<InitiatorMessage<R::Key, R::Hash>, Error = E>
        + Send,
    E: std::error::Error + Send + Sync + 'static,
{
    let (tx_want_values, rx_want_values) = channel(WANT_VALUES_BUFFER);
    let metrics = recon.metrics();
    let protocol = Protocol::new(
        Initiator::new(stream, recon, tx_want_values),
        rx_want_values,
        metrics,
    );
    protocol.run().await?;
    Ok(())
}
/// Respond to an initiated Recon synchronization with a peer over a stream.
#[tracing::instrument(skip(recon, stream), ret(level = Level::DEBUG))]
pub async fn respond_synchronize<S, R, E>(recon: R, stream: S) -> Result<()>
where
    R: Recon,
    S: Stream<Item = std::result::Result<InitiatorMessage<R::Key, R::Hash>, E>>
        + Sink<ResponderMessage<R::Key, R::Hash>, Error = E>
        + Send,
    E: std::error::Error + Send + Sync + 'static,
{
    let (tx_want_values, rx_want_values) = channel(WANT_VALUES_BUFFER);
    let metrics = recon.metrics();
    let protocol = Protocol::new(
        Responder::new(stream, recon, tx_want_values),
        rx_want_values,
        metrics,
    );
    protocol.run().await?;
    Ok(())
}

/// Message that the initiator produces
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum InitiatorMessage<K: Key, H: AssociativeHash> {
    /// Declares interestes to the responder.
    InterestRequest(Vec<RangeOpen<K>>),
    /// Request to synchronize a range.
    RangeRequest(Range<K, H>),
    /// Request a value from the responder.
    ValueRequest(K),
    /// Send a value to the responder.
    ValueResponse(ValueResponse<K>),
    /// Inform the responder we are done sending requests.
    /// The initiator will continue to respond to any incoming requests.
    ListenOnly,
    /// Inform the responder we have processed all their requests.
    /// This is always the last message sent.
    Finished,
}

/// Message that the responder produces
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ResponderMessage<K: Key, H: AssociativeHash> {
    /// Respond with the intersection of interests
    InterestResponse(Vec<RangeOpen<K>>),
    /// Respond to a range request with the same range or splits according to sync status.
    RangeResponse(Vec<Range<K, H>>),
    /// Request a value from the initiator
    ValueRequest(K),
    /// Send a value to the initiator
    ValueResponse(ValueResponse<K>),
    /// Inform the initiator we are done sending requests.
    /// The responder will continue to respond to any incoming requests.
    ListenOnly,
}

/// Container for a key and its value
#[derive(Clone, Serialize, Deserialize)]
pub struct ValueResponse<K> {
    pub(crate) key: K,
    pub(crate) value: Vec<u8>,
}

impl<K> std::fmt::Debug for ValueResponse<K>
where
    K: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueResponse")
            .field("key", &self.key)
            .field("value.len", &self.value.len())
            .finish()
    }
}

// Protocol manages the state machine of the protocol, delegating to a role implementation.
struct Protocol<R: Role> {
    role: R,

    rx_want_values: Receiver<R::Key>,

    listen_only_sent: bool,
    remote_done: bool,
    want_values_done: bool,

    metrics: Metrics,
}

impl<R, E> Protocol<R>
where
    R: Role,
    MessageLabels: for<'a> From<&'a R::In>,
    R::Stream: Stream<Item = Result<R::In, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    fn new(role: R, rx_want_values: Receiver<R::Key>, metrics: Metrics) -> Self {
        Self {
            role,
            rx_want_values,
            listen_only_sent: false,
            remote_done: false,
            want_values_done: false,
            metrics,
        }
    }
    async fn run(mut self) -> Result<()> {
        self.role
            .init()
            .await
            .context("initializing protocol loop")?;

        loop {
            trace!(
                self.listen_only_sent,
                self.remote_done,
                self.want_values_done,
                "iter"
            );

            self.metrics.record(&ProtocolLoop);
            let stream = self.role.stream();
            select! {
                biased;
                message = stream.try_next() => {
                    if let Some(message) = message? {
                        self.metrics.record(&MessageRecv(&message));
                        self.handle_incoming(message).await.context("handle incoming")?;
                    }
                }
                // Send want value requests
                key = self.rx_want_values.recv(), if !self.want_values_done => {
                    if let Some(key) = key {
                        self.metrics.record(&WantDequeued);
                        self.role.feed_want_value(key).await.context("feed want value")?;
                    } else {
                        self.want_values_done = true;
                    }
                }
            }
            self.role.each().await?;

            if self.want_values_done && self.role.is_done() {
                if !self.listen_only_sent {
                    self.listen_only_sent = true;
                    self.role
                        .send_listen_only()
                        .await
                        .context("sending listen only")?;
                }
                if self.remote_done {
                    break;
                }
            }
        }

        self.role
            .finish()
            .await
            .context("finishing protocol loop")?;

        self.role.close().await.context("closing stream")?;
        self.metrics.record(&ProtocolRun);
        Ok(())
    }

    async fn handle_incoming(&mut self, message: R::In) -> Result<()> {
        match self.role.handle_incoming(message).await? {
            RemoteStatus::Active => {}
            RemoteStatus::ListenOnly => {
                // Remote will no longer send ranges, therefore local will no longer enqueue any
                // new wants. We close the channels to signal they will not grow and can be
                // drained.
                self.rx_want_values.close();
            }
            RemoteStatus::Finished => {
                self.remote_done = true;
            }
        }
        Ok(())
    }
}

// Role represents a specific behavior within the overal protocol state machine
// There are two roles, Initiator and Responder.
#[async_trait]
trait Role {
    type In;
    type Key;
    type Stream;

    // Borrow the stream so we can read from it.
    fn stream(&mut self) -> &mut Pin<Box<Self::Stream>>;

    // Do work before the main event loop.
    async fn init(&mut self) -> Result<()>;
    // Do work within each event loop.
    // This is called only after the main select has resovled.
    // This allows pending work to make progress without competing with the main event loop.
    async fn each(&mut self) -> Result<()>;
    // Do work after the main event loop has finished.
    async fn finish(&mut self) -> Result<()>;
    // Close the stream + sink down.
    async fn close(&mut self) -> Result<()>;

    // Report if we are expecting more incoming messages.
    fn is_done(&self) -> bool;

    // Handle an incoming message from the remote.
    async fn handle_incoming(&mut self, message: Self::In) -> Result<RemoteStatus>;

    // Send a mesage to the remote indicating we are only listening.
    async fn send_listen_only(&mut self) -> Result<()>;
    // Feed a want value request to the remote.
    async fn feed_want_value(&mut self, key: Self::Key) -> Result<()>;
}

type InitiatorValueResponseFn<K, H> = fn(ValueResponse<K>) -> InitiatorMessage<K, H>;

// Initiator implements the Role that starts the synchronize conversation.
struct Initiator<R, S>
where
    R: Recon,
{
    common: Common<R, S, InitiatorValueResponseFn<R::Key, R::Hash>>,

    // Use a stack for buffered ranges as this ensures we traverse depth first
    // through the key space tree.
    ranges_stack: Vec<Range<R::Key, R::Hash>>,
    pending_ranges: usize,

    metrics: Metrics,
}

impl<R, S, E> Initiator<R, S>
where
    R: Recon,
    S: Stream<Item = Result<ResponderMessage<R::Key, R::Hash>, E>>
        + Sink<InitiatorMessage<R::Key, R::Hash>, Error = E>,
    E: std::error::Error + Send + Sync + 'static,
{
    fn new(stream: S, recon: R, tx_want_values: Sender<R::Key>) -> Self {
        let metrics = recon.metrics();
        let stream = SinkFlusher::new(stream, metrics.clone());
        // Use a stack size large enough to handle the split factor of range requests.
        let ranges_stack = Vec::with_capacity(PENDING_RANGES_LIMIT * 10);

        Self {
            common: Common {
                stream,
                recon,
                value_resp_fn: InitiatorMessage::ValueResponse,
                tx_want_values,
                metrics: metrics.clone(),
            },
            ranges_stack,
            pending_ranges: 0,
            metrics,
        }
    }

    async fn process_range(&mut self, remote_range: Range<R::Key, R::Hash>) -> Result<()> {
        let (sync_state, new_keys) = self.common.recon.process_range(remote_range).await?;
        self.common.process_new_keys(&new_keys);
        match sync_state {
            SyncState::Synchronized => {
                // TODO: This is where we can append this range to a queue of synced ranges to
                // later process and ensure we have all values for this range.
            }
            SyncState::RemoteMissing { range } => {
                self.common.process_remote_missing_range(&range).await?;
            }
            SyncState::Unsynchronized { ranges } => {
                if self.pending_ranges < PENDING_RANGES_LIMIT {
                    self.pending_ranges += ranges.len();
                    self.common
                        .stream
                        .send_all(
                            ranges
                                .into_iter()
                                .map(InitiatorMessage::RangeRequest)
                                .collect(),
                        )
                        .await?;
                } else {
                    for range in ranges {
                        if self.ranges_stack.len() < self.ranges_stack.capacity() {
                            self.ranges_stack.push(range);
                            self.metrics.record(&RangeEnqueued);
                        } else {
                            self.metrics.record(&RangeEnqueueFailed);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<R, S, E> Role for Initiator<R, S>
where
    R: Recon,
    S: Stream<Item = std::result::Result<ResponderMessage<R::Key, R::Hash>, E>>
        + Sink<InitiatorMessage<R::Key, R::Hash>, Error = E>
        + Send,
    E: std::error::Error + Send + Sync + 'static,
{
    type In = ResponderMessage<R::Key, R::Hash>;
    type Key = R::Key;
    type Stream = S;

    fn stream(&mut self) -> &mut Pin<Box<Self::Stream>> {
        &mut self.common.stream.inner
    }

    async fn init(&mut self) -> Result<()> {
        //  Send interests
        let interests = self.common.recon.interests().await.context("interests")?;
        self.common
            .stream
            .send(InitiatorMessage::InterestRequest(interests))
            .await
            .context("sending interests ")
    }
    async fn each(&mut self) -> Result<()> {
        if self.pending_ranges < PENDING_RANGES_LIMIT {
            if let Some(range) = self.ranges_stack.pop() {
                self.metrics.record(&RangeDequeued);
                self.pending_ranges += 1;
                self.common
                    .stream
                    .send(InitiatorMessage::RangeRequest(range))
                    .await?;
            };
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.common.stream.send(InitiatorMessage::Finished).await
    }
    async fn close(&mut self) -> Result<()> {
        self.common.stream.close().await
    }
    fn is_done(&self) -> bool {
        self.pending_ranges == 0
    }

    async fn handle_incoming(&mut self, message: Self::In) -> Result<RemoteStatus> {
        trace!(?message, "handle_incoming");
        match message {
            ResponderMessage::InterestResponse(interests) => {
                self.pending_ranges += interests.len();
                for interest in interests {
                    let range = self
                        .common
                        .recon
                        .initial_range(interest)
                        .await
                        .context("querying initial range")?;
                    self.common
                        .stream
                        .feed(InitiatorMessage::RangeRequest(range))
                        .await
                        .context("feeding range request")?;
                }
                self.common
                    .stream
                    .flush()
                    .await
                    .context("flushing range requests")?;
                // Handle the case of no interests in common
                if self.is_done() {
                    Ok(RemoteStatus::ListenOnly)
                } else {
                    Ok(RemoteStatus::Active)
                }
            }
            ResponderMessage::RangeResponse(ranges) => {
                self.pending_ranges -= 1;
                for range in ranges {
                    self.process_range(range)
                        .await
                        .context("processing range")?;
                }
                if self.is_done() {
                    Ok(RemoteStatus::ListenOnly)
                } else {
                    Ok(RemoteStatus::Active)
                }
            }
            ResponderMessage::ValueRequest(key) => {
                self.common
                    .process_value_request(key)
                    .await
                    .context("processing value request")?;
                Ok(RemoteStatus::Active)
            }
            ResponderMessage::ValueResponse(ValueResponse { key, value }) => {
                self.common
                    .process_value_response(key, value)
                    .await
                    .context("processing value response")?;
                Ok(RemoteStatus::Active)
            }
            // If the remote (a responder) has sent a listen only then it is finished because it
            // will never initiate a request.
            ResponderMessage::ListenOnly => Ok(RemoteStatus::Finished),
        }
    }

    async fn feed_want_value(&mut self, key: Self::Key) -> Result<()> {
        self.common
            .feed_want_value(InitiatorMessage::ValueRequest(key))
            .await
    }
    async fn send_listen_only(&mut self) -> Result<()> {
        self.common.stream.send(InitiatorMessage::ListenOnly).await
    }
}

type ResponderValueResponseFn<K, H> = fn(ValueResponse<K>) -> ResponderMessage<K, H>;

// Responder implements the [`Role`] where it responds to incoming requests.
struct Responder<R, S>
where
    R: Recon,
{
    common: Common<R, S, ResponderValueResponseFn<R::Key, R::Hash>>,
}

impl<R, S, E> Responder<R, S>
where
    R: Recon,
    S: Stream<Item = std::result::Result<InitiatorMessage<R::Key, R::Hash>, E>>
        + Sink<ResponderMessage<R::Key, R::Hash>, Error = E>,
    E: std::error::Error + Send + Sync + 'static,
{
    fn new(stream: S, recon: R, tx_want_values: Sender<R::Key>) -> Self {
        let metrics = recon.metrics();
        let stream = SinkFlusher::new(stream, metrics.clone());

        Self {
            common: Common {
                stream,
                recon,
                value_resp_fn: ResponderMessage::ValueResponse,
                tx_want_values,
                metrics,
            },
        }
    }

    async fn process_range(&mut self, range: Range<R::Key, R::Hash>) -> Result<()> {
        let (sync_state, new_keys) = self.common.recon.process_range(range.clone()).await?;
        self.common.process_new_keys(&new_keys);
        match sync_state {
            SyncState::Synchronized => {
                // TODO: This is where we can append this range to a queue of synced ranges to
                // later process and ensure we have all values for this range.

                // We are sync echo back the same range so that the remote learns we are in sync.
                self.common
                    .stream
                    .send(ResponderMessage::RangeResponse(vec![range]))
                    .await?;
            }
            SyncState::RemoteMissing { range } => {
                self.common.process_remote_missing_range(&range).await?;
                // Send the range hash after we have sent all keys so the remote learns we are in
                // sync.
                self.common
                    .stream
                    .send(ResponderMessage::RangeResponse(vec![range]))
                    .await?;
            }
            SyncState::Unsynchronized { ranges: splits } => {
                trace!("unsynchronized sending response");
                self.common
                    .stream
                    .send(ResponderMessage::RangeResponse(splits))
                    .await?;
                trace!("unsynchronized sent response");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<R, S, E> Role for Responder<R, S>
where
    R: Recon,
    S: Stream<Item = std::result::Result<InitiatorMessage<R::Key, R::Hash>, E>>
        + Sink<ResponderMessage<R::Key, R::Hash>, Error = E>
        + Send,
    E: std::error::Error + Send + Sync + 'static,
{
    type In = InitiatorMessage<R::Key, R::Hash>;
    type Key = R::Key;
    type Stream = S;

    fn stream(&mut self) -> &mut Pin<Box<Self::Stream>> {
        &mut self.common.stream.inner
    }

    async fn init(&mut self) -> Result<()> {
        Ok(())
    }
    async fn each(&mut self) -> Result<()> {
        Ok(())
    }
    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
    async fn close(&mut self) -> Result<()> {
        self.common.stream.close().await
    }
    fn is_done(&self) -> bool {
        true
    }

    async fn handle_incoming(&mut self, message: Self::In) -> Result<RemoteStatus> {
        trace!(?message, "handle_incoming");
        match message {
            InitiatorMessage::InterestRequest(interests) => {
                let ranges = self.common.recon.process_interests(interests).await?;
                self.common
                    .stream
                    .send(ResponderMessage::InterestResponse(ranges))
                    .await?;
                Ok(RemoteStatus::Active)
            }
            InitiatorMessage::RangeRequest(range) => {
                self.process_range(range).await?;
                Ok(RemoteStatus::Active)
            }
            InitiatorMessage::ValueRequest(key) => {
                self.common.process_value_request(key).await?;
                Ok(RemoteStatus::Active)
            }
            InitiatorMessage::ValueResponse(ValueResponse { key, value }) => {
                self.common.process_value_response(key, value).await?;
                Ok(RemoteStatus::Active)
            }
            InitiatorMessage::ListenOnly => Ok(RemoteStatus::ListenOnly),
            InitiatorMessage::Finished => Ok(RemoteStatus::Finished),
        }
    }
    async fn feed_want_value(&mut self, key: Self::Key) -> Result<()> {
        self.common
            .feed_want_value(ResponderMessage::ValueRequest(key))
            .await
    }
    async fn send_listen_only(&mut self) -> Result<()> {
        self.common.stream.send(ResponderMessage::ListenOnly).await
    }
}

// Common implments common behaviors to both [`Initiator`] and [`Responder`].
struct Common<R: Recon, S, V> {
    stream: SinkFlusher<S>,
    recon: R,

    value_resp_fn: V,

    tx_want_values: Sender<R::Key>,

    metrics: Metrics,
}

impl<R, S, E, In, Out, V> Common<R, S, V>
where
    R: Recon,
    S: Stream<Item = std::result::Result<In, E>> + Sink<Out, Error = E>,
    E: std::error::Error + Send + Sync + 'static,
    V: Fn(ValueResponse<R::Key>) -> Out,
    MessageLabels: for<'a> From<&'a Out>,
{
    fn process_new_keys(&mut self, new_keys: &[R::Key]) {
        for key in new_keys {
            if self.tx_want_values.try_send(key.clone()).is_err() {
                self.metrics.record(&WantEnqueueFailed);
            } else {
                self.metrics.record(&WantEnqueued);
            }
        }
    }

    async fn process_value_request(&mut self, key: R::Key) -> Result<()> {
        // TODO: Measure impact of fetching value inline
        let value = self
            .recon
            .value_for_key(key.clone())
            .await
            .context("value for key")?;
        if let Some(value) = value {
            self.stream
                .feed((self.value_resp_fn)(ValueResponse { key, value }))
                .await
                .context("feeding value response")?;
        }
        Ok(())
    }
    async fn process_value_response(&mut self, key: R::Key, value: Vec<u8>) -> Result<()> {
        self.recon.insert(key.clone()).await.context("store key")?;
        self.recon
            .store_value_for_key(key, &value)
            .await
            .context("store value for key")
    }
    // The remote is missing all keys in the range send them over.
    async fn process_remote_missing_range(&mut self, range: &Range<R::Key, R::Hash>) -> Result<()> {
        // TODO: This logic has two potential failure modes we need to test them
        // 1. We allocate memory of all keys in the range, this can be very large.
        // 2. We spend a lot of time writing out to the stream but not reading from the stream.
        //    This can be a potential deadlock if both side enter this method for a large amount of
        //    keys at the same time.
        let keys = self
            .recon
            .range(range.first.clone(), range.last.clone(), 0, usize::MAX)
            .await?;
        for key in keys {
            if let Some(value) = self.recon.value_for_key(key.clone()).await? {
                self.stream
                    .feed((self.value_resp_fn)(ValueResponse { key, value }))
                    .await?;
            }
        }
        self.stream.flush().await?;
        Ok(())
    }
    async fn feed_want_value(&mut self, message: Out) -> Result<()> {
        self.stream
            .feed(message)
            .await
            .context("feeding value request")?;
        Ok(())
    }
}

enum RemoteStatus {
    // The remote is still actively sending requests.
    Active,
    // The remote will no longer send any new requests but will respond.
    ListenOnly,
    // The remote will no longer send any messages.
    Finished,
}

// Wrapper around a sink that flushes at least every [`SINK_BUFFER_COUNT`].
struct SinkFlusher<S> {
    inner: Pin<Box<S>>,
    feed_count: usize,
    metrics: Metrics,
}

impl<S> SinkFlusher<S> {
    fn new<T>(stream: S, metrics: Metrics) -> Self
    where
        S: Sink<T>,
    {
        let stream = Box::pin(stream);
        Self {
            inner: stream,
            feed_count: 0,
            metrics,
        }
    }
    async fn send<T, E>(&mut self, message: T) -> Result<()>
    where
        S: Sink<T, Error = E>,
        E: std::error::Error + Send + Sync + 'static,
        MessageLabels: for<'a> From<&'a T>,
    {
        self.metrics.record(&MessageSent(&message));
        self.inner.send(message).await?;
        self.feed_count = 0;
        Ok(())
    }
    async fn send_all<T, E>(&mut self, messages: Vec<T>) -> Result<()>
    where
        S: Sink<T, Error = E>,
        E: std::error::Error + Send + Sync + 'static,
        MessageLabels: for<'a> From<&'a T>,
    {
        for message in messages {
            self.metrics.record(&MessageSent(&message));
            self.inner.feed(message).await?;
        }
        self.inner.flush().await?;
        self.feed_count = 0;
        Ok(())
    }
    async fn feed<T, E>(&mut self, message: T) -> Result<()>
    where
        S: Sink<T, Error = E>,
        E: std::error::Error + Send + Sync + 'static,
        MessageLabels: for<'a> From<&'a T>,
    {
        self.feed_count += 1;
        self.metrics.record(&MessageSent(&message));
        self.inner.feed(message).await?;
        if self.feed_count > SINK_BUFFER_COUNT {
            self.feed_count = 0;
            self.flush().await?;
        }
        Ok(())
    }
    async fn flush<T, E>(&mut self) -> Result<()>
    where
        S: Sink<T, Error = E>,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.inner.flush().await.context("flushing")
    }
    async fn close<T, E>(&mut self) -> Result<()>
    where
        S: Sink<T, Error = E>,
        E: std::error::Error + Send + Sync + 'static,
    {
        self.inner.close().await.context("closing")
    }
}

/// Defines the Recon API.
#[async_trait]
pub trait Recon: Clone + Send + Sync + 'static {
    /// The type of Key to communicate.
    type Key: Key + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>;
    /// The type of Hash to compute over the keys.
    type Hash: AssociativeHash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>;

    /// Insert a new key into the key space.
    async fn insert(&self, key: Self::Key) -> Result<()>;

    /// Get all keys in the specified range
    async fn range(
        &self,
        left_fencepost: Self::Key,
        right_fencepost: Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Self::Key>>;

    /// Reports total number of keys
    async fn len(&self) -> Result<usize>;

    /// Reports if the set is empty.
    async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// retrieve a value associated with a recon key
    async fn value_for_key(&self, key: Self::Key) -> Result<Option<Vec<u8>>>;

    /// associate a value with a recon key
    async fn store_value_for_key(&self, key: Self::Key, value: &[u8]) -> Result<()>;

    /// Reports the interests of this recon instance
    async fn interests(&self) -> Result<Vec<RangeOpen<Self::Key>>>;

    /// Computes the intersection of input interests with the local interests
    async fn process_interests(
        &self,
        interests: Vec<RangeOpen<Self::Key>>,
    ) -> Result<Vec<RangeOpen<Self::Key>>>;

    /// Compute an initial hash for the range
    async fn initial_range(
        &self,
        interest: RangeOpen<Self::Key>,
    ) -> Result<Range<Self::Key, Self::Hash>>;

    /// Computes a response to a remote range
    async fn process_range(
        &self,
        range: Range<Self::Key, Self::Hash>,
    ) -> Result<(SyncState<Self::Key, Self::Hash>, Vec<Self::Key>)>;

    /// Create a handle to the metrics
    fn metrics(&self) -> Metrics;
}

#[async_trait]
impl<K, H> Recon for Client<K, H>
where
    K: Key + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>,
    H: AssociativeHash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>,
{
    type Key = K;
    type Hash = H;

    async fn insert(&self, key: Self::Key) -> Result<()> {
        let _ = Client::insert(self, key).await?;
        Ok(())
    }

    async fn range(
        &self,
        left_fencepost: Self::Key,
        right_fencepost: Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Self::Key>> {
        Ok(
            Client::range(self, left_fencepost, right_fencepost, offset, limit)
                .await?
                .collect(),
        )
    }

    async fn len(&self) -> Result<usize> {
        Client::len(self).await
    }

    async fn value_for_key(&self, key: Self::Key) -> Result<Option<Vec<u8>>> {
        Client::value_for_key(self, key).await
    }
    async fn store_value_for_key(&self, key: Self::Key, value: &[u8]) -> Result<()> {
        Client::store_value_for_key(self, key, value).await
    }
    async fn interests(&self) -> Result<Vec<RangeOpen<Self::Key>>> {
        Client::interests(self).await
    }
    async fn process_interests(
        &self,
        interests: Vec<RangeOpen<Self::Key>>,
    ) -> Result<Vec<RangeOpen<Self::Key>>> {
        Client::process_interests(self, interests).await
    }

    async fn initial_range(
        &self,
        interest: RangeOpen<Self::Key>,
    ) -> Result<Range<Self::Key, Self::Hash>> {
        Client::initial_range(self, interest).await
    }

    async fn process_range(
        &self,
        range: Range<Self::Key, Self::Hash>,
    ) -> Result<(SyncState<Self::Key, Self::Hash>, Vec<Self::Key>)> {
        Client::process_range(self, range).await
    }
    fn metrics(&self) -> Metrics {
        Client::metrics(self)
    }
}

#[cfg(test)]
mod tests {
    use expect_test::expect;

    use crate::{tests::AlphaNumBytes, Sha256a};

    use super::*;

    #[test]
    fn message_serializes() {
        let msg = InitiatorMessage::RangeRequest(Range {
            first: AlphaNumBytes::min_value(),
            hash: Sha256a::digest(&AlphaNumBytes::from("hello world")).into(),
            last: AlphaNumBytes::max_value(),
        });

        let cbor_hex = hex::encode(serde_cbor::to_vec(&msg).unwrap());
        expect!["a16c52616e676552657175657374a3656669727374406468617368a264686173685820b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde965636f756e7400646c61737441ff"].assert_eq(&cbor_hex);
    }

    #[test]
    fn message_serializes_with_small_zero_hash() {
        let msg = InitiatorMessage::RangeRequest(Range {
            first: AlphaNumBytes::min_value(),
            hash: Sha256a::identity().into(),
            last: AlphaNumBytes::max_value(),
        });

        let cbor_hex = hex::encode(serde_cbor::to_vec(&msg).unwrap());
        expect!["a16c52616e676552657175657374a3656669727374406468617368a264686173684065636f756e7400646c61737441ff"].assert_eq(&cbor_hex);
    }
}
