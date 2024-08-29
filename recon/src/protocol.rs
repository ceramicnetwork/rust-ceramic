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

use anyhow::{anyhow, bail, Context, Result};
use async_stream::try_stream;
use async_trait::async_trait;
use ceramic_core::RangeOpen;
use ceramic_metrics::Recorder;
use futures::{pin_mut, stream::BoxStream, Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, time::Instant};
use tokio_stream::once;
use tracing::{instrument, trace, Level};
use uuid::Uuid;

use crate::{
    metrics::{
        MessageLabels, MessageRecv, MessageSent, Metrics, PendingEvents, ProtocolRun,
        ProtocolWriteLoop,
    },
    recon::{RangeHash, SyncState},
    AssociativeHash, InsertResult, Key, ReconItem, Result as ReconResult,
};

// Limit to the number of pending range requests.
// Range requets grow logistically, meaning they grow
// exponentially while splitting and then decay
// exponentially when they discover in sync sections.
//
// In order to ensure we do not have too much pending work in
// progress at any moment we place a limit.
// A higher limit means more concurrent work is getting done between peers.
// Too high of a limit means peers can livelock because each peer can be
// spending its resources producing new work instead of completing
// in progress work.
//
// Even a small limit will quickly mean that both peers have work to do.
const PENDING_RANGES_LIMIT: usize = 20;

// The max number of writes we'll batch up before flushing anything to disk.
// As we descend the tree and find smaller ranges, this won't apply as we have to flush
// before recomputing a range, but it will be used when we're processing large ranges we don't yet have.
const INSERT_BATCH_SIZE: usize = 100;

type IM<K, H> = ReconMessage<InitiatorMessage<K, H>>;
type RM<K, H> = ReconMessage<ResponderMessage<K, H>>;

#[derive(Clone, Debug)]
/// Parameters for the protocol that can be used to track the conversation or adjust internal behavior
pub struct ProtocolConfig {
    /// The max number of writes we'll batch up before flushing anything to disk.
    /// As we descend the tree and find smaller ranges, this won't apply as we have to flush
    /// before recomputing a range, but it will be used when we're processing large ranges we don't yet have.
    pub insert_batch_size: usize,
    #[allow(dead_code)]
    /// The ID of the peer we're syncing with.
    peer_id: PeerId,
}

impl ProtocolConfig {
    /// Create an instance of the config
    pub fn new(insert_batch_size: usize, peer_id: PeerId) -> Self {
        Self {
            insert_batch_size,
            peer_id,
        }
    }

    /// Uses the constant defaults defined for batch size (100) and max items (1000)
    pub fn new_peer_id(peer_id: PeerId) -> Self {
        Self {
            insert_batch_size: INSERT_BATCH_SIZE,
            peer_id,
        }
    }
}

/// Intiate Recon synchronization with a peer over a stream.
#[tracing::instrument(skip(recon, stream), ret(level = Level::DEBUG))]
pub async fn initiate_synchronize<S, R, E>(
    recon: R,
    stream: S,
    config: ProtocolConfig,
) -> Result<()>
where
    R: Recon,
    S: Stream<Item = Result<RM<R::Key, R::Hash>, E>>
        + Sink<IM<R::Key, R::Hash>, Error = E>
        + Send
        + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    let metrics = recon.metrics();
    let sync_id = Some(Uuid::new_v4().to_string());
    protocol(sync_id, Initiator::new(recon, config), stream, metrics).await?;
    Ok(())
}
/// Respond to an initiated Recon synchronization with a peer over a stream.
#[tracing::instrument(skip(recon, stream), ret(level = Level::DEBUG))]
pub async fn respond_synchronize<S, R, E>(recon: R, stream: S, config: ProtocolConfig) -> Result<()>
where
    R: Recon,
    S: Stream<Item = std::result::Result<IM<R::Key, R::Hash>, E>>
        + Sink<RM<R::Key, R::Hash>, Error = E>
        + Send
        + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    let metrics = recon.metrics();
    protocol(None, Responder::new(recon, config), stream, metrics).await?;
    Ok(())
}

/// Recon message envelope
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReconMessage<T> {
    /// Sync ID for the conversation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_id: Option<String>,
    /// Recon message
    pub body: T,
}

impl<T> ReconMessage<T> {
    fn new(sync_id: Option<String>, body: T) -> ReconMessage<T> {
        ReconMessage { sync_id, body }
    }
}

/// Message that the initiator produces
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum InitiatorMessage<K: Key, H: AssociativeHash> {
    /// Declares interests to the responder.
    InterestRequest(Vec<RangeOpen<K>>),
    /// Request to synchronize a range.
    RangeRequest(RangeHash<K, H>),
    /// Send a value to the responder.
    Value(Value<K>),
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
    RangeResponse(Vec<RangeHash<K, H>>),
    /// Send a value to the initiator
    Value(Value<K>),
}

/// Container for a key and its value
#[derive(Clone, Serialize, Deserialize)]
pub struct Value<K> {
    pub(crate) key: K,
    pub(crate) value: Vec<u8>,
}

impl<K> std::fmt::Debug for Value<K>
where
    K: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Value")
            .field("key", &self.key)
            .field("value.len", &self.value.len())
            .finish()
    }
}

type ToWriterSender<M> = mpsc::Sender<ToWriter<M>>;

enum ToWriter<M> {
    // SyncId learned from the remote.
    SyncId(String),
    // Send all messages from the stream to the remote.
    SendAll(BoxStream<'static, Result<M>>),
    // Send the messages to the remote as long as its below the work-in-progress limit.
    // Otherwise messages are queued in LIFO order.
    SendWIPLimited(Vec<M>),
    // Mark that in-progress work has completed.
    WIPCompleted(usize),
    // Send the finish message as the final message to the remote.
    // Sender must ensure all in progress work has been completed before sending Finish.
    Finish,
}

// Run the recon protocol conversation to completion.
#[instrument(skip_all)]
async fn protocol<R, S, E>(
    sync_id: Option<String>,
    mut role: R,
    stream: S,
    metrics: Metrics,
) -> Result<()>
where
    R: Role,
    R::Out: std::fmt::Debug + Send + 'static,
    R::In: std::fmt::Debug,
    MessageLabels: for<'a> From<&'a R::Out>,
    MessageLabels: for<'a> From<&'a R::In>,
    S: Stream<Item = Result<ReconMessage<R::In>, E>>
        + Sink<ReconMessage<R::Out>, Error = E>
        + Send
        + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    let start = Instant::now();
    let (sink, stream) = stream.split();
    let (to_writer_tx, to_writer_rx) = mpsc::channel(1000);

    let init = role.init().await?;
    let write = write(
        sync_id.clone(),
        sink.sink_map_err(anyhow::Error::from),
        to_writer_rx,
        init,
        role.finish(),
        PENDING_RANGES_LIMIT,
        metrics.clone(),
    );

    let read = read(sync_id, stream, role, to_writer_tx, metrics.clone());

    // In a recon conversation there are 4 futures being polled:
    //
    //  * Initiator Read
    //  * Initiator Write
    //  * Responder Read
    //  * Responder Write
    //
    //  The following sequence occurs to end the conversation:
    //
    //  1. Initator Read determines there is no more work to do when there are no interests in
    //     common, or it reads the final [`ResponderMessage::RangeResponse`] from the Responder.
    //  2. Initator Read sends [`ToWriter::Finish`] to the Initator Writer.
    //  3. Initiator Writer sends the [`InitiatorMessage::Finished`] to the Responder and
    //     completes.
    //  4. Responder Read receives the Finished message and completes, dropping the
    //     to_writer_tx sender.
    //  5. Responder Write completes because the to_writer_rx has completed. This drops the substream
    //     to the remote which closes it.
    //  6. Initiator Read sees the substream has closed and completes.
    //
    //  This is analogous to the FIN -> FIN ACK sequence in TCP which ensures that boths ends of
    //  the conversation agree it has completed. This prevents a class of bugs where the Initiator
    //  may try and start a new conversation before Responder is aware the previous one has completed.
    let _res = tokio::try_join!(write, read)
        .map_err(|e: anyhow::Error| anyhow!("protocol error: {}", e))?;

    metrics.record(&ProtocolRun(start.elapsed()));
    Ok(())
}

#[instrument(skip_all, fields(sync_id))]
async fn read<R, S, E>(
    mut sync_id: Option<String>,
    stream: S,
    mut role: R,
    mut to_writer_tx: mpsc::Sender<ToWriter<R::Out>>,
    metrics: Metrics,
) -> Result<()>
where
    R: Role,
    R::Out: std::fmt::Debug + Send,
    R::In: std::fmt::Debug,
    MessageLabels: for<'a> From<&'a R::Out>,
    MessageLabels: for<'a> From<&'a R::In>,
    S: Stream<Item = Result<ReconMessage<R::In>, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    if let Some(sync_id) = &sync_id {
        // Record sync_id on the tracing span
        tracing::Span::current().record("sync_id", sync_id);
    }

    pin_mut!(stream);

    // Read from network, until the stream is closed
    while let Some(message) = stream.try_next().await? {
        metrics.record(&MessageRecv(&message.body));
        if sync_id.is_none() {
            if let Some(remote_sync_id) = message.sync_id {
                // Record sync_id on the tracing span
                tracing::Span::current().record("sync_id", &remote_sync_id);
                // Send sync_id to writer task
                to_writer_tx
                    .send(ToWriter::SyncId(remote_sync_id.clone()))
                    .await
                    .map_err(|err| anyhow!("{err}"))
                    .context("sending sync id")?;
                sync_id = Some(remote_sync_id);
            }
        }
        trace!(?message.body, "received message");
        if let RemoteStatus::Finished = role
            .handle_incoming(&mut to_writer_tx, message.body)
            .await
            .context("handle incoming")?
        {
            break;
        }
    }
    Ok(())
}

#[instrument(skip_all, fields(sync_id))]
async fn write<M, S>(
    mut sync_id: Option<String>,
    sink: S,
    mut to_writer_rx: mpsc::Receiver<ToWriter<M>>,
    init: Option<M>,
    finish: Option<M>,
    wip_limit: usize,
    metrics: Metrics,
) -> Result<()>
where
    S: Sink<ReconMessage<M>, Error = anyhow::Error>,
    MessageLabels: for<'a> From<&'a M>,
    M: std::fmt::Debug,
{
    if let Some(sync_id) = &sync_id {
        // Record sync_id on the tracing span
        tracing::Span::current().record("sync_id", sync_id);
    }
    pin_mut!(sink);

    // Use a stack so we get depth first traversal
    let mut message_stack = Vec::new();
    // Track in progress
    let mut in_progress = 0;

    if let Some(init) = init {
        trace!(?init, "sending init");
        metrics.record(&MessageSent(&init));
        sink.send(ReconMessage::new(sync_id.clone(), init)).await?;
    }

    // Write messages to the remote until there are no more messages to send.
    while let Some(action) = to_writer_rx.recv().await {
        metrics.record(&ProtocolWriteLoop);
        match action {
            ToWriter::SyncId(remote_sync_id) => {
                // Record sync_id on the tracing span
                tracing::Span::current().record("sync_id", &remote_sync_id);
                sync_id = Some(remote_sync_id);
            }
            ToWriter::SendAll(messages) => {
                sink.send_all(&mut messages.map(|msg| {
                    msg.map(|message| {
                        metrics.record(&MessageSent(&message));
                        trace!(?message, "sending message");
                        ReconMessage::new(sync_id.clone(), message)
                    })
                }))
                .await?;
            }
            ToWriter::WIPCompleted(count) => {
                in_progress -= count;
            }
            ToWriter::SendWIPLimited(mut messages) => {
                // Send all message that fit within the work in progress limit.
                if in_progress < wip_limit {
                    let capacity = wip_limit - in_progress;
                    let mut sent = false;
                    for message in messages.drain(..std::cmp::min(messages.len(), capacity)) {
                        in_progress += 1;
                        sent = true;
                        trace!(?message, "feeding message");
                        metrics.record(&MessageSent(&message));
                        sink.feed(ReconMessage::new(sync_id.clone(), message))
                            .await?;
                    }
                    if sent {
                        sink.flush().await?;
                    }
                }
                // Queue any extra messages for later in LIFO order.
                message_stack.extend(messages);
            }
            ToWriter::Finish => {
                // We should never get a Finish if all in flight work has not completed.
                debug_assert!(message_stack.is_empty());
                debug_assert_eq!(0, in_progress);
                if let Some(finish) = finish {
                    sink.send(ReconMessage::new(sync_id, finish)).await?;
                }
                break;
            }
        }

        // Send any pending messages from the stack
        let mut sent = false;
        while in_progress < wip_limit {
            if let Some(message) = message_stack.pop() {
                sent = true;
                in_progress += 1;
                trace!(?message, "feeding message");
                metrics.record(&MessageSent(&message));
                sink.feed(ReconMessage::new(sync_id.clone(), message))
                    .await?
            } else {
                break;
            }
        }
        if sent {
            sink.flush().await?;
        }
    }
    Ok(())
}

// Role represents a specific behavior within the overal protocol state machine
// There are two roles, Initiator and Responder.
#[async_trait]
trait Role {
    type In;
    type Out;
    type Key;

    // Compute initial message if any to send
    async fn init(&mut self) -> Result<Option<Self::Out>>;

    // Finish message if any to send when the protocol is complete
    fn finish(&mut self) -> Option<Self::Out>;

    // Handle an incoming message from the remote.
    async fn handle_incoming(
        &mut self,
        to_writer: &mut ToWriterSender<Self::Out>,
        message: Self::In,
    ) -> Result<RemoteStatus>;
}

// Initiator implements the Role that starts the synchronize conversation.
struct Initiator<R>
where
    R: Recon,
{
    common: Common<R>,

    pending_ranges: usize,
}

impl<R> Initiator<R>
where
    R: Recon,
{
    fn new(recon: R, config: ProtocolConfig) -> Self {
        Self {
            common: Common::new(recon, config),
            pending_ranges: 0,
        }
    }

    async fn process_range(
        &mut self,
        to_writer: &mut ToWriterSender<InitiatorMessage<R::Key, R::Hash>>,
        remote_range: RangeHash<R::Key, R::Hash>,
    ) -> Result<()> {
        let sync_state = self.common.recon.process_range(remote_range).await?;
        match sync_state {
            SyncState::Synchronized { .. } => {}
            SyncState::RemoteMissing { ranges, reply_with } => {
                to_writer
                    .send(ToWriter::SendAll(
                        self.common
                            .process_remote_missing_ranges(ranges.clone())
                            .map(|value| value.map(InitiatorMessage::Value))
                            .boxed(),
                    ))
                    .await
                    .map_err(|err| anyhow!("{err}"))
                    .context("sending missing values for ranges")?;

                self.send_ranges(reply_with.into_iter(), to_writer).await?;
            }
            SyncState::Unsynchronized { ranges } => {
                self.send_ranges(ranges.into_iter(), to_writer).await?;
            }
        }

        Ok(())
    }
    async fn send_ranges(
        &mut self,
        ranges: impl ExactSizeIterator<Item = RangeHash<R::Key, R::Hash>>,
        to_writer: &mut ToWriterSender<InitiatorMessage<R::Key, R::Hash>>,
    ) -> Result<()> {
        self.pending_ranges += ranges.len();
        to_writer
            .send(ToWriter::SendWIPLimited(
                ranges.map(InitiatorMessage::RangeRequest).collect(),
            ))
            .await
            .map_err(|err| anyhow!("{err}"))
            .context("sending range requests")?;

        Ok(())
    }
}

#[async_trait]
impl<R> Role for Initiator<R>
where
    R: Recon,
{
    type In = ResponderMessage<R::Key, R::Hash>;
    type Out = InitiatorMessage<R::Key, R::Hash>;
    type Key = R::Key;

    async fn init(&mut self) -> Result<Option<Self::Out>> {
        //  Send interests
        let interests = self.common.recon.interests().await.context("interests")?;
        Ok(Some(InitiatorMessage::InterestRequest(interests)))
    }

    fn finish(&mut self) -> Option<Self::Out> {
        Some(InitiatorMessage::Finished)
    }

    async fn handle_incoming(
        &mut self,
        to_writer: &mut ToWriterSender<Self::Out>,
        message: Self::In,
    ) -> Result<RemoteStatus> {
        match message {
            ResponderMessage::InterestResponse(interests) => {
                if interests.is_empty() {
                    to_writer
                        .send(ToWriter::Finish)
                        .await
                        .map_err(|err| anyhow!("{err}"))
                        .context("sending finish")?;
                } else {
                    let mut ranges = Vec::with_capacity(interests.len());
                    for interest in interests {
                        ranges.push(
                            self.common
                                .recon
                                .initial_range(interest)
                                .await
                                .context("querying initial range")?,
                        );
                    }
                    self.send_ranges(ranges.into_iter(), to_writer).await?;
                }
            }
            ResponderMessage::RangeResponse(ranges) => {
                self.pending_ranges -= 1;
                to_writer
                    .send(ToWriter::WIPCompleted(1))
                    .await
                    .map_err(|err| anyhow!("{err}"))
                    .context("sending wip completed")?;
                for range in ranges {
                    self.process_range(to_writer, range)
                        .await
                        .context("processing range")?;
                }
                if self.pending_ranges == 0 {
                    // All work has completed, we no longer expect any messages from the remote
                    // responder.
                    self.common.persist_all().await?;
                    to_writer
                        .send(ToWriter::Finish)
                        .await
                        .map_err(|err| anyhow!("{err}"))
                        .context("sending finish")?;
                }
            }
            ResponderMessage::Value(Value { key, value }) => {
                self.common
                    .process_value_response(key, value)
                    .await
                    .context("processing value response")?;
            }
        };
        Ok(RemoteStatus::Active)
    }
}

// Responder implements the [`Role`] where it responds to incoming requests.
struct Responder<R>
where
    R: Recon,
{
    common: Common<R>,
}

impl<R> Responder<R>
where
    R: Recon,
{
    fn new(recon: R, config: ProtocolConfig) -> Self {
        Self {
            common: Common::new(recon, config),
        }
    }

    async fn process_range(
        &mut self,
        to_writer: &mut ToWriterSender<ResponderMessage<R::Key, R::Hash>>,
        range: RangeHash<R::Key, R::Hash>,
    ) -> Result<()> {
        let sync_state = self
            .common
            .recon
            .process_range(range)
            .await
            .context("responder process_range")?;
        match sync_state {
            SyncState::Synchronized { range } => {
                // We are sync echo back the same range so that the remote learns we are in sync.
                to_writer
                    .send(ToWriter::SendAll(
                        once(Ok(ResponderMessage::RangeResponse(vec![range]))).boxed(),
                    ))
                    .await
                    .map_err(|err| anyhow!("{err}"))
                    .context("sending range response synchronized")?;
            }
            SyncState::RemoteMissing { ranges, reply_with } => {
                to_writer
                    .send(ToWriter::SendAll(
                        self.common
                            .process_remote_missing_ranges(ranges.clone())
                            .map(move |value| value.map(ResponderMessage::Value))
                            // Send only the necessary ranges to discover the values we're missing (i.e. bounding keys)
                            .chain(once(Ok(ResponderMessage::RangeResponse(reply_with))))
                            .boxed(),
                    ))
                    .await
                    .map_err(|err| anyhow!("{err}"))
                    .context("sending missing values and range response")?;
            }
            SyncState::Unsynchronized { ranges: splits } => {
                to_writer
                    .send(ToWriter::SendAll(
                        once(Ok(ResponderMessage::RangeResponse(splits))).boxed(),
                    ))
                    .await
                    .map_err(|err| anyhow!("{err}"))
                    .context("sending range response splits")?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<R> Role for Responder<R>
where
    R: Recon,
{
    type In = InitiatorMessage<R::Key, R::Hash>;
    type Out = ResponderMessage<R::Key, R::Hash>;
    type Key = R::Key;

    async fn init(&mut self) -> Result<Option<Self::Out>> {
        Ok(None)
    }
    fn finish(&mut self) -> Option<Self::Out> {
        None
    }

    async fn handle_incoming(
        &mut self,
        to_writer: &mut ToWriterSender<Self::Out>,
        message: Self::In,
    ) -> Result<RemoteStatus> {
        match message {
            InitiatorMessage::InterestRequest(interests) => {
                let ranges = self.common.recon.process_interests(interests).await?;
                to_writer
                    .send(ToWriter::SendAll(
                        once(Ok(ResponderMessage::InterestResponse(ranges))).boxed(),
                    ))
                    .await
                    .map_err(|err| anyhow!("{err}"))
                    .context("sending interest response")?;
                Ok(RemoteStatus::Active)
            }
            InitiatorMessage::RangeRequest(range) => {
                self.process_range(to_writer, range).await?;
                Ok(RemoteStatus::Active)
            }
            InitiatorMessage::Value(Value { key, value }) => {
                self.common.process_value_response(key, value).await?;
                Ok(RemoteStatus::Active)
            }
            InitiatorMessage::Finished => {
                self.common.persist_all().await?;
                Ok(RemoteStatus::Finished)
            }
        }
    }
}

// Common implements common behaviors to both [`Initiator`] and [`Responder`].
struct Common<R: Recon> {
    recon: R,
    event_q: Vec<ReconItem<R::Key>>,
    config: ProtocolConfig,
}

impl<R> Common<R>
where
    R: Recon,
{
    fn new(recon: R, config: ProtocolConfig) -> Self {
        Self {
            recon,
            event_q: Vec::with_capacity(config.insert_batch_size.saturating_add(1)),
            config,
        }
    }

    async fn process_value_response(&mut self, key: R::Key, value: Vec<u8>) -> Result<()> {
        let new = ReconItem::new(key, value);
        self.event_q.push(new);

        if self.event_q.len() >= self.config.insert_batch_size {
            self.persist_all().await?;
        }
        Ok(())
    }

    // The remote is missing all keys in the range send them over.
    fn process_remote_missing_ranges(
        &mut self,
        ranges: Vec<RangeHash<R::Key, R::Hash>>,
    ) -> impl Stream<Item = Result<Value<R::Key>>> {
        let recon = self.recon.clone();
        try_stream! {
            for range in ranges {
                // TODO this holds all keys in memory
                // Paginate or otherwise stream the keys and values back out
                let keys = recon
                    .range(range.first, range.last, 0, usize::MAX)
                    .await?;
                for key in keys {
                    let value = recon.value_for_key(key.clone()).await?.ok_or_else(|| anyhow!("recon key does not have a value: key={}", key))?;
                    yield Value { key, value };
                }
            }
        }
    }

    /// We attempt to write data in batches to reduce lock contention. However, we need to persist everything in a few cases:
    ///     - before we calculate a range response, we need to make sure our result (calculated from disk) is up to date
    ///     - before we sign off on a conversation as either the initiator or responder
    ///     - when our in memory list gets too large
    async fn persist_all(&mut self) -> Result<()> {
        tracing::info!("calling persist all: {}", self.event_q.len());
        if self.event_q.is_empty() {
            return Ok(());
        }

        let evs: Vec<_> = self.event_q.drain(..).collect();

        let batch = self.recon.insert(evs).await.context("persisting all")?;
        if !batch.invalid.is_empty() {
            for invalid in &batch.invalid {
                self.recon.metrics().record(invalid)
            }
            tracing::warn!(
                invalid_cnt=%batch.invalid.len(), peer_id=%self.config.peer_id,
                "Recon discovered data it will never allow. Hanging up on peer",
            );
            bail!("Received unknown data from peer: {}", self.config.peer_id);
        }

        // for now, we record the metrics from recon but the service is the one that will track and try to store them
        // this may get more sophisticated as we want to tie reputation into this, or make recon more aware of the meaning of
        // events and how to drive the conversation forward. it can cause some odd behavior currently as ranges won't match
        // until events are persisted, but we expect the items to arrive at almost the same time with a well behaved peer.
        if batch.pending_count() > 0 {
            if let Ok(cnt) = batch.pending_count().try_into() {
                self.recon.metrics().record(&PendingEvents(cnt))
            }
        }

        Ok(())
    }
}

enum RemoteStatus {
    // The remote is still actively sending requests.
    Active,
    // The remote will no longer send any messages.
    Finished,
}

/// Defines the Recon API.
#[async_trait]
pub trait Recon: Clone + Send + Sync + 'static {
    /// The type of Key to communicate.
    type Key: Key + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>;
    /// The type of Hash to compute over the keys.
    type Hash: AssociativeHash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>;

    /// Insert new keys into the key space.
    async fn insert(
        &self,
        items: Vec<ReconItem<Self::Key>>,
    ) -> ReconResult<InsertResult<Self::Key>>;

    /// Get all keys in the specified range
    async fn range(
        &self,
        left_fencepost: Self::Key,
        right_fencepost: Self::Key,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Vec<Self::Key>>;

    /// Reports total number of keys
    async fn len(&self) -> ReconResult<usize>;

    /// Reports if the set is empty.
    async fn is_empty(&self) -> ReconResult<bool> {
        Ok(self.len().await? == 0)
    }

    /// Retrieve a value associated with a recon key
    async fn value_for_key(&self, key: Self::Key) -> ReconResult<Option<Vec<u8>>>;

    /// Reports the interests of this recon instance
    async fn interests(&self) -> ReconResult<Vec<RangeOpen<Self::Key>>>;

    /// Computes the intersection of input interests with the local interests
    async fn process_interests(
        &self,
        interests: Vec<RangeOpen<Self::Key>>,
    ) -> ReconResult<Vec<RangeOpen<Self::Key>>>;

    /// Compute an initial hash for the range
    async fn initial_range(
        &self,
        interest: RangeOpen<Self::Key>,
    ) -> ReconResult<RangeHash<Self::Key, Self::Hash>>;

    /// Computes a response to a remote range
    async fn process_range(
        &self,
        range: RangeHash<Self::Key, Self::Hash>,
    ) -> ReconResult<SyncState<Self::Key, Self::Hash>>;

    /// Create a handle to the metrics
    fn metrics(&self) -> Metrics;
}
#[cfg(test)]
mod tests {
    use expect_test::expect;

    use crate::{tests::AlphaNumBytes, HashCount, Sha256a};

    use super::*;

    #[test]
    fn message_serializes() {
        let msg = InitiatorMessage::RangeRequest(RangeHash {
            first: AlphaNumBytes::min_value(),
            hash: HashCount::new(Sha256a::digest(&AlphaNumBytes::from("hello world")), 1),
            last: AlphaNumBytes::max_value(),
        });

        let cbor_hex = hex::encode(serde_cbor::to_vec(&msg).unwrap());
        expect!["a16c52616e676552657175657374a3656669727374406468617368a264686173685820b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde965636f756e7401646c61737441ff"].assert_eq(&cbor_hex);
    }

    #[test]
    fn message_serializes_with_small_zero_hash() {
        let msg = InitiatorMessage::RangeRequest(RangeHash {
            first: AlphaNumBytes::min_value(),
            hash: HashCount::new(Sha256a::identity(), 0),
            last: AlphaNumBytes::max_value(),
        });

        let cbor_hex = hex::encode(serde_cbor::to_vec(&msg).unwrap());
        expect!["a16c52616e676552657175657374a3656669727374406468617368a264686173684065636f756e7400646c61737441ff"].assert_eq(&cbor_hex);
    }
}
