use std::{ops::RangeInclusive, time::Duration};

use anyhow::{Context, Result};
use asynchronous_codec::{CborCodec, Framed};
use ceramic_metrics::Recorder;
use libp2p::{
    futures::{pin_mut, AsyncRead, AsyncWrite, SinkExt, TryStreamExt},
    swarm::ConnectionId,
};
use libp2p_identity::PeerId;
use serde::{Deserialize, Serialize};
use tokio::{
    select,
    sync::mpsc::{channel, Receiver, Sender},
    time,
};
use tracing::trace;

use crate::{
    libp2p::{
        metrics::{EnvelopeRecv, EnvelopeSent, Metrics, WantEnqueueFailed},
        stream_set::StreamSet,
        Recon,
    },
    AssociativeHash, Key, Message,
};

#[derive(Serialize, Deserialize)]
pub(crate) enum Envelope<K: Key, H: AssociativeHash> {
    Synchronize(Vec<Message<K, H>>),
    ValueRequest(K),
    ValueResponse(K, Vec<u8>),
    HangUp,
}

impl<K: Key, H: AssociativeHash> std::fmt::Debug for Envelope<K, H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Synchronize(arg0) => f.debug_tuple("Synchronize").field(arg0).finish(),
            Self::ValueRequest(arg0) => f.debug_tuple("ValueRequest").field(arg0).finish(),
            Self::ValueResponse(arg0, arg1) => f
                .debug_tuple("ValueResponse")
                .field(arg0)
                .field(&arg1.len())
                .finish(),
            Self::HangUp => write!(f, "HangUp"),
        }
    }
}

struct Synchronize<S: AsyncRead + AsyncWrite + Unpin, R: Recon> {
    stream: Framed<S, CborCodec<Envelope<R::Key, R::Hash>, Envelope<R::Key, R::Hash>>>,
    recon: R,

    initiate: bool,

    rx_want_values: Receiver<R::Key>,
    tx_want_values: Sender<R::Key>,
    rx_synced_ranges: Receiver<RangeInclusive<R::Key>>,
    tx_synced_ranges: Sender<RangeInclusive<R::Key>>,

    local_is_done: bool,
    remote_is_done: bool,
    keys_synchronized: bool,
    is_want_values_done: bool,
    is_synced_ranges_done: bool,

    metrics: Metrics,
}

impl<S: AsyncRead + AsyncWrite + Unpin, R: Recon> Synchronize<S, R> {
    fn new(stream: S, recon: R, metrics: Metrics, initiate: bool) -> Self {
        let codec = CborCodec::new();
        let stream = Framed::new(stream, codec);

        let (tx_want_values, rx_want_values) = channel(1000);
        let (tx_synced_ranges, rx_synced_ranges) = channel(1000);

        let local_is_done = false;
        let remote_is_done = false;
        let keys_synchronized = false;

        let is_want_values_done = false;
        let is_synched_ranges_done = false;

        Self {
            stream,
            recon,
            initiate,
            rx_want_values,
            tx_want_values,
            rx_synced_ranges,
            tx_synced_ranges,
            local_is_done,
            remote_is_done,
            keys_synchronized,
            is_want_values_done,
            is_synced_ranges_done: is_synched_ranges_done,
            metrics,
        }
    }

    async fn run(mut self) -> Result<()> {
        if self.initiate {
            let messages = self
                .recon
                .initial_messages()
                .await
                .context("initial_messages")?;
            self.send(Envelope::Synchronize(messages))
                .await
                .context("sending initial messages")?;
        }

        let flush_interval = time::interval(Duration::from_secs(1));
        pin_mut!(flush_interval);

        loop {
            trace!(
                self.keys_synchronized,
                self.is_want_values_done,
                self.is_synced_ranges_done,
                self.remote_is_done,
                "iter"
            );
            select! {
                biased;
                _ = flush_interval.tick() => {
                    self.stream.flush().await.context("flushing stream")?;
                }
                request = self.stream.try_next() => {
                    if let Some(request) = request? {
                        self.handle_incoming(request).await.context("handle incoming message")?;
                    }
                }
                // Send want value requests
                key = self.rx_want_values.recv(), if !self.is_want_values_done => {
                    if let Some(key) = key {
                        self.handle_want_value(key).await.context("handle want value")?;
                    } else {
                        self.is_want_values_done = true;
                    }
                }
                // Process any synchronized ranges
                range = self.rx_synced_ranges.recv(), if !self.is_synced_ranges_done => {
                    if let Some(range) = range {
                        self.handle_synced_range(range).await.context("handle synced range")?;
                    } else {
                        self.is_synced_ranges_done = true;
                    }
                }
            }

            if self.keys_synchronized && self.is_want_values_done && self.is_synced_ranges_done {
                if !self.local_is_done {
                    self.local_is_done = true;
                    self.send(Envelope::HangUp)
                        .await
                        .context("sending hangup")?;
                }
                if self.remote_is_done {
                    break;
                }
            }
        }

        self.stream.close().await.context("closing stream")?;
        Ok(())
    }

    async fn handle_incoming(&mut self, request: Envelope<R::Key, R::Hash>) -> Result<()> {
        self.metrics.record(&EnvelopeRecv(&request));
        trace!(?request, "recon request");
        match request {
            Envelope::Synchronize(messages) => {
                let response = self
                    .recon
                    .process_messages(messages)
                    .await
                    .context("processing message")?;
                trace!(?response, "recon response");

                for key in response.new_keys() {
                    if let Err(_) = self.tx_want_values.try_send(key.clone()) {
                        self.metrics.record(&WantEnqueueFailed);
                    }
                }
                // TODO append synced_ranges

                let local_is_synchronized = response.is_synchronized();
                if local_is_synchronized {
                    self.keys_synchronized = true;
                    self.rx_want_values.close();
                    self.rx_synced_ranges.close();
                }
                if !local_is_synchronized || !self.initiate {
                    self.send(Envelope::Synchronize(response.into_messages()))
                        .await
                        .context("sending synchronize")?;
                }
            }
            Envelope::ValueRequest(key) => {
                // TODO: Measure impact of fetching value inline
                let value = self
                    .recon
                    .value_for_key(key.clone())
                    .await
                    .context("value for key")?;
                if let Some(value) = value {
                    self.feed(Envelope::ValueResponse(key, value))
                        .await
                        .context("feeding value response")?;
                }
            }
            Envelope::ValueResponse(key, value) => {
                self.recon
                    .store_value_for_key(key, &value)
                    .await
                    .context("store value for key")?;
            }
            Envelope::HangUp => {
                self.remote_is_done = true;
            }
        };
        Ok(())
    }
    async fn handle_want_value(&mut self, key: R::Key) -> Result<()> {
        self.feed(Envelope::ValueRequest(key))
            .await
            .context("feeding value request")?;
        Ok(())
    }
    async fn handle_synced_range(&mut self, _range: RangeInclusive<R::Key>) -> Result<()> {
        // TODO
        Ok(())
    }

    async fn send(&mut self, envelope: Envelope<R::Key, R::Hash>) -> Result<()> {
        self.metrics.record(&EnvelopeSent(&envelope));
        self.stream.send(envelope).await?;
        Ok(())
    }
    async fn feed(&mut self, envelope: Envelope<R::Key, R::Hash>) -> Result<()> {
        self.metrics.record(&EnvelopeSent(&envelope));
        self.stream.feed(envelope).await?;
        Ok(())
    }
}

// Intiate Recon synchronization with a peer over a stream.
#[tracing::instrument(skip(recon, stream, metrics), ret)]
pub async fn synchronize<S: AsyncRead + AsyncWrite + Unpin, R: Recon>(
    remote_peer_id: PeerId,      // included for context only
    connection_id: ConnectionId, // included for context only
    stream_set: StreamSet,
    recon: R,
    stream: S,
    initiate: bool,
    metrics: Metrics,
) -> Result<StreamSet> {
    let synchronize = Synchronize::new(stream, recon, metrics, initiate);
    synchronize.run().await?;
    Ok(stream_set)
}
