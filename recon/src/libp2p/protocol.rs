use std::ops::RangeInclusive;

use anyhow::{Context, Result};
use asynchronous_codec::{CborCodec, Framed};
use ceramic_metrics::Recorder;
use libp2p::{
    futures::{pin_mut, AsyncRead, AsyncWrite, SinkExt, TryStreamExt},
    swarm::ConnectionId,
};
use libp2p_identity::PeerId;
use serde::{Deserialize, Serialize};
use tokio::{select, sync::mpsc::channel};
use tracing::{debug, trace};

use crate::{
    libp2p::{
        metrics::{EnvelopeRecv, EnvelopeSent, Metrics},
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

// Intiate Recon synchronization with a peer over a stream.
#[tracing::instrument(skip(recon, stream, metrics), ret)]
pub async fn synchronize<S: AsyncRead + AsyncWrite + Unpin, R: Recon>(
    remote_peer_id: PeerId,
    connection_id: ConnectionId,
    stream_set: StreamSet,
    recon: R,
    stream: S,
    initiate: bool,
    metrics: Metrics,
) -> Result<StreamSet> {
    debug!("synchronize");
    let codec = CborCodec::<Envelope<R::Key, R::Hash>, Envelope<R::Key, R::Hash>>::new();
    let mut framed = Framed::new(stream, codec);

    if initiate {
        let messages = recon.initial_messages().await.context("initial_messages")?;
        let envelope = Envelope::Synchronize(messages);
        metrics.record(&EnvelopeSent(&envelope));
        framed
            .send(envelope)
            .await
            .context("sending initial messages")?;
    }

    let (tx_want_values, mut rx_want_values) = channel(1000);
    let (_tx_synched_ranges, mut rx_synced_ranges) = channel::<RangeInclusive<R::Key>>(1000);

    let mut local_is_done = false;
    let mut remote_is_done = false;
    let mut keys_synchronized = false;

    let mut is_want_values_done = false;
    let mut is_synched_ranges_done = false;

    pin_mut!(framed);
    loop {
        select! {
            request = framed.try_next() => {
                if let Some(request) = request? {
                    metrics.record(&EnvelopeRecv(&request));
                    trace!(?request, "recon request");
                    match request {
                        Envelope::Synchronize(messages, ) => {
                            let response = recon
                                .process_messages(messages)
                                .await
                                .context("processing message")?;
                            trace!(?response, "recon response");

                            for key in response.new_keys() {
                                let _ = tx_want_values.try_send(key.clone());
                            }
                            // TODO append synced_ranges

                            let local_is_synchronized = response.is_synchronized();
                            if local_is_synchronized {
                                keys_synchronized = true;
                                rx_want_values.close();
                                rx_synced_ranges.close();
                            }
                            if !local_is_synchronized || !initiate {
                                framed
                                    .send(Envelope::Synchronize(response.into_messages()))
                                    .await?;
                            }
                        }
                        Envelope::ValueRequest(key) => {
                            // TODO queue up value requests for worker
                            let value = recon
                                .value_for_key(key.clone())
                                .await
                                .context("value for key")?;
                            if let Some(value) = value {
                                framed.feed(Envelope::ValueResponse(key, value)).await?;
                            }
                        }
                        Envelope::ValueResponse(key, value) => {
                            recon
                                .store_value_for_key(key, &value)
                                .await
                                .context("store value for key")?;
                        }
                        Envelope::HangUp => {
                            remote_is_done = true;
                        }
                    };
                }
            }
            // Send want value requests
            key = rx_want_values.recv(), if !is_want_values_done => {
                if let Some(key) = key {
                    let envelope = Envelope::ValueRequest(key);
                    metrics.record(&EnvelopeSent(&envelope));
                    framed
                        .feed(envelope)
                        .await
                        .context("feeding value request")?;
                } else {
                    trace!(is_want_values_done, "is_want_values_done");
                    is_want_values_done = true;
                }
            }
            // Process any synchronized ranges
            range = rx_synced_ranges.recv(), if !is_synched_ranges_done => {
                if let Some(_range) = range {
                    // TODO
                } else {
                    trace!(is_synched_ranges_done, "is_synched_ranges_done");
                    is_synched_ranges_done = true;
                }
            }
        }

        if keys_synchronized && is_want_values_done && is_synched_ranges_done {
            trace!(local_is_done, "local is done");
            if !local_is_done {
                local_is_done = true;
                framed
                    .send(Envelope::HangUp)
                    .await
                    .context("feeding value request")?;
            }
            if remote_is_done {
                break;
            }
        }
    }

    framed.close().await.context("closing stream")?;
    debug!(
        "finished synchronize, number of keys {}",
        recon.len().await?
    );
    Ok(stream_set)
}
