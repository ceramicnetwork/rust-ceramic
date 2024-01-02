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
    sync::mpsc::{channel, Sender},
};
use tracing::{debug, info, trace};

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
    Synchronize(Vec<Message<K, H>>, bool /* in_sync */),
    ValueRequest(K),
    ValueResponse(K, Vec<u8>),
    HangUp,
}

impl<K: Key, H: AssociativeHash> std::fmt::Debug for Envelope<K, H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Synchronize(arg0, arg1) => f
                .debug_tuple("Synchronize")
                .field(arg0)
                .field(arg1)
                .finish(),
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
#[tracing::instrument(skip(recon, stream), ret)]
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
        let envelope = Envelope::Synchronize(messages, false);
        metrics.record(&EnvelopeSent(&envelope));
        framed
            .send(envelope)
            .await
            .context("sending initial messages")?;
    }

    let (tx, mut rx) = channel(10000);

    pin_mut!(framed);
    loop {
        select! {
            biased;
            request = framed.try_next() => {
                if let Some(request) = request? {
                    if !do_recv(recon.clone(), request, tx.clone(), metrics.clone()).await? {
                        break;
                    }
                }
            }
            response = rx.recv() => {
                if let Some(response) = response {
                    metrics.record(&EnvelopeSent(&response));
                    framed.send(response).await.context("sending response")?;
                } else {
                    break;
                }
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

async fn do_recv<R: Recon>(
    recon: R,
    request: Envelope<R::Key, R::Hash>,
    tx_envelope: Sender<Envelope<R::Key, R::Hash>>,
    metrics: Metrics,
) -> Result<bool> {
    metrics.record(&EnvelopeRecv(&request));
    trace!(?request, "recon request");
    match request {
        Envelope::Synchronize(messages, remote_synchronized) => {
            let response = recon
                .process_messages(messages)
                .await
                .context("processing message")?;
            trace!(?response, "recon response");

            let is_synchronized = response.is_synchronized();
            if remote_synchronized && is_synchronized {
                tx_envelope.send(Envelope::HangUp).await?;
                return Ok(false);
            }

            for key in response.new_keys() {
                tx_envelope
                    .send(Envelope::ValueRequest(key.clone()))
                    .await?;
            }
            tx_envelope
                .send(Envelope::Synchronize(
                    response.into_messages(),
                    is_synchronized,
                ))
                .await?;
        }
        Envelope::ValueRequest(key) => {
            // TODO queue up value requests for worker
            let value = recon
                .value_for_key(key.clone())
                .await
                .context("value for key")?;
            if let Some(value) = value {
                tx_envelope
                    .send(Envelope::ValueResponse(key, value))
                    .await?;
            }
        }
        Envelope::ValueResponse(key, value) => {
            recon
                .store_value_for_key(key, &value)
                .await
                .context("store value for key")?;
        }
        Envelope::HangUp => return Ok(false),
    };
    Ok(true)
}
