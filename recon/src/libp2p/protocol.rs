use anyhow::Result;
use asynchronous_codec::{CborCodec, Framed};
use libp2p::{
    futures::{AsyncRead, AsyncWrite, SinkExt},
    swarm::ConnectionId,
};
use libp2p_identity::PeerId;
use serde::{Deserialize, Serialize};
use tracing::{debug, trace};

use crate::{
    libp2p::{stream_set::StreamSet, Recon},
    AssociativeHash, Key, Message,
};

#[derive(Debug, Serialize, Deserialize)]
struct Envelope<K: Key, H: AssociativeHash> {
    messages: Vec<Message<K, H>>,
}

// Intiate Recon synchronization with a peer over a stream.
#[tracing::instrument(skip(recon, stream))]
pub async fn initiate_synchronize<S: AsyncRead + AsyncWrite + Unpin, R: Recon>(
    remote_peer_id: PeerId,
    connection_id: ConnectionId,
    stream_set: StreamSet,
    mut recon: R,
    stream: S,
) -> Result<StreamSet> {
    debug!("start synchronize");
    let codec = CborCodec::<Envelope<R::Key, R::Hash>, Envelope<R::Key, R::Hash>>::new();
    let mut framed = Framed::new(stream, codec);

    let messages = recon.initial_messages()?;
    framed.send(Envelope { messages }).await?;

    while let Some(request) = libp2p::futures::TryStreamExt::try_next(&mut framed).await? {
        let response = recon.process_messages(&request.messages)?;
        trace!(?request, ?response, "recon exchange");

        let is_synchronized = response.is_synchronized();
        if is_synchronized {
            // Do not send the last message since we initiated
            break;
        }
        framed
            .send(Envelope {
                messages: response.into_messages(),
            })
            .await?;
        if is_synchronized {
            break;
        }
    }
    framed.close().await?;
    debug!("finished synchronize, number of keys {}", recon.len());
    Ok(stream_set)
}

// Perform Recon synchronization with a peer over a stream.
// Expect the remote peer to initiate the communication.
#[tracing::instrument(skip(stream, recon))]
pub async fn accept_synchronize<S: AsyncRead + AsyncWrite + Unpin, R: Recon>(
    remote_peer_id: PeerId,
    connection_id: ConnectionId,
    stream_set: StreamSet,
    mut recon: R,
    stream: S,
) -> Result<StreamSet> {
    debug!("accept_synchronize_interests");
    let codec = CborCodec::<Envelope<R::Key, R::Hash>, Envelope<R::Key, R::Hash>>::new();
    let mut framed = Framed::new(stream, codec);

    while let Some(request) = libp2p::futures::TryStreamExt::try_next(&mut framed).await? {
        let response = recon.process_messages(&request.messages)?;
        trace!(?request, ?response, "recon exchange");

        let is_synchronized = response.is_synchronized();
        framed
            .send(Envelope {
                messages: response.into_messages(),
            })
            .await?;
        if is_synchronized {
            debug!("finished synchronize, number of keys {}", recon.len());
            break;
        }
    }
    framed.close().await?;

    Ok(stream_set)
}
