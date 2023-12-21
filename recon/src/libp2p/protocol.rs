use anyhow::Result;
use asynchronous_codec::{CborCodec, Framed};
use libp2p::{
    futures::{AsyncRead, AsyncWrite, SinkExt},
    swarm::ConnectionId,
};
use libp2p_identity::PeerId;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, trace};

use crate::{
    libp2p::{stream_set::StreamSet, Recon},
    AssociativeHash, Key, Message,
};

#[derive(Debug, Serialize, Deserialize)]
struct Envelope<K: Key, H: AssociativeHash> {
    messages: Vec<Message<K, H>>,
}

// Intiate Recon synchronization with a peer over a stream.
#[tracing::instrument(skip(recon, stream), ret)]
pub async fn initiate_synchronize<S: AsyncRead + AsyncWrite + Unpin, R: Recon>(
    remote_peer_id: PeerId,
    connection_id: ConnectionId,
    stream_set: StreamSet,
    recon: R,
    stream: S,
) -> Result<StreamSet> {
    info!("initiate_synchronize");
    let codec = CborCodec::<Envelope<R::Key, R::Hash>, Envelope<R::Key, R::Hash>>::new();
    let mut framed = Framed::new(stream, codec);

    let messages = recon.initial_messages().await?;
    framed.send(Envelope { messages }).await?;

    while let Some(request) = libp2p::futures::TryStreamExt::try_next(&mut framed).await? {
        trace!(?request, "recon request");
        let response = recon.process_messages(request.messages).await?;
        trace!(?response, "recon response");

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
    }
    framed.close().await?;
    debug!(
        "finished initiate_synchronize, number of keys {}",
        recon.len().await?
    );
    Ok(stream_set)
}

// Perform Recon synchronization with a peer over a stream.
// Expect the remote peer to initiate the communication.
#[tracing::instrument(skip(stream, recon), ret)]
pub async fn accept_synchronize<S: AsyncRead + AsyncWrite + Unpin, R: Recon>(
    remote_peer_id: PeerId,
    connection_id: ConnectionId,
    stream_set: StreamSet,
    recon: R,
    stream: S,
) -> Result<StreamSet> {
    info!("accept_synchronize");
    let codec = CborCodec::<Envelope<R::Key, R::Hash>, Envelope<R::Key, R::Hash>>::new();
    let mut framed = Framed::new(stream, codec);

    while let Some(request) = libp2p::futures::TryStreamExt::try_next(&mut framed).await? {
        trace!(?request, "recon request");
        let response = recon.process_messages(request.messages).await?;
        trace!(?response, "recon response");

        let is_synchronized = response.is_synchronized();
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
    debug!(
        "finished accept_synchronize, number of keys {}",
        recon.len().await?
    );
    Ok(stream_set)
}
