use anyhow::Result;
use asynchronous_codec::{CborCodec, Framed};
use libp2p::{
    futures::{AsyncRead, AsyncWrite, SinkExt},
    swarm::ConnectionId,
};
use libp2p_identity::PeerId;
use tracing::{debug, trace};

use crate::{libp2p::Recon, Message};

// Perform Recon synchronization with a peer over a stream.
//
// When initiate is true, send the initial message instead of waiting for one.
#[tracing::instrument(skip(recon, stream))]
pub async fn synchronize<S: AsyncRead + AsyncWrite + Unpin, R: Recon>(
    remote_peer_id: PeerId,
    connection_id: ConnectionId,
    mut recon: R,
    stream: S,
    initiate: bool,
) -> Result<()> {
    debug!("start synchronize");
    let codec = CborCodec::<Message<R::Key, R::Hash>, Message<R::Key, R::Hash>>::new();
    let mut framed = Framed::new(stream, codec);

    if initiate {
        let msg = recon.initial_message();
        framed.send(msg).await?;
    }

    while let Some(request) = libp2p::futures::TryStreamExt::try_next(&mut framed).await? {
        let response = recon.process_message(&request)?;
        trace!(%request, %response, "recon exchange");

        let is_synchronized = response.is_synchronized();
        if is_synchronized && initiate {
            // Do not send the last message if we initiated
            break;
        }
        framed.send(response.into_message()).await?;
        if is_synchronized {
            break;
        }
    }
    framed.close().await?;
    debug!("finished synchronize, number of keys {}", recon.len());
    Ok(())
}
