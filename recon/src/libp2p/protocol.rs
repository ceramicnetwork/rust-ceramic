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
enum Envelope<K: Key, H: AssociativeHash> {
    Synchronize(Vec<Message<K, H>>, bool),
    ValueRequest(K),
    ValueResponse(K, Vec<u8>),
    HangUp,
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
) -> Result<StreamSet> {
    info!("initiate_synchronize");
    let codec = CborCodec::<Envelope<R::Key, R::Hash>, Envelope<R::Key, R::Hash>>::new();
    let mut framed = Framed::new(stream, codec);

    if initiate {
        let messages = recon.initial_messages().await?;
        framed.send(Envelope::Synchronize(messages, false)).await?;
    }

    while let Some(request) = libp2p::futures::TryStreamExt::try_next(&mut framed).await? {
        trace!(?request, "recon request");
        match request {
            Envelope::Synchronize(messages, remote_synchronized) => {
                let response = recon.process_messages(messages).await?;
                trace!(?response, "recon response");

                let is_synchronized = response.is_synchronized();
                if remote_synchronized && is_synchronized {
                    framed.send(Envelope::HangUp).await?;
                    break;
                }

                for key in response.new_keys() {
                    framed.send(Envelope::ValueRequest(key.clone())).await?;
                }

                framed
                    .send(Envelope::Synchronize(
                        response.into_messages(),
                        is_synchronized,
                    ))
                    .await?;
            }
            Envelope::ValueRequest(key) => {
                // TODO queue up value requests for worker
                let value = recon.value_for_key(key.clone()).await?;
                if let Some(value) = value {
                    framed.send(Envelope::ValueResponse(key, value)).await?;
                }
            }
            Envelope::ValueResponse(key, value) => {
                recon.store_value_for_key(key, &value).await?;
            }
            Envelope::HangUp => break,
        };
    }
    framed.close().await?;
    debug!(
        "finished accept_synchronize, number of keys {}",
        recon.len().await?
    );
    Ok(stream_set)
}
