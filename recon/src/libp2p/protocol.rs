use anyhow::Result;
use asynchronous_codec::{CborCodec, Framed};
use libp2p::futures::{AsyncRead, AsyncWrite};
use libp2p::swarm::ConnectionId;
use libp2p_identity::PeerId;
use tracing::Level;

use crate::protocol::ProtocolConfig;
use crate::{
    libp2p::stream_set::StreamSet,
    protocol::{self, Recon},
};

// Intiate Recon synchronization with a peer over a stream.
#[tracing::instrument(skip(recon, stream, ), ret(level = Level::DEBUG))]
pub async fn initiate_synchronize<S, R>(
    remote_peer_id: PeerId,
    connection_id: ConnectionId, // included for context only
    stream_set: StreamSet,
    recon: R,
    stream: S,
) -> Result<StreamSet>
where
    R: Recon,
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let codec = CborCodec::new();
    let stream = Framed::new(stream, codec);
    protocol::initiate_synchronize(recon, stream, ProtocolConfig::new_peer_id(remote_peer_id))
        .await?;
    Ok(stream_set)
}
// Intiate Recon synchronization with a peer over a stream.
#[tracing::instrument(skip(recon, stream, ), ret(level = Level::DEBUG))]
pub async fn respond_synchronize<S, R>(
    remote_peer_id: PeerId,
    connection_id: ConnectionId, // included for context only
    stream_set: StreamSet,
    recon: R,
    stream: S,
) -> Result<StreamSet>
where
    R: Recon,
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let codec = CborCodec::new();
    let stream = Framed::new(stream, codec);
    protocol::respond_synchronize(recon, stream, ProtocolConfig::new_peer_id(remote_peer_id))
        .await?;
    Ok(stream_set)
}
