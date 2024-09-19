use anyhow::Result;
use asynchronous_codec::{CborCodec, Framed};
use libp2p::futures::{AsyncRead, AsyncWrite};
use libp2p::swarm::ConnectionId;
use libp2p_identity::PeerId;
use tracing::Level;

use ceramic_core::NodeId;

use crate::{
    libp2p::stream_set::StreamSet,
    protocol::{self, ProtocolConfig, Recon},
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
    let remote_node_id = NodeId::try_from_peer_id(&remote_peer_id)?;
    protocol::initiate_synchronize(
        recon,
        stream,
        ProtocolConfig::new_node_id(remote_node_id),
    )
    .await?;
    Ok(stream_set)
}
// Intiate Recon synchronization with a peer over a stream.
#[tracing::instrument(skip(recon, stream, ), ret(level = Level::DEBUG))]
pub async fn respond_synchronize<S, R>(
    remote_peer_id: PeerId,      // included for context only
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
    let remote_node_id = NodeId::try_from_peer_id(&remote_peer_id)?;
    protocol::respond_synchronize(
        recon,
        stream,
        ProtocolConfig::new_node_id(remote_node_id),
    )
    .await?;
    Ok(stream_set)
}
