use anyhow::Result;
use asynchronous_codec::{CborCodec, Framed};
use libp2p::futures::{AsyncRead, AsyncWrite};
use libp2p::swarm::ConnectionId;
use libp2p_identity::PeerId;
use tracing::Level;

use crate::{
    libp2p::stream_set::StreamSet,
    protocol::{self, Recon},
};

// The max number of writes we'll batch up before flushing anything to disk.
// As we descend the tree and find smaller ranges, this won't apply as we have to flush
// before recomputing a range, but it will be used when we're processing large ranges we don't yet have.
const INSERT_BATCH_SIZE: usize = 100;

// Initiate Recon synchronization with a peer over a stream.
#[tracing::instrument(skip(recon, stream, ), ret(level = Level::DEBUG))]
pub async fn initiate_synchronize<S, R>(
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
    protocol::initiate_synchronize(recon, stream, INSERT_BATCH_SIZE).await?;
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
    protocol::respond_synchronize(recon, stream, INSERT_BATCH_SIZE).await?;
    Ok(stream_set)
}
