//! Provides methods for looking up peer info.

use multihash::Multihash;

use crate::{error::Error, IpfsDep, PeerId, PeerInfo};

/// Lookup information about a specific peer.
#[tracing::instrument(skip(client))]
pub async fn lookup<T>(client: T, peer_id: PeerId) -> Result<PeerInfo, Error>
where
    T: IpfsDep,
{
    Ok(add_peer_id_to_addrs(client.lookup(peer_id).await?))
}
/// Lookup information about the local peer.
pub async fn lookup_local<T>(client: T) -> Result<PeerInfo, Error>
where
    T: IpfsDep,
{
    Ok(add_peer_id_to_addrs(client.lookup_local().await?))
}

// Adds the peer's own peer ID to its listen_addrs if it's not already present.
fn add_peer_id_to_addrs(mut peer: PeerInfo) -> PeerInfo {
    let peer_mh: Multihash = peer.peer_id.into();
    peer.listen_addrs = peer
        .listen_addrs
        .into_iter()
        .map(|addr| {
            if !addr
                .iter()
                .any(|addr| matches!(addr, multiaddr::Protocol::P2p(_)))
            {
                addr.with(multiaddr::Protocol::P2p(peer_mh))
            } else {
                addr
            }
        })
        .collect();
    peer
}
