//! Provides methods for looking up peer info.
use crate::{error::Error, IpfsDep, PeerId, PeerInfo};

/// Lookup information about a specific peer.
#[tracing::instrument(skip(client))]
pub async fn lookup<T>(client: T, peer_id: PeerId) -> Result<PeerInfo, Error>
where
    T: IpfsDep,
{
    client.lookup(peer_id).await
}
/// Lookup information about the local peer.
pub async fn lookup_local<T>(client: T) -> Result<PeerInfo, Error>
where
    T: IpfsDep,
{
    client.lookup_local().await
}
