//! Implements the swarm related endpoints.
use std::collections::BTreeMap;

use crate::{error::Error, IpfsDep, Multiaddr, PeerId};

/// Report all connected peers of the current node.
#[tracing::instrument(skip(client))]
pub async fn peers<T>(client: T) -> Result<BTreeMap<PeerId, Vec<Multiaddr>>, Error>
where
    T: IpfsDep,
{
    // Use a BTreeMap for consistent ordering of peers
    Ok(client
        .peers()
        .await?
        .into_iter()
        .collect::<BTreeMap<_, _>>())
}

/// Connect to a specific peer node.
#[tracing::instrument(skip(client))]
pub async fn connect<T>(client: T, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<(), Error>
where
    T: IpfsDep,
{
    client.connect(peer_id, addrs).await?;
    Ok(())
}
