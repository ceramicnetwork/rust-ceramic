//! Implements the /swarm/* endpoints.
use std::collections::HashMap;

use iroh_api::{Multiaddr, PeerId};

use crate::{error::Error, IrohClient, P2pClient};

#[tracing::instrument(skip(client))]
pub async fn peers<T>(client: T) -> Result<HashMap<PeerId, Vec<Multiaddr>>, Error>
where
    T: IrohClient,
{
    let p2p = client.try_p2p().map_err(|e| Error::Internal(e))?;
    Ok(p2p.peers().await.map_err(|e| Error::Internal(e))?)
}

#[tracing::instrument(skip(client))]
pub async fn connect<T>(client: T, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<(), Error>
where
    T: IrohClient,
{
    let p2p = client.try_p2p().map_err(|e| Error::Internal(e))?;
    p2p.connect(peer_id, addrs)
        .await
        .map_err(|e| Error::Internal(e))?;

    Ok(())
}
