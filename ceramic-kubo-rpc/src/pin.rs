//! Implements the pin endpoints.
use anyhow::anyhow;
use iroh_api::{Cid, IpfsPath};

use crate::{error::Error, IpfsDep};

/// Add a DAG node to local store and mark it to not be garbaged collected.
#[tracing::instrument(skip(client))]
pub async fn add<T>(client: T, ipfs_path: &IpfsPath) -> Result<Cid, Error>
where
    T: IpfsDep,
{
    // Beetle does not have any garbage collection for its store so everything is pinned.
    // Therefore we do not need to track which blocks are pinned.

    // Get the data, this will ensure its stored locally.
    let (cid, _data) = client.get(ipfs_path).await?;
    Ok(cid)
}

/// Mark a DAG node as safe to garbage collect.
#[tracing::instrument(skip(_client))]
pub async fn remove<T>(_client: T, ipfs_path: &IpfsPath) -> Result<Cid, Error>
where
    T: IpfsDep,
{
    if let Some(cid) = ipfs_path.cid() {
        // Beetle does not have any garbage collection for its store so everything is pinned.
        // Therefore we do not need to track which blocks are pinned.
        // Do nothing
        Ok(*cid)
    } else {
        Err(Error::Invalid(anyhow!("IPFS path does not have a CID")))
    }
}

/// List pinned blocks
#[tracing::instrument(skip(client))]
pub async fn list<T>(client: T) -> Result<Vec<Cid>, Error>
where
    T: IpfsDep,
{
    Ok(client.list().await?)
}
