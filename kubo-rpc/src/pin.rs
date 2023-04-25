//! Implements the pin endpoints.
use crate::{error::Error, Cid, IpfsDep, IpfsPath};

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
    // We do not have any garbage collection for its store so everything is pinned.
    // Therefore we do not need to track which blocks are pinned.
    // Do nothing
    Ok(ipfs_path.cid())
}
