//! Implements the dag endpoints.

use crate::{error::Error, Cid, IpfsDep};

/// Get a block from IPFS.
#[tracing::instrument(skip(client))]
pub async fn get<T>(client: T, cid: Cid) -> Result<Vec<u8>, Error>
where
    T: IpfsDep,
{
    let bytes = client.block_get(cid).await?;
    Ok(bytes.to_vec())
}

/// Resolve an IPLD block.
#[tracing::instrument(skip(client))]
pub async fn stat<T>(client: T, cid: Cid) -> Result<u64, Error>
where
    T: IpfsDep,
{
    client.block_size(cid).await
}
