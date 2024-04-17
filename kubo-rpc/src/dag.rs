//! Implements the dag endpoints.

use ipld_core::{codec::Codec, ipld::Ipld};

use crate::{error::Error, IpfsDep};
use crate::{Cid, IpfsPath};

/// Get a DAG node from IPFS.
#[tracing::instrument(skip(client))]
pub async fn get<T, C>(client: T, ipfs_path: &IpfsPath) -> Result<Vec<u8>, Error>
where
    T: IpfsDep,
    C: Codec<Ipld>,
    <C as Codec<Ipld>>::Error: std::error::Error + Send + Sync + 'static,
{
    let (_cid, data) = client.get(ipfs_path).await?;
    let bytes = C::encode_to_vec(&data)
        .map_err(anyhow::Error::from)
        .map_err(Error::Internal)?;
    Ok(bytes)
}

/// Resolve an IPLD node
#[tracing::instrument(skip(client))]
pub async fn resolve<T>(client: T, path: &IpfsPath) -> Result<(Cid, String), Error>
where
    T: IpfsDep,
{
    client.resolve(path).await
}
