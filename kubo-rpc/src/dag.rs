//! Implements the dag endpoints.

use libipld::{
    prelude::{Codec, Encode},
    Ipld,
};

use crate::{error::Error, IpfsDep};
use crate::{Cid, IpfsPath};

/// Get a DAG node from IPFS.
#[tracing::instrument(skip(client, output_codec))]
pub async fn get<T, C>(client: T, ipfs_path: &IpfsPath, output_codec: C) -> Result<Vec<u8>, Error>
where
    T: IpfsDep,
    C: Codec,
    Ipld: Encode<C>,
{
    let (_cid, data) = client.get(ipfs_path).await?;
    let mut bytes: Vec<u8> = Vec::new();
    data.encode(output_codec, &mut bytes)
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
