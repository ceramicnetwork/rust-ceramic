//! Implements the dag endpoints.
use std::io::{Read, Seek};

use anyhow::anyhow;
use bytes::Bytes;
use libipld::{
    multihash::{Code, MultihashDigest},
    prelude::{Codec, Decode, Encode},
    Ipld,
};
use tokio::io::AsyncRead;

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

/// Store a DAG node into IPFS.
#[tracing::instrument(skip_all)]
pub async fn put<T, I, S, R>(
    client: T,
    input_codec: I,
    store_codec: S,
    data: &mut R,
) -> Result<Cid, Error>
where
    T: IpfsDep,
    I: Codec,
    S: Codec,
    Ipld: Decode<I>,
    Ipld: Encode<S>,
    R: Read + Seek,
{
    let dag_data = Ipld::decode(input_codec, data).map_err(Error::Invalid)?;

    let mut blob: Vec<u8> = Vec::new();
    dag_data
        .encode(store_codec, &mut blob)
        .map_err(Error::Internal)?;

    let hash = Code::Sha2_256.digest(&blob);
    let cid = Cid::new_v1(store_codec.into(), hash);
    client.put(cid, blob.into(), vec![]).await?;
    Ok(cid)
}

/// Import data representing a car file
#[tracing::instrument(skip_all)]
pub async fn import<T, R>(client: T, data: R) -> Result<Vec<Cid>, Error>
where
    T: IpfsDep,
    R: AsyncRead + Send + Unpin,
{
    let mut reader = iroh_car::CarReader::new(data)
        .await
        .map_err(|e| Error::Internal(e.into()))?;
    if reader.header().roots().is_empty() {
        // Ref: https://ipld.io/specs/transport/car/carv1/#number-of-roots
        return Err(Error::Invalid(anyhow!(
            "car file must have at least one root."
        )));
    }
    while let Some(block) = reader
        .next_block()
        .await
        .map_err(|e| Error::Internal(e.into()))?
    {
        client.put(block.0, Bytes::from(block.1), vec![]).await?;
    }
    Ok(reader.header().roots().to_vec())
}

/// Resolve an IPLD node
#[tracing::instrument(skip(client))]
pub async fn resolve<T>(client: T, path: &IpfsPath) -> Result<(Cid, String), Error>
where
    T: IpfsDep,
{
    client.resolve(path).await
}
