//! Implements the dag endpoints.
use std::io::{Read, Seek};

use crate::{Cid, IpfsPath};
use libipld::{
    multihash::{Code, MultihashDigest},
    prelude::{Codec, Decode, Encode},
    Ipld,
};

use crate::{error::Error, IpfsDep};

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

/// Store a DAG node into IFPS.
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

/// Resolve an IPLD node
#[tracing::instrument(skip(client))]
pub async fn resolve<T>(client: T, path: &IpfsPath) -> Result<(Cid, String), Error>
where
    T: IpfsDep,
{
    client.resolve(path).await
}
