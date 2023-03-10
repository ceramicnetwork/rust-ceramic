//! Implements the dag endpoints.
use std::io::{Cursor, Read, Seek};

use anyhow::anyhow;
use dag_jose::DagJoseCodec;
use iroh_api::{Cid, IpfsPath};
use libipld::{
    cbor::DagCborCodec,
    multihash::{Code, MultihashDigest},
    pb::DagPbCodec,
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
    let (cid, bytes) = client.get(ipfs_path).await?;
    let dag_data = match cid.codec() {
        // dag-pb
        0x70 => Ipld::decode(DagPbCodec, &mut Cursor::new(&bytes)).map_err(Error::Internal)?,
        // dag-cbor
        0x71 => Ipld::decode(DagCborCodec, &mut Cursor::new(&bytes)).map_err(Error::Internal)?,
        // dag-jose
        0x85 => Ipld::decode(DagJoseCodec, &mut Cursor::new(&bytes)).map_err(Error::Internal)?,
        _ => {
            return Err(Error::Invalid(anyhow!("unsupported codec {}", cid.codec())));
        }
    };
    let mut data: Vec<u8> = Vec::new();
    dag_data
        .encode(output_codec, &mut data)
        .map_err(Error::Internal)?;
    Ok(data)
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

/// Resolve an IPLD block.
#[tracing::instrument(skip(client))]
pub async fn resolve<T>(client: T, path: &IpfsPath) -> Result<Cid, Error>
where
    T: IpfsDep,
{
    let resolved_path = client.resolve(path).await?;

    Ok(resolved_path
        .iter()
        .last()
        .ok_or(Error::Internal(anyhow!(
            "resolved path should have at least one element"
        )))?
        .to_owned())
}
