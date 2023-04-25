//! Implements the dag endpoints.

use crate::Cid;
use libipld::{
    multihash::{Code, MultihashDigest},
    prelude::Codec,
};

use crate::{error::Error, IpfsDep};

/// Get a block from IPFS.
#[tracing::instrument(skip(client))]
pub async fn get<T>(client: T, cid: Cid) -> Result<Vec<u8>, Error>
where
    T: IpfsDep,
{
    let bytes = client.block_get(cid).await?;
    Ok(bytes.to_vec())
}

/// Store a block into IFPS.
#[tracing::instrument(skip_all)]
pub async fn put<T, C>(client: T, codec: C, data: Vec<u8>) -> Result<Cid, Error>
where
    T: IpfsDep,
    C: Codec,
{
    let hash = Code::Sha2_256.digest(&data);
    let cid = Cid::new_v1(codec.into(), hash);
    client.put(cid, data.into(), vec![]).await?;
    Ok(cid)
}

/// Resolve an IPLD block.
#[tracing::instrument(skip(client))]
pub async fn stat<T>(client: T, cid: Cid) -> Result<u64, Error>
where
    T: IpfsDep,
{
    client.block_size(cid).await
}
