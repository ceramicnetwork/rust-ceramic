use cid::Cid;
use ipld_core::{codec::Codec, ipld::Ipld};
use multihash_codetable::{Code, MultihashDigest};
use serde_ipld_dagcbor::codec::DagCborCodec;

/// DagCborIpfsBlock generates a CIDv1 assuming the Vec<u8> is DagCbor
#[derive(Debug)]
pub struct DagCborIpfsBlock {
    /// A CIDv1 DagCbor cid
    pub cid: Cid,
    /// DagCbor bytes
    pub data: Vec<u8>,
}

impl From<&[u8]> for DagCborIpfsBlock {
    fn from(data: &[u8]) -> DagCborIpfsBlock {
        DagCborIpfsBlock {
            cid: Cid::new_v1(
                <DagCborCodec as Codec<Ipld>>::CODE,
                Code::Sha2_256.digest(data),
            ),
            data: data.to_vec(),
        }
    }
}

impl From<Vec<u8>> for DagCborIpfsBlock {
    fn from(data: Vec<u8>) -> DagCborIpfsBlock {
        DagCborIpfsBlock {
            cid: Cid::new_v1(
                <DagCborCodec as Codec<Ipld>>::CODE,
                Code::Sha2_256.digest(&data),
            ),
            data,
        }
    }
}
