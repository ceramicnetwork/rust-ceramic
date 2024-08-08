use cid::Cid;
use ipld_core::{codec::Codec, ipld::Ipld};
use multihash_codetable::{Code, MultihashDigest};
use serde_ipld_dagcbor::codec::DagCborCodec;

/// A DagCbor block with a CIDv1 and the raw bytes.
#[derive(Clone, PartialEq)]
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

impl std::fmt::Debug for DagCborIpfsBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DagCborIpfsBlock")
            .field("cid", &format!("{:?}", &self.cid))
            .field("data", &hex::encode::<&Vec<u8>>(&self.data))
            .finish()
    }
}
