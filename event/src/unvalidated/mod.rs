mod builder;
mod event;
mod payload;

pub mod signed;

pub use builder::*;
pub use event::*;
use multihash_codetable::{Code, MultihashDigest};
pub use payload::*;

use cid::Cid;
use ipld_core::{codec::Codec, ipld::Ipld};
use serde_ipld_dagcbor::codec::DagCborCodec;

fn cid_from_dag_cbor(data: &[u8]) -> Cid {
    Cid::new_v1(
        <DagCborCodec as Codec<Ipld>>::CODE,
        Code::Sha2_256.digest(data),
    )
}

fn cid_from_dag_jose(data: &[u8]) -> Cid {
    Cid::new_v1(
        0x85, // TODO use constant for DagJose codec
        Code::Sha2_256.digest(data),
    )
}
