use self::EventStore;

use super::*;

use std::str::FromStr;

use ceramic_core::{
    event_id::{Builder, WithInit},
    EventId, Network,
};

use cid::Cid;
use iroh_bitswap::Block;
use iroh_car::{CarHeader, CarWriter};
use libipld::{ipld, prelude::Encode, Ipld};
use libipld_cbor::DagCborCodec;
use multihash::{Code, MultihashDigest};
use rand::Rng;
use recon::Sha256a;

pub(crate) async fn new_store() -> EventStore<Sha256a> {
    let conn = SqlitePool::connect_in_memory().await.unwrap();
    EventStore::new(conn).await.unwrap()
}

// for the highwater tests that care about event ordering
pub(crate) async fn new_local_store() -> EventStore<Sha256a> {
    let conn = SqlitePool::connect_in_memory().await.unwrap();
    EventStore::new_local(conn).await.unwrap()
}

#[tokio::test]
async fn get_nonexistent_block() {
    let store = new_store().await;

    let cid = Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line

    let exists = iroh_bitswap::Store::has(&store, &cid).await.unwrap();
    assert!(!exists);
}

const MODEL_ID: &str = "k2t6wz4yhfp1r5pwi52gw89nzjbu53qk7m32o5iguw42c6knsaj0feuf927agb";
const CONTROLLER: &str = "did:key:z6Mkqtw7Pj5Lv9xc4PgUYAnwfaVoMC6FRneGWVr5ekTEfKVL";
const INIT_ID: &str = "baeabeiajn5ypv2gllvkk4muvzujvcnoen2orknxix7qtil2daqn6vu6khq";
const SORT_KEY: &str = "model";

// Return an builder for an event with the same network,model,controller,stream.
pub(crate) fn event_id_builder() -> Builder<WithInit> {
    EventId::builder()
        .with_network(&Network::DevUnstable)
        .with_sort_value(SORT_KEY, MODEL_ID)
        .with_controller(CONTROLLER)
        .with_init(&Cid::from_str(INIT_ID).unwrap())
}

// Generate an event for the same network,model,controller,stream
// The event and height are random when when its None.
pub(crate) fn random_event_id(height: Option<u64>, event: Option<&str>) -> EventId {
    event_id_builder()
        .with_event_height(height.unwrap_or_else(|| rand::thread_rng().gen()))
        .with_event(
            &event
                .map(|cid| Cid::from_str(cid).unwrap())
                .unwrap_or_else(random_cid),
        )
        .build()
}
// The EventId that is the minumum of all possible random event ids
pub(crate) fn random_event_id_min() -> EventId {
    event_id_builder().with_min_event_height().build_fencepost()
}
// The EventId that is the maximum of all possible random event ids
pub(crate) fn random_event_id_max() -> EventId {
    event_id_builder().with_max_event_height().build_fencepost()
}

pub(crate) fn random_cid() -> Cid {
    let mut data = [0u8; 8];
    rand::Rng::fill(&mut ::rand::thread_rng(), &mut data);
    let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
    Cid::new_v1(0x00, hash)
}

pub(crate) async fn build_car_file(count: usize) -> (Vec<Block>, Vec<u8>) {
    let blocks: Vec<Block> = (0..count).map(|_| random_block()).collect();
    let root = ipld!( {
        "links": blocks.iter().map(|block| Ipld::Link(block.cid)).collect::<Vec<Ipld>>(),
    });
    let mut root_bytes = Vec::new();
    root.encode(DagCborCodec, &mut root_bytes).unwrap();
    let root_cid = Cid::new_v1(
        DagCborCodec.into(),
        MultihashDigest::digest(&Code::Sha2_256, &root_bytes),
    );
    let mut car = Vec::new();
    let roots: Vec<Cid> = vec![root_cid];
    let mut writer = CarWriter::new(CarHeader::V1(roots.into()), &mut car);
    writer.write(root_cid, root_bytes).await.unwrap();
    for block in &blocks {
        writer.write(block.cid, &block.data).await.unwrap();
    }
    writer.finish().await.unwrap();
    (blocks, car)
}

pub(crate) fn random_block() -> Block {
    let mut data = [0u8; 1024];
    rand::Rng::fill(&mut ::rand::thread_rng(), &mut data);
    let hash = ::multihash::MultihashDigest::digest(&::multihash::Code::Sha2_256, &data);
    Block {
        cid: Cid::new_v1(0x00, hash),
        data: data.to_vec().into(),
    }
}
