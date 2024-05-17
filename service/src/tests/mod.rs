mod event;
mod interest;

use std::str::FromStr;

use ceramic_core::{
    event_id::{Builder, WithInit},
    DagCborEncoded, EventId, Network, StreamId,
};

use cid::Cid;
use ipld_core::{codec::Codec, ipld, ipld::Ipld};
use iroh_bitswap::Block;
use iroh_car::{CarHeader, CarWriter};
use multihash_codetable::{Code, MultihashDigest};
use rand::{thread_rng, Rng};
use serde_ipld_dagcbor::codec::DagCborCodec;

const MODEL_ID: &str = "k2t6wz4yhfp1r5pwi52gw89nzjbu53qk7m32o5iguw42c6knsaj0feuf927agb";
const CONTROLLER: &str = "did:key:z6Mkqtw7Pj5Lv9xc4PgUYAnwfaVoMC6FRneGWVr5ekTEfKVL";
const INIT_ID: &str = "baeabeiajn5ypv2gllvkk4muvzujvcnoen2orknxix7qtil2daqn6vu6khq";
const SEP_KEY: &str = "model";

// Return an builder for an event with the same network,model,controller,stream.
pub(crate) fn event_id_builder() -> Builder<WithInit> {
    EventId::builder()
        .with_network(&Network::DevUnstable)
        .with_sep(SEP_KEY, &multibase::decode(MODEL_ID).unwrap().1)
        .with_controller(CONTROLLER)
        .with_init(&Cid::from_str(INIT_ID).unwrap())
}

// Generate an event for the same network,model,controller,stream
// The event and height are random when when its None.
pub(crate) fn random_event_id(event: Option<&str>) -> EventId {
    event_id_builder()
        .with_event(
            &event
                .map(|cid| Cid::from_str(cid).unwrap())
                .unwrap_or_else(random_cid),
        )
        .build()
}
// The EventId that is the minumum of all possible random event ids
pub(crate) fn random_event_id_min() -> EventId {
    event_id_builder().with_min_event().build_fencepost()
}
// The EventId that is the maximum of all possible random event ids
pub(crate) fn random_event_id_max() -> EventId {
    event_id_builder().with_max_event().build_fencepost()
}

pub(crate) fn random_cid() -> Cid {
    let mut data = [0u8; 8];
    rand::Rng::fill(&mut ::rand::thread_rng(), &mut data);
    let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
    Cid::new_v1(0x00, hash)
}

pub(crate) async fn build_car_file(count: usize) -> (EventId, Vec<Block>, Vec<u8>) {
    let blocks: Vec<Block> = (0..count).map(|_| random_block()).collect();

    let controller = thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(32)
        .map(char::from)
        .collect::<String>();

    let model = StreamId::document(random_cid());
    let unique = gen_rand_bytes::<12>();
    let init = ipld!( {
        "header": {
            "controllers": [controller],
            "model": model.to_vec(),
            "sep": "model",
            "unique": unique.as_slice(),
        },
        "links": blocks.iter().map(|block| Ipld::Link(block.cid)).collect::<Vec<Ipld>>(),
    });

    let commit = DagCborEncoded::new(&init).unwrap();
    let root_cid = Cid::new_v1(
        <DagCborCodec as Codec<Ipld>>::CODE,
        Code::Sha2_256.digest(commit.as_ref()),
    );

    let mut car = Vec::new();
    let roots: Vec<Cid> = vec![root_cid];
    let mut writer = CarWriter::new(CarHeader::V1(roots.into()), &mut car);
    writer.write(root_cid, commit).await.unwrap();
    for block in &blocks {
        writer.write(block.cid, &block.data).await.unwrap();
    }
    writer.finish().await.unwrap();
    let event_id = random_event_id(Some(&root_cid.to_string()));
    (event_id, blocks, car)
}

fn gen_rand_bytes<const SIZE: usize>() -> [u8; SIZE] {
    // can't take &mut rng cause of Send even if we drop it
    let mut rng = thread_rng();
    let mut arr = [0; SIZE];
    for x in &mut arr {
        *x = rng.gen_range(0..=255);
    }
    arr
}

pub(crate) fn random_block() -> Block {
    let mut data = [0u8; 1024];
    rand::Rng::fill(&mut ::rand::thread_rng(), &mut data);
    let hash = Code::Sha2_256.digest(&data);
    Block {
        cid: Cid::new_v1(0x00, hash),
        data: data.to_vec().into(),
    }
}
