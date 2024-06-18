mod event;
mod interest;
mod ordering;

use ceramic_core::{DidDocument, EventId, Network, StreamId};
use ceramic_event::unvalidated::{self, signed};
use cid::Cid;
use ipld_core::{ipld, ipld::Ipld};
use iroh_bitswap::Block;
use multihash_codetable::{Code, MultihashDigest};
use rand::{thread_rng, Rng};

const CONTROLLER: &str = "did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw";
const SEP_KEY: &str = "model";

// Generate an event (sep key and controller are fixed)
pub(crate) fn build_event_id(cid: &Cid, init: &Cid, model: &StreamId) -> EventId {
    EventId::builder()
        .with_network(&Network::DevUnstable)
        .with_sep(SEP_KEY, &model.to_vec())
        .with_controller(CONTROLLER)
        .with_init(init)
        .with_event(cid)
        .build()
}

// The EventId that is the minumum of all possible random event ids for that stream
pub(crate) fn event_id_min(init: &Cid, model: &StreamId) -> EventId {
    EventId::builder()
        .with_network(&Network::DevUnstable)
        .with_sep(SEP_KEY, &model.to_vec())
        .with_controller(CONTROLLER)
        .with_init(init)
        .with_min_event()
        .build_fencepost()
}
// The EventId that is the maximum of all possible random event ids for that stream
pub(crate) fn event_id_max(init: &Cid, model: &StreamId) -> EventId {
    EventId::builder()
        .with_network(&Network::DevUnstable)
        .with_sep(SEP_KEY, &model.to_vec())
        .with_controller(CONTROLLER)
        .with_init(init)
        .with_max_event()
        .build_fencepost()
}

pub(crate) fn random_cid() -> Cid {
    let mut data = [0u8; 8];
    rand::Rng::fill(&mut ::rand::thread_rng(), &mut data);
    let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
    Cid::new_v1(0x00, hash)
}

#[derive(Debug)]
pub(crate) struct TestEventInfo {
    pub(crate) event_id: EventId,
    pub(crate) car: Vec<u8>,
    pub(crate) blocks: Vec<Block>,
}

async fn build_event_fixed_model(model: StreamId) -> TestEventInfo {
    let controller = thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(32)
        .map(char::from)
        .collect::<String>();

    let unique = gen_rand_bytes::<12>();
    let init = ceramic_event::unvalidated::Builder::init()
        .with_controller(controller)
        .with_sep("model".to_string(), model.to_vec())
        .with_unique(unique.to_vec())
        .with_data(ipld!({"radius": 1, "red": 2, "green": 3, "blue": 4}))
        .build();

    let signer = crate::tests::signer().await;
    let signed =
        signed::Event::from_payload(ceramic_event::unvalidated::Payload::Init(init), signer)
            .unwrap();
    let init_cid = signed.envelope_cid();

    let event_id = build_event_id(&init_cid, &init_cid, &model);
    let car = signed.encode_car().await.unwrap();
    TestEventInfo {
        event_id,
        blocks: vec![
            Block::new(
                signed.encode_envelope().unwrap().into(),
                signed.envelope_cid(),
            ),
            Block::new(
                signed.encode_payload().unwrap().into(),
                signed.payload_cid(),
            ),
        ],
        car,
    }
}

/// returns (event ID, array of block CIDs, car bytes)
pub(crate) async fn build_event() -> TestEventInfo {
    let model = StreamId::document(random_cid());
    build_event_fixed_model(model).await
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

pub(crate) async fn check_deliverable(
    pool: &ceramic_store::SqlitePool,
    cid: &Cid,
    deliverable: bool,
) {
    let (exists, delivered) = ceramic_store::CeramicOneEvent::deliverable_by_cid(pool, cid)
        .await
        .unwrap();
    assert!(exists);
    if deliverable {
        assert!(delivered, "{} should be delivered", cid);
    } else {
        assert!(!delivered, "{} should NOT be delivered", cid);
    }
}

pub(crate) async fn signer() -> signed::JwkSigner {
    signed::JwkSigner::new(
        DidDocument::new(CONTROLLER),
        "810d51e02cb63066b7d2d2ec67e05e18c29b938412050bdd3c04d878d8001f3c",
    )
    .await
    .unwrap()
}

async fn init_event(model: &StreamId, signer: &signed::JwkSigner) -> signed::Event<Ipld> {
    let init = unvalidated::Builder::init()
        .with_controller("controller".to_string())
        .with_sep("model".to_string(), model.to_vec())
        .build();
    signed::Event::from_payload(unvalidated::Payload::Init(init), signer.to_owned()).unwrap()
}

async fn data_event(
    init_id: Cid,
    prev: Cid,
    data: Ipld,
    signer: &signed::JwkSigner,
) -> signed::Event<Ipld> {
    let commit = unvalidated::Builder::data()
        .with_id(init_id)
        .with_prev(prev)
        .with_data(data)
        .build();

    signed::Event::from_payload(unvalidated::Payload::Data(commit), signer.to_owned()).unwrap()
}

async fn get_events_with_model(model: &StreamId) -> [(EventId, Vec<u8>); 3] {
    let signer = Box::new(signer().await);

    let data = gen_rand_bytes::<50>();
    let data2 = gen_rand_bytes::<50>();

    let data = ipld!({
        "radius": 1,
        "red": 2,
        "green": 3,
        "blue": 4,
        "raw": data.as_slice(),
    });

    let data2 = ipld!({
        "radius": 1,
        "red": 2,
        "green": 3,
        "blue": 4,
        "raw": data2.as_slice(),
    });

    let init = init_event(model, &signer).await;
    let init_cid = init.envelope_cid();
    let (event_id, car) = (
        build_event_id(&init_cid, &init_cid, model),
        init.encode_car().await.unwrap(),
    );

    let init_cid = event_id.cid().unwrap();
    let data = data_event(init_cid, init_cid, data, &signer).await;
    let cid = data.envelope_cid();
    let (data_id, data_car) = (
        build_event_id(&data.envelope_cid(), &init_cid, model),
        data.encode_car().await.unwrap(),
    );
    let data2 = data_event(init_cid, cid, data2, &signer).await;
    let (data_id_2, data_car_2) = (
        build_event_id(&data2.envelope_cid(), &init_cid, model),
        data2.encode_car().await.unwrap(),
    );

    [
        (event_id, car),
        (data_id, data_car),
        (data_id_2, data_car_2),
    ]
}

pub(crate) async fn get_events_return_model() -> (StreamId, [(EventId, Vec<u8>); 3]) {
    let model = StreamId::document(random_cid());
    let events = get_events_with_model(&model).await;
    (model, events)
}

// builds init -> data -> data that are a stream (will be a different stream each call)
pub(crate) async fn get_events() -> [(EventId, Vec<u8>); 3] {
    let model = StreamId::document(random_cid());
    get_events_with_model(&model).await
}
