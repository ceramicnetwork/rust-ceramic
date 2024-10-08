mod event;
mod migration;
mod ordering;

use std::str::FromStr;

use ceramic_api::ApiItem;
use ceramic_core::{DidDocument, EventId, Network, StreamId};
use ceramic_event::unvalidated::{self, signed};
use cid::Cid;
use ipld_core::{ipld, ipld::Ipld};
use iroh_bitswap::Block;
use multihash_codetable::{Code, MultihashDigest};
use rand::{thread_rng, Rng};
use recon::ReconItem;

const CONTROLLER: &str = "did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw";
const SEP_KEY: &str = "model";
const METAMODEL_STREAM_ID: &str = "kh4q0ozorrgaq2mezktnrmdwleo1d";

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

async fn build_event_fixed_model(model: StreamId, controller: String) -> TestEventInfo {
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

    let event_id = build_event_id(init_cid, init_cid, &model);
    let car = signed.encode_car().unwrap();
    TestEventInfo {
        event_id,
        blocks: vec![
            Block::new(
                signed.encode_envelope().unwrap().into(),
                *signed.envelope_cid(),
            ),
            Block::new(
                signed.encode_payload().unwrap().into(),
                *signed.payload_cid(),
            ),
        ],
        car,
    }
}

pub(crate) async fn build_recon_item_with_controller(controller: String) -> ReconItem<EventId> {
    let model = StreamId::document(random_cid());
    let e = build_event_fixed_model(model, controller).await;
    ReconItem::new(e.event_id, e.car)
}

/// returns (event ID, array of block CIDs, car bytes)
pub(crate) async fn build_event() -> TestEventInfo {
    let model = StreamId::document(random_cid());
    build_event_fixed_model(model, CONTROLLER.to_owned()).await
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

pub(crate) async fn check_deliverable(
    pool: &crate::store::SqlitePool,
    cid: &Cid,
    deliverable: bool,
) {
    let (exists, delivered) = crate::store::CeramicOneEvent::deliverable_by_cid(pool, cid)
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
        .with_controller(CONTROLLER.to_string())
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

// returns init + N events
async fn get_init_plus_n_events_with_model(
    model: &StreamId,
    number: usize,
) -> Vec<ReconItem<EventId>> {
    let signer = Box::new(signer().await);

    let init = init_event(model, &signer).await;
    let init_cid = init.envelope_cid();
    let (event_id, car) = (
        build_event_id(init_cid, init_cid, model),
        init.encode_car().unwrap(),
    );

    let init_cid = event_id.cid().unwrap();

    let mut events = Vec::with_capacity(number);
    events.push(ReconItem::new(event_id, car));
    let mut prev = init_cid;
    for _ in 0..number {
        let data = gen_rand_bytes::<50>();
        let data = ipld!({
            "radius": 1,
            "red": 2,
            "green": 3,
            "blue": 4,
            "raw": data.as_slice(),
        });

        let data = data_event(init_cid, prev, data, &signer).await;
        let (data_id, data_car) = (
            build_event_id(data.envelope_cid(), &init_cid, model),
            data.encode_car().unwrap(),
        );
        prev = data_id.cid().unwrap();
        events.push(ReconItem::new(data_id, data_car));
    }
    events
}

pub(crate) async fn get_events_return_model() -> (StreamId, Vec<ReconItem<EventId>>) {
    let model = StreamId::document(random_cid());
    let events = get_init_plus_n_events_with_model(&model, 3).await;
    (model, events)
}

// builds init -> data -> data that are a stream (will be a different stream each call)
pub(crate) async fn get_events() -> Vec<ReconItem<EventId>> {
    let model = StreamId::document(random_cid());
    get_init_plus_n_events_with_model(&model, 3).await
}

// Get N events with the same model (init + N-1 data events)
pub(crate) async fn get_n_events(number: usize) -> Vec<ReconItem<EventId>> {
    let model = &StreamId::document(random_cid());
    get_init_plus_n_events_with_model(model, number - 1).await
}

/// Generates a sequence of chained events across two different streams.
///
/// This function creates a series of events that are linked together in a specific order,
/// simulating a chain of events across two separate streams. It's useful for testing or
/// simulating complex event sequences.
///
/// # Returns
///
/// A `Vec<ReconItem<EventId>>` containing 5 events:
/// - 3 events for the first stream (1 init event and 2 data events)
/// - 2 events for the second stream (1 init event and 1 data event)
///
/// # Example
///
/// ```rust
/// let chained_events = generate_chained_events().await;
/// assert_eq!(chained_events.len(), 5);
/// ```
pub(crate) async fn generate_chained_events() -> Vec<ApiItem> {
    let mut events: Vec<ApiItem> = Vec::with_capacity(5);

    let signer = Box::new(signer().await);
    let stream_id_1 = create_deterministic_stream_id_model(&[0x01]);
    let stream_id_2 = create_meta_model_stream_id();
    let init_1 = init_event(&stream_id_1, &signer).await;
    let init_1_cid = init_1.envelope_cid();
    let (event_id_1, car_1) = (
        build_event_id(init_1_cid, init_1_cid, &stream_id_1),
        init_1.encode_car().unwrap(),
    );
    let init_1_cid = event_id_1.cid().unwrap();

    let data_1 = data_event(
        init_1_cid,
        init_1_cid,
        ipld!({
            "stream_1" : "data_1"
        }),
        &signer,
    )
    .await;
    let (data_1_id, data_1_car) = (
        build_event_id(data_1.envelope_cid(), &init_1_cid, &stream_id_1),
        data_1.encode_car().unwrap(),
    );

    let data_2 = data_event(
        init_1_cid,
        data_1_id.cid().unwrap(),
        ipld!({
            "stream_1" : "data_2"
        }),
        &signer,
    )
    .await;

    let (data_2_id, data_2_car) = (
        build_event_id(data_2.envelope_cid(), &init_1_cid, &stream_id_1),
        data_2.encode_car().unwrap(),
    );

    let init_2 = init_event(&stream_id_2, &signer).await;
    let init_2_cid = init_2.envelope_cid();
    let (event_id_2, car_2) = (
        build_event_id(init_2_cid, init_2_cid, &stream_id_2),
        init_2.encode_car().unwrap(),
    );
    let init_2_cid = event_id_2.cid().unwrap();

    let data_3 = data_event(
        init_2_cid,
        init_2_cid,
        ipld!({
            "stream2" : "data_1"
        }),
        &signer,
    )
    .await;
    let (data_3_id, data_3_car) = (
        build_event_id(data_3.envelope_cid(), &init_2_cid, &stream_id_2),
        data_3.encode_car().unwrap(),
    );

    // push the events in the order they should be inserted
    events.push(ApiItem::new(event_id_1, car_1));
    events.push(ApiItem::new(data_1_id, data_1_car));
    events.push(ApiItem::new(data_2_id, data_2_car));
    events.push(ApiItem::new(event_id_2, car_2));
    events.push(ApiItem::new(data_3_id, data_3_car));

    events
}

/// Creates a deterministic StreamId of type Model based on the provided initial data.
///
/// This function generates a reproducible StreamId by hashing the input data
/// using SHA-256 and creating a CID (Content Identifier) from the resulting digest.
///
/// # Arguments
///
/// * `initial_data` - A byte slice containing the data to be used for generating the StreamId.
///
/// # Returns
///
/// A `StreamId` that is deterministically generated from the input data.
///
/// # Example
///
/// ```rust
/// let stream_id = create_deterministic_stream_id_model(&[0x01]);
/// ```
fn create_deterministic_stream_id_model(initial_data: &[u8]) -> StreamId {
    let digest = Code::Sha2_256.digest(initial_data);
    let cid = Cid::new_v1(0x55, digest);
    StreamId::model(cid)
}

/// Creates a StreamId for the metamodel stream.
///
/// This function returns a predefined StreamId that represents the metamodel stream.
/// The StreamId is created from a constant string defined elsewhere in the code.
///
/// # Returns
///
/// A `StreamId` representing the metamodel stream.
///
/// # Example
///
/// ```rust
/// let stream_id = create_meta_model_stream_id();
/// assert_eq!(stream_id.to_string(), "kh4q0ozorrgaq2mezktnrmdwleo1d");
/// ```
fn create_meta_model_stream_id() -> StreamId {
    StreamId::from_str(METAMODEL_STREAM_ID).unwrap()
}
