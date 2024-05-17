mod event;
mod interest;
mod ordering;

use std::str::FromStr;

use ceramic_core::{
    event_id::{self, WithInit},
    DidDocument, EventId, Network, StreamId,
};

use ceramic_event::unvalidated::signed;
use cid::Cid;

use ipld_core::ipld;
use iroh_bitswap::Block;

use multihash_codetable::{Code, MultihashDigest};
use rand::{thread_rng, Rng};

const MODEL_ID: &str = "k2t6wz4yhfp1r5pwi52gw89nzjbu53qk7m32o5iguw42c6knsaj0feuf927agb";
const CONTROLLER: &str = "did:key:z6Mkqtw7Pj5Lv9xc4PgUYAnwfaVoMC6FRneGWVr5ekTEfKVL";
const INIT_ID: &str = "baeabeiajn5ypv2gllvkk4muvzujvcnoen2orknxix7qtil2daqn6vu6khq";
const SEP_KEY: &str = "model";

// Return an builder for an event with the same network,model,controller,stream.
pub(crate) fn event_id_builder() -> event_id::Builder<WithInit> {
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

/// returns (event ID, array of block CIDs, car bytes)
pub(crate) async fn build_event() -> (EventId, Vec<Block>, Vec<u8>) {
    let controller = thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(32)
        .map(char::from)
        .collect::<String>();

    let model = StreamId::document(random_cid());
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

    let event_id = random_event_id(Some(signed.envelope_cid().to_string().as_str()));
    let car = signed.encode_car().await.unwrap();
    (
        event_id,
        vec![
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
    )
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
    let (exists, delivered) = ceramic_store::CeramicOneEvent::delivered_by_cid(pool, cid)
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
        DidDocument::new("did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw"),
        "810d51e02cb63066b7d2d2ec67e05e18c29b938412050bdd3c04d878d8001f3c",
    )
    .await
    .unwrap()
}
