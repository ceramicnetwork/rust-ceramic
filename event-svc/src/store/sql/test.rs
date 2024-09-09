use std::str::FromStr;

use ceramic_core::{
    event_id::{Builder, WithInit},
    EventId, Network,
};
use ceramic_event::unvalidated;
use cid::Cid;
use expect_test::expect;
use ipld_core::codec::Codec;
use ipld_core::ipld::Ipld;
use multihash_codetable::{Code, MultihashDigest};
use serde_ipld_dagcbor::codec::DagCborCodec;
use test_log::test;

use crate::store::{CeramicOneEvent, EventInsertable, SqlitePool};

const MODEL_ID: &str = "k2t6wz4yhfp1r5pwi52gw89nzjbu53qk7m32o5iguw42c6knsaj0feuf927agb";
const CONTROLLER: &str = "did:key:z6Mkqtw7Pj5Lv9xc4PgUYAnwfaVoMC6FRneGWVr5ekTEfKVL";
const INIT_ID: &str = "baeabeiajn5ypv2gllvkk4muvzujvcnoen2orknxix7qtil2daqn6vu6khq";
const SEP_KEY: &str = "model";

// Return an builder for an event with the same network,model,controller,stream.
fn event_id_builder() -> Builder<WithInit> {
    EventId::builder()
        .with_network(&Network::DevUnstable)
        .with_sep(SEP_KEY, &multibase::decode(MODEL_ID).unwrap().1)
        .with_controller(CONTROLLER)
        .with_init(&Cid::from_str(INIT_ID).unwrap())
}

fn random_events(num: usize) -> Vec<EventInsertable> {
    let mut events = Vec::with_capacity(num);

    for i in 0..num {
        let header = unvalidated::init::Header::new(
            vec![format!("controller{}", i)],
            "model".to_string(),
            vec![],
            None,
            None,
            None,
        );
        let payload = unvalidated::init::Payload::new(header, None);
        let cid = Cid::new_v1(
            <DagCborCodec as Codec<Ipld>>::CODE,
            Code::Sha2_256.digest(&serde_ipld_dagcbor::to_vec(&payload).unwrap()),
        );
        let order_key = event_id_builder().with_event(&cid).build();
        let event = unvalidated::Event::from(payload);

        events.push(EventInsertable::new(order_key, cid, event, true).unwrap())
    }

    events
}

#[test(tokio::test)]
async fn hash_range_query() {
    let pool = SqlitePool::connect_in_memory().await.unwrap();
    let events = random_events(2);
    let first = &events[0];
    let second = &events[1];

    let x = CeramicOneEvent::insert_many(&pool, [first, second].into_iter())
        .await
        .unwrap();

    assert_eq!(x.count_new_keys(), 2);

    let hash = CeramicOneEvent::hash_range(
        &pool,
        &event_id_builder().with_min_event().build()..&event_id_builder().with_max_event().build(),
    )
    .await
    .unwrap();
    expect!["71F104AFD1BCDBB85C1548F59DFF2A5FB50E21A23F1A65CCB2F38EF6D92FA659#2"]
        .assert_eq(&format!("{hash}"));
}

#[test(tokio::test)]
async fn range_query() {
    let events = random_events(2);
    let first = &events[0];
    let second = &events[1];
    let pool = SqlitePool::connect_in_memory().await.unwrap();
    let x = CeramicOneEvent::insert_many(&pool, [first, second].into_iter())
        .await
        .unwrap();

    assert_eq!(x.count_new_keys(), 2);

    let ids = CeramicOneEvent::range(
        &pool,
        &event_id_builder().with_min_event().build()..&event_id_builder().with_max_event().build(),
        0,
        usize::MAX,
    )
    .await
    .unwrap();

    expect![[r#"
        [
            EventId {
                bytes: "ce010502e320708396e92d964f16d8429ae87f86ead3ca3c0171122073dbe85a09bf83ea51cb249576a2113ca991e77b387c014ec4d7861845c12466",
                network_id: Some(
                    2,
                ),
                separator: Some(
                    "e320708396e92d96",
                ),
                controller: Some(
                    "4f16d8429ae87f86",
                ),
                stream_id: Some(
                    "ead3ca3c",
                ),
                cid: Some(
                    "bafyreidt3pufucn7qpvfdszesv3keej4vgi6o6zypqau5rgxqymelqjemy",
                ),
            },
            EventId {
                bytes: "ce010502e320708396e92d964f16d8429ae87f86ead3ca3c01711220a0e20fa7c043f882d1bca32caeae1a3ba8996ad5b45e0b37b154aac5ea934d92",
                network_id: Some(
                    2,
                ),
                separator: Some(
                    "e320708396e92d96",
                ),
                controller: Some(
                    "4f16d8429ae87f86",
                ),
                stream_id: Some(
                    "ead3ca3c",
                ),
                cid: Some(
                    "bafyreifa4ih2pqcd7cbndpfdfsxk4gr3vcmwvvnulyftpmkuvlc6ve2nsi",
                ),
            },
        ]
    "#]]
        .assert_debug_eq(&ids);
}

#[test(tokio::test)]
async fn undelivered_with_values() {
    let pool = SqlitePool::connect_in_memory().await.unwrap();
    let (res, hw) = CeramicOneEvent::undelivered_with_values(&pool, 0, 10000)
        .await
        .unwrap();
    assert_eq!(res.len(), 0);
    assert_eq!(hw, 0);
}

#[test(tokio::test)]
async fn range_with_values() {
    let pool = SqlitePool::connect_in_memory().await.unwrap();

    let res = CeramicOneEvent::range_with_values(
        &pool,
        &event_id_builder().with_min_event().build()..&event_id_builder().with_max_event().build(),
        0,
        100000,
    )
    .await
    .unwrap();
    assert_eq!(res.len(), 0);
}