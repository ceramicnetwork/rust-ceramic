use std::str::FromStr;

use crate::{CeramicOneEvent, EventInsertable, SqlitePool};
use ceramic_core::{
    event_id::{Builder, WithInit},
    EventId, Network,
};
use ceramic_event::unvalidated;
use cid::Cid;
use expect_test::expect;
use test_log::test;

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

fn random_event(cid: &str) -> EventInsertable {
    let order_key = event_id_builder()
        .with_event(&Cid::from_str(cid).unwrap())
        .build();

    let header =
        unvalidated::init::Header::new(vec![], "model".to_string(), vec![], None, None, None);
    let payload = unvalidated::init::Payload::new(header, None);
    let event = unvalidated::Event::from(payload);
    EventInsertable::new(order_key, event, true)
}

#[test(tokio::test)]
async fn hash_range_query() {
    let pool = SqlitePool::connect_in_memory().await.unwrap();
    let first = random_event("baeabeiazgwnti363jifhxaeaegbluw4ogcd2t5hsjaglo46wuwcgajqa5u");
    let second = random_event("baeabeihyl35xdlfju3zrkvy2exmnl6wics3rc5ppz7hwg7l7g4brbtnpny");

    let x = CeramicOneEvent::insert_many(&pool, [&first, &second].into_iter())
        .await
        .unwrap();

    assert_eq!(x.count_new_keys(), 2);

    let hash = CeramicOneEvent::hash_range(
        &pool,
        &event_id_builder().with_min_event().build()..&event_id_builder().with_max_event().build(),
    )
    .await
    .unwrap();
    expect!["082F8D30F129E0E26C3136F7FE503E4D30EBDDB1EEFFF1EDEF853F2C96A0898E#2"]
        .assert_eq(&format!("{hash}"));
}

#[test(tokio::test)]
async fn range_query() {
    let first = random_event("baeabeichhhmbhsic4maraneqf5gkhekgzcawhtpj3fh6opjtglznapz524");
    let second = random_event("baeabeibmek7v4ljsu575ohgjhovdxhcw6p6oivgb55hzkeap5po7ghzqty");
    let pool = SqlitePool::connect_in_memory().await.unwrap();
    let x = CeramicOneEvent::insert_many(&pool, [&first, &second].into_iter())
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
                bytes: "ce010502e320708396e92d964f16d8429ae87f86ead3ca3c010012202c22bf5e2d32a77fd71cc93baa3b9c56f3fce454c1ef4f95100febddf31f309e",
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
                    "baeabeibmek7v4ljsu575ohgjhovdxhcw6p6oivgb55hzkeap5po7ghzqty",
                ),
            },
            EventId {
                bytes: "ce010502e320708396e92d964f16d8429ae87f86ead3ca3c010012204739d813c902e3011034902f4ca39146c88163cde9d94fe73d3332f2d03f3dd7",
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
                    "baeabeichhhmbhsic4maraneqf5gkhekgzcawhtpj3fh6opjtglznapz524",
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
