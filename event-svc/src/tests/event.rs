use std::str::FromStr;

use crate::EventService;
use anyhow::Error;
use bytes::Bytes;
use ceramic_api::{ApiItem, EventService as ApiEventService};
use ceramic_core::NodeId;
use ceramic_flight::server::ConclusionFeed as _;
use ceramic_flight::ConclusionEvent;
use ceramic_sql::sqlite::SqlitePool;
use cid::{Cid, CidGeneric};
use expect_test::expect;
use iroh_bitswap::Store;
use itertools::Itertools;
use prettytable::{Cell, Row, Table};
use recon::Sha256a;
use recon::{InsertResult, ReconItem};
use test_log::test;

use super::*;

macro_rules! test_with_sqlite {
    ($test_name: ident, $test_fn: expr $(, $sql_stmts:expr)?) => {
        paste::paste! {
            #[test_log::test(tokio::test)]
            async fn [<$test_name _sqlite>]() {

                let conn = $crate::store::SqlitePool::connect_in_memory().await.unwrap();
                let store = $crate::EventService::try_new(conn, true, true, vec![]).await.unwrap();
                $(
                    for stmt in $sql_stmts {
                        store.pool.run_statement(stmt).await.unwrap();
                    }
                )?
                $test_fn(store).await;
            }
        }
    };
}

/// test_name (will generate multiple in the future when we have multiple backends)
/// test_fn (the test function that will be run for both databases)
/// sql_stmts (optional, array of sql statements to run before the test)
macro_rules! test_with_dbs {
    ($test_name: ident, $test_fn: expr $(, $sql_stmts:expr)?) => {
        test_with_sqlite!($test_name, $test_fn $(, $sql_stmts)?);
    }
}

test_with_dbs!(
    range_query_with_values,
    range_query_with_values,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);

async fn range_query_with_values<S>(store: S)
where
    S: recon::Store<Key = EventId, Hash = Sha256a>,
{
    let (model, events) = get_events_return_model().await;
    let one = &events[0];
    let two = &events[1];
    let init_cid = one.key.cid().unwrap();
    let min_id = event_id_min(&init_cid, &model);
    let max_id = event_id_max(&init_cid, &model);
    recon::Store::insert_many(&store, &[one.clone()], NodeId::random().0)
        .await
        .unwrap();
    recon::Store::insert_many(&store, &[two.clone()], NodeId::random().0)
        .await
        .unwrap();
    let values: Vec<(EventId, Vec<u8>)> =
        recon::Store::range_with_values(&store, &min_id..&max_id, 0, usize::MAX)
            .await
            .unwrap()
            .collect();

    let mut expected = vec![
        (one.key.to_owned(), one.value.to_vec()),
        (two.key.to_owned(), two.value.to_vec()),
    ];
    expected.sort();
    assert_eq!(expected, values);
}

test_with_dbs!(
    double_insert,
    double_insert,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);
async fn double_insert<S>(store: S)
where
    S: recon::Store<Key = EventId, Hash = Sha256a>,
{
    let TestEventInfo {
        event_id: id, car, ..
    } = build_event().await;
    let item = &[ReconItem::new(id, car)];

    // first insert reports its a new key
    assert!(recon::Store::insert_many(&store, item, NodeId::random().0)
        .await
        .unwrap()
        .included_new_key());

    // second insert of same key reports it already existed
    assert!(!recon::Store::insert_many(&store, item, NodeId::random().0)
        .await
        .unwrap()
        .included_new_key());
}

test_with_dbs!(
    try_update_value,
    try_update_value,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);
async fn try_update_value<S>(store: S)
where
    S: recon::Store<Key = EventId, Hash = Sha256a>,
{
    let TestEventInfo {
        event_id: id,
        car: car1,
        ..
    } = build_event().await;
    let TestEventInfo { car: car2, .. } = build_event().await;
    let expected = hex::encode(&car1);

    let actual = recon::Store::insert_many(
        &store,
        &[ReconItem::new(id.clone(), car1)],
        NodeId::random().0,
    )
    .await
    .unwrap();
    assert_eq!(actual, InsertResult::new(1));

    let res = recon::Store::insert_many(
        &store,
        &[ReconItem::new(id.clone(), car2)],
        NodeId::random().0,
    )
    .await
    .unwrap();

    assert_eq!(1, res.invalid.len());
    let invalid = res.invalid.first().unwrap();
    match invalid {
        // Event ID does not match the root CID of the CAR file
        recon::InvalidItem::InvalidFormat { key } => assert_eq!(key, &id),
        recon::InvalidItem::InvalidSignature { .. } => unreachable!("Should not happen"),
    }

    assert_eq!(
        expected,
        hex::encode(
            recon::Store::value_for_key(&store, &id)
                .await
                .unwrap()
                .unwrap()
        ),
    );
}

test_with_dbs!(
    store_value_for_key,
    store_value_for_key,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);
async fn store_value_for_key<S>(store: S)
where
    S: recon::Store<Key = EventId, Hash = Sha256a>,
{
    let TestEventInfo {
        event_id: key,
        car: store_value,
        ..
    } = build_event().await;
    let expected = hex::encode(&store_value);
    recon::Store::insert_many(
        &store,
        &[ReconItem::new(key.clone(), store_value)],
        NodeId::random().0,
    )
    .await
    .unwrap();
    let value = recon::Store::value_for_key(&store, &key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expected, hex::encode(value));
}

test_with_dbs!(
    read_value_as_block,
    read_value_as_block,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);
async fn read_value_as_block<S>(store: S)
where
    S: recon::Store<Key = EventId, Hash = Sha256a> + iroh_bitswap::Store,
{
    let TestEventInfo {
        event_id: key,
        car: store_value,
        blocks,
        ..
    } = build_event().await;
    let expected = hex::encode(&store_value);
    recon::Store::insert_many(
        &store,
        &[ReconItem::new(key.clone(), store_value)],
        NodeId::random().0,
    )
    .await
    .unwrap();
    let value = recon::Store::value_for_key(&store, &key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expected, hex::encode(value));

    // Read each block from the CAR
    for block in blocks {
        let value = iroh_bitswap::Store::get(&store, &block.cid).await.unwrap();
        assert_eq!(block, value);
    }
}

// stores 3 keys with 3,5,10 block long CAR files
// each one takes n+1 blocks as it needs to store the root and all blocks so we expect 3+5+10+3=21 blocks
// but we use a delivered integer per event, so we expect it to increment by 1 for each event
async fn prep_highwater_tests(store: &dyn ApiEventService) -> (Cid, Cid, Cid) {
    let mut keys = Vec::with_capacity(3);
    for _ in 0..3 {
        let TestEventInfo {
            event_id: key,
            car: store_value,
            ..
        } = build_event().await;
        keys.push(ceramic_api::ApiItem::new(key, store_value));
    }
    let res = (
        keys[0].key.cid().unwrap(),
        keys[1].key.cid().unwrap(),
        keys[2].key.cid().unwrap(),
    );
    store.insert_many(keys, NodeId::random().0).await.unwrap();
    res
}

test_with_dbs!(
    events_since_highwater_mark_all_global_counter_with_data,
    events_since_highwater_mark_all_global_counter_with_data,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);
async fn events_since_highwater_mark_all_global_counter_with_data<S>(store: S)
where
    S: ApiEventService,
{
    events_since_highwater_mark_all_global_counter(store, true).await;
}

test_with_dbs!(
    events_since_highwater_mark_all_global_counter_no_data,
    events_since_highwater_mark_all_global_counter_no_data,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);
async fn events_since_highwater_mark_all_global_counter_no_data<S>(store: S)
where
    S: ApiEventService,
{
    events_since_highwater_mark_all_global_counter(store, false).await;
}

async fn events_since_highwater_mark_all_global_counter(
    store: impl ApiEventService,
    include_data: bool,
) {
    let include_data = if include_data {
        ceramic_api::IncludeEventData::Full
    } else {
        ceramic_api::IncludeEventData::None
    };
    let (key_a, key_b, key_c) = prep_highwater_tests(&store).await;

    let (hw, res) = store
        .events_since_highwater_mark(0, 10, include_data)
        .await
        .unwrap();
    let res = res.into_iter().map(|r| r.id).collect::<Vec<_>>();
    assert_eq!(3, res.len(), "include_data={:?}", include_data);
    assert!(hw >= 4); // THIS IS THE GLOBAL COUNTER. we have 3 rows in the db we have a counter of 4 or more
    let exp = [key_a, key_b, key_c];
    assert_eq!(exp, res.as_slice());
}

test_with_dbs!(
    events_since_highwater_mark_limit_1_with_data,
    events_since_highwater_mark_limit_1_with_data,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);
async fn events_since_highwater_mark_limit_1_with_data<S>(store: S)
where
    S: ApiEventService,
{
    events_since_highwater_mark_limit_1(store, true).await;
}

test_with_dbs!(
    events_since_highwater_mark_limit_1_no_data,
    events_since_highwater_mark_limit_1_no_data,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);
async fn events_since_highwater_mark_limit_1_no_data<S>(store: S)
where
    S: ApiEventService,
{
    events_since_highwater_mark_limit_1(store, false).await;
}

async fn events_since_highwater_mark_limit_1<S>(store: S, include_data: bool)
where
    S: ApiEventService,
{
    let include_data = if include_data {
        ceramic_api::IncludeEventData::Full
    } else {
        ceramic_api::IncludeEventData::None
    };
    let (key_a, _key_b, _key_c) = prep_highwater_tests(&store).await;
    let (hw_og, res) = store
        .events_since_highwater_mark(0, 1, include_data)
        .await
        .unwrap();
    let res = res.into_iter().map(|r| r.id).collect::<Vec<_>>();
    assert_eq!(1, res.len(), "include_data={:?}", include_data);
    assert!(hw_og >= 2); // other tests might be incrementing the count. but we should have at least 2 and it shouldn't change between calls
    let (hw, res) = store
        .events_since_highwater_mark(0, 1, include_data)
        .await
        .unwrap();
    let res = res.into_iter().map(|r| r.id).collect::<Vec<_>>();
    assert_eq!(hw_og, hw);
    assert_eq!([key_a], res.as_slice());
}

test_with_dbs!(
    events_since_highwater_mark_middle_start_with_data,
    events_since_highwater_mark_middle_start_with_data,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);
async fn events_since_highwater_mark_middle_start_with_data(store: impl ApiEventService) {
    events_since_highwater_mark_middle_start(store, true).await;
}

test_with_dbs!(
    events_since_highwater_mark_middle_start_no_data,
    events_since_highwater_mark_middle_start_no_data,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);
async fn events_since_highwater_mark_middle_start_no_data(store: impl ApiEventService) {
    events_since_highwater_mark_middle_start(store, false).await;
}

async fn events_since_highwater_mark_middle_start(store: impl ApiEventService, include_data: bool) {
    let include_data = if include_data {
        ceramic_api::IncludeEventData::Full
    } else {
        ceramic_api::IncludeEventData::None
    };
    let (key_a, key_b, key_c) = prep_highwater_tests(&store).await;

    // starting at rowid 1 which is in the middle of key A should still return key A
    let (hw, res) = store
        .events_since_highwater_mark(1, 2, include_data)
        .await
        .unwrap();
    let res = res.into_iter().map(|r| r.id).collect::<Vec<_>>();
    assert_eq!(2, res.len(), "include_data={:?}", include_data);
    assert!(hw >= 3);
    assert_eq!([key_a, key_b], res.as_slice());

    let (hw, res) = store
        .events_since_highwater_mark(hw, 1, include_data)
        .await
        .unwrap();
    let res = res.into_iter().map(|r| r.id).collect::<Vec<_>>();
    assert_eq!(1, res.len());
    assert!(hw >= 4);
    assert_eq!([key_c], res.as_slice());

    let (hw, res) = store
        .events_since_highwater_mark(hw, 1, include_data)
        .await
        .unwrap();
    assert_eq!(0, res.len());
    assert!(hw >= 4); // previously returned 0
}

test_with_dbs!(
    get_event_by_event_id,
    get_event_by_event_id,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);

async fn get_event_by_event_id<S>(store: S)
where
    S: ApiEventService,
{
    let TestEventInfo {
        event_id: key,
        car: store_value,
        ..
    } = build_event().await;
    let item = ApiItem::new(key, store_value);
    store
        .insert_many(vec![item.clone()], NodeId::random().0)
        .await
        .unwrap();

    let res = store.value_for_order_key(&item.key).await.unwrap().unwrap();
    assert_eq!(&res, item.value.as_ref());
}

test_with_dbs!(
    get_event_by_cid,
    get_event_by_cid,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);

async fn get_event_by_cid<S>(store: S)
where
    S: ApiEventService,
{
    let TestEventInfo {
        event_id: key,
        car: store_value,
        ..
    } = build_event().await;
    let item = ApiItem::new(key, store_value);

    store
        .insert_many(vec![item.clone()], NodeId::random().0)
        .await
        .unwrap();

    let res = store
        .value_for_cid(&item.key.cid().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&res, item.value.as_ref());
}

test_with_dbs!(
    test_store_block,
    test_store_block,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block"
    ]
);

async fn test_store_block<S>(store: S)
where
    S: iroh_bitswap::Store,
{
    let data: Bytes = hex::decode("0a050001020304").unwrap().into();
    let cid: CidGeneric<64> =
        Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line

    let result = store.put(&Block { cid, data }).await.unwrap();
    // assert the block is new
    assert!(result);

    let has: Result<bool, Error> = Store::has(&store, &cid).await;
    expect![["true"]].assert_eq(&has.unwrap().to_string());

    let size: Result<usize, Error> = Store::get_size(&store, &cid).await;
    expect![["7"]].assert_eq(&size.unwrap().to_string());

    let block = Store::get(&store, &cid).await.unwrap();
    expect!["bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom"] // cspell:disable-line
        .assert_eq(&block.cid().to_string());
    expect![["0A050001020304"]].assert_eq(&hex::encode_upper(block.data()));
}

test_with_dbs!(
    test_double_store_block,
    test_double_store_block,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block"
    ]
);
async fn test_double_store_block<S>(store: S)
where
    S: iroh_bitswap::Store,
{
    let data: Bytes = hex::decode("0a050001020304").unwrap().into();
    let cid: CidGeneric<64> =
        Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line

    let blob = Block { cid, data };
    let result = store.put(&blob).await;
    // Assert that the block is new
    assert!(result.unwrap());

    // Try to put the block again
    let result = store.put(&blob).await;
    // Assert that the block already existed
    assert!(!result.unwrap());

    let has: Result<bool, Error> = Store::has(&store, &cid).await;
    expect![["true"]].assert_eq(&has.unwrap().to_string());

    let size: Result<usize, Error> = Store::get_size(&store, &cid).await;
    expect![["7"]].assert_eq(&size.unwrap().to_string());

    let block = Store::get(&store, &cid).await.unwrap();
    expect!["bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom"] // cspell:disable-line
        .assert_eq(&block.cid().to_string());
    expect![["0A050001020304"]].assert_eq(&hex::encode_upper(block.data()));
}

test_with_dbs!(
    test_get_nonexistent_block,
    test_get_nonexistent_block,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block"
    ]
);
async fn test_get_nonexistent_block<S>(store: S)
where
    S: iroh_bitswap::Store,
{
    let cid = Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line
    let exists = iroh_bitswap::Store::has(&store, &cid).await.unwrap();
    assert!(!exists);
    let err = store.get(&cid).await.unwrap_err().to_string();
    assert!(
        err.contains(
            "block bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom does not exist"
        ),
        "{}",
        err
    );
}

#[test(tokio::test)]
async fn test_conclusion_events_since() -> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePool::connect_in_memory().await?;
    let service = EventService::try_new(pool, false, false, vec![]).await?;
    let test_events = generate_chained_events().await;

    ceramic_api::EventService::insert_many(
        &service,
        test_events
            .into_iter()
            .map(|(event_id, event)| {
                ApiItem::new(
                    event_id,
                    event.encode_car().expect("test event should encode"),
                )
            })
            .collect(),
        NodeId::random().0,
    )
    .await?;

    // Fetch conclusion events
    let conclusion_events = service.conclusion_events_since(0, 6).await?;

    expect![[r#"
        +-------+------------+---------------------------------------------------------------+-------------+----------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------------------------------------------+
        | index | event_type | stream_cid                                                    | stream_type | controller                                               | dimensions                                                                                                                                                                                                                             | event_cid                                                     | data                                       | previous                                                             |
        +-------+------------+---------------------------------------------------------------+-------------+----------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------------------------------------------+
        | 1     | Data       | bagcqcerahx5i27vqxigdq3xulceu5qv6yzdvxzamfueubsyxam5kmjcpp45q | 3           | did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw | [model: "ce0102015512204bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a", controller: "6469643a6b65793a7a364d6b6b337274666f4b444d4d47347a7961724e4777435173343447535134397063594b517370484a5058536e5677", context: ""] | bagcqcerahx5i27vqxigdq3xulceu5qv6yzdvxzamfueubsyxam5kmjcpp45q | 6e756c6c                                   | []                                                                   |
        +-------+------------+---------------------------------------------------------------+-------------+----------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------------------------------------------+
        | 2     | Data       | bafyreihum3smvdc36yl2qnbl4gqv3nfxwdxj7v2zdozcrrqgmtc3zfhb7i   | 2           | did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw | [model: "ce01040171710b0009686d6f64656c2d7631", controller: "6469643a6b65793a7a364d6b6b337274666f4b444d4d47347a7961724e4777435173343447535134397063594b517370484a5058536e5677", context: ""]                                           | bafyreihum3smvdc36yl2qnbl4gqv3nfxwdxj7v2zdozcrrqgmtc3zfhb7i   | 6e756c6c                                   | []                                                                   |
        +-------+------------+---------------------------------------------------------------+-------------+----------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------------------------------------------+
        | 3     | Data       | bafyreihum3smvdc36yl2qnbl4gqv3nfxwdxj7v2zdozcrrqgmtc3zfhb7i   | 2           | did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw | [model: "ce01040171710b0009686d6f64656c2d7631", controller: "6469643a6b65793a7a364d6b6b337274666f4b444d4d47347a7961724e4777435173343447535134397063594b517370484a5058536e5677", context: ""]                                           | bagcqcerap4iyp25kvufzcesbi4ijfseeb4koayw2m2y2wtk3pkm3nb2iezaq | 7b2273747265616d32223a22646174615f31227d   | [Cid(bafyreihum3smvdc36yl2qnbl4gqv3nfxwdxj7v2zdozcrrqgmtc3zfhb7i)]   |
        +-------+------------+---------------------------------------------------------------+-------------+----------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------------------------------------------+
        | 4     | Data       | bagcqcerahx5i27vqxigdq3xulceu5qv6yzdvxzamfueubsyxam5kmjcpp45q | 3           | did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw | [model: "ce0102015512204bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a", controller: "6469643a6b65793a7a364d6b6b337274666f4b444d4d47347a7961724e4777435173343447535134397063594b517370484a5058536e5677", context: ""] | bagcqcerakug4jvwbhisuo4zlhzkinwfca2dbcv63ea7jan27zlwhzpxyrleq | 7b2273747265616d5f31223a22646174615f31227d | [Cid(bagcqcerahx5i27vqxigdq3xulceu5qv6yzdvxzamfueubsyxam5kmjcpp45q)] |
        +-------+------------+---------------------------------------------------------------+-------------+----------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------------------------------------------+
        | 5     | Time       | bafyreihum3smvdc36yl2qnbl4gqv3nfxwdxj7v2zdozcrrqgmtc3zfhb7i   | 2           | did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw | [model: "ce01040171710b0009686d6f64656c2d7631", controller: "6469643a6b65793a7a364d6b6b337274666f4b444d4d47347a7961724e4777435173343447535134397063594b517370484a5058536e5677", context: ""]                                           | bafyreiftj6l432kco7hnb6reklbd7bh2j4jbg5beuvtxp3rhgny7omgali   |                                            | [Cid(bagcqcerap4iyp25kvufzcesbi4ijfseeb4koayw2m2y2wtk3pkm3nb2iezaq)] |
        +-------+------------+---------------------------------------------------------------+-------------+----------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------------------------------------------+
        | 6     | Data       | bagcqcerahx5i27vqxigdq3xulceu5qv6yzdvxzamfueubsyxam5kmjcpp45q | 3           | did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw | [model: "ce0102015512204bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a", controller: "6469643a6b65793a7a364d6b6b337274666f4b444d4d47347a7961724e4777435173343447535134397063594b517370484a5058536e5677", context: ""] | bagcqceraccgbaicjznz45ov4wgc3wnr62zqwba24sxzreqerlzdklidysfdq | 7b2273747265616d5f31223a22646174615f32227d | [Cid(bagcqcerakug4jvwbhisuo4zlhzkinwfca2dbcv63ea7jan27zlwhzpxyrleq)] |
        +-------+------------+---------------------------------------------------------------+-------------+----------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------------------------------------------+
    "#]].assert_eq(&events_to_table(&conclusion_events));

    // Fetch conclusion events, with non zero watermark
    let conclusion_events = service.conclusion_events_since(3, 6).await?;

    expect![[r#"
        +-------+------------+---------------------------------------------------------------+-------------+----------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------------------------------------------+
        | index | event_type | stream_cid                                                    | stream_type | controller                                               | dimensions                                                                                                                                                                                                                             | event_cid                                                     | data                                       | previous                                                             |
        +-------+------------+---------------------------------------------------------------+-------------+----------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------------------------------------------+
        | 4     | Data       | bagcqcerahx5i27vqxigdq3xulceu5qv6yzdvxzamfueubsyxam5kmjcpp45q | 3           | did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw | [model: "ce0102015512204bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a", controller: "6469643a6b65793a7a364d6b6b337274666f4b444d4d47347a7961724e4777435173343447535134397063594b517370484a5058536e5677", context: ""] | bagcqcerakug4jvwbhisuo4zlhzkinwfca2dbcv63ea7jan27zlwhzpxyrleq | 7b2273747265616d5f31223a22646174615f31227d | [Cid(bagcqcerahx5i27vqxigdq3xulceu5qv6yzdvxzamfueubsyxam5kmjcpp45q)] |
        +-------+------------+---------------------------------------------------------------+-------------+----------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------------------------------------------+
        | 5     | Time       | bafyreihum3smvdc36yl2qnbl4gqv3nfxwdxj7v2zdozcrrqgmtc3zfhb7i   | 2           | did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw | [model: "ce01040171710b0009686d6f64656c2d7631", controller: "6469643a6b65793a7a364d6b6b337274666f4b444d4d47347a7961724e4777435173343447535134397063594b517370484a5058536e5677", context: ""]                                           | bafyreiftj6l432kco7hnb6reklbd7bh2j4jbg5beuvtxp3rhgny7omgali   |                                            | [Cid(bagcqcerap4iyp25kvufzcesbi4ijfseeb4koayw2m2y2wtk3pkm3nb2iezaq)] |
        +-------+------------+---------------------------------------------------------------+-------------+----------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------------------------------------------+
        | 6     | Data       | bagcqcerahx5i27vqxigdq3xulceu5qv6yzdvxzamfueubsyxam5kmjcpp45q | 3           | did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw | [model: "ce0102015512204bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a", controller: "6469643a6b65793a7a364d6b6b337274666f4b444d4d47347a7961724e4777435173343447535134397063594b517370484a5058536e5677", context: ""] | bagcqceraccgbaicjznz45ov4wgc3wnr62zqwba24sxzreqerlzdklidysfdq | 7b2273747265616d5f31223a22646174615f32227d | [Cid(bagcqcerakug4jvwbhisuo4zlhzkinwfca2dbcv63ea7jan27zlwhzpxyrleq)] |
        +-------+------------+---------------------------------------------------------------+-------------+----------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------------------------------------------+
    "#]].assert_eq(&events_to_table(&conclusion_events));

    Ok(())
}

fn events_to_table(conclusion_events: &[ConclusionEvent]) -> String {
    // Create a table
    let mut table = Table::new();
    table.add_row(Row::new(vec![
        Cell::new("index"),
        Cell::new("event_type"),
        Cell::new("stream_cid"),
        Cell::new("stream_type"),
        Cell::new("controller"),
        Cell::new("dimensions"),
        Cell::new("event_cid"),
        Cell::new("data"),
        Cell::new("previous"),
    ]));

    for event in conclusion_events {
        let (
            event_type,
            index,
            stream_cid,
            stream_type,
            controller,
            dimensions,
            event_cid,
            data,
            previous,
        ) = match event {
            ConclusionEvent::Data(data_event) => (
                "Data",
                &data_event.index,
                &data_event.init.stream_cid,
                &data_event.init.stream_type,
                &data_event.init.controller,
                &data_event.init.dimensions,
                &data_event.event_cid,
                hex::encode(&data_event.data),
                format!("{:?}", data_event.previous),
            ),
            ConclusionEvent::Time(time_event) => (
                "Time",
                &time_event.index,
                &time_event.init.stream_cid,
                &time_event.init.stream_type,
                &time_event.init.controller,
                &time_event.init.dimensions,
                &time_event.event_cid,
                String::new(), // Time events don't have data
                format!("{:?}", time_event.previous),
            ),
        };

        table.add_row(Row::new(vec![
            Cell::new(&index.to_string()),
            Cell::new(event_type),
            Cell::new(&stream_cid.to_string()),
            Cell::new(&stream_type.to_string()),
            Cell::new(controller),
            Cell::new(&format!(
                "[{}]",
                dimensions
                    .iter()
                    .map(|(key, value)| format!("{key}: \"{}\"", hex::encode(value)))
                    .join(", ")
            )),
            Cell::new(&event_cid.to_string()),
            Cell::new(&data),
            Cell::new(&previous),
        ]));
    }

    table.to_string()
}
