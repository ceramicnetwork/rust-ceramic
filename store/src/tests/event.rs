use std::str::FromStr;

use anyhow::Error;
use bytes::Bytes;
use ceramic_api::AccessModelStore;
use cid::{Cid, CidGeneric};
use expect_test::expect;
use iroh_bitswap::Store;
use recon::{Key, ReconItem, Sha256a};

use super::*;

macro_rules! test_with_sqlite {
    ($test_name: ident, $test_fn: expr $(, $sql_stmts:expr)?) => {
        paste::paste! {
            #[tokio::test]
            async fn [<$test_name _sqlite>]() {
                let _  =ceramic_metrics::init_local_tracing();

                let conn = $crate::sql::SqlitePool::connect_in_memory().await.unwrap();
                let store = $crate::SqliteEventStore::new(conn).await.unwrap();
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
    hash_range_query,
    hash_range_query,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);

async fn hash_range_query<S>(store: S)
where
    S: recon::Store<Key = EventId, Hash = Sha256a>,
{
    recon::Store::insert(
        &store,
        &ReconItem::new_key(&random_event_id(
            Some(1),
            Some("baeabeiazgwnti363jifhxaeaegbluw4ogcd2t5hsjaglo46wuwcgajqa5u"),
        )),
    )
    .await
    .unwrap();
    recon::Store::insert(
        &store,
        &ReconItem::new_key(&random_event_id(
            Some(2),
            Some("baeabeihyl35xdlfju3zrkvy2exmnl6wics3rc5ppz7hwg7l7g4brbtnpny"),
        )),
    )
    .await
    .unwrap();
    let hash = recon::Store::hash_range(&store, &random_event_id_min(), &random_event_id_max())
        .await
        .unwrap();
    expect!["65C7A25327CC05C19AB5812103EEB8D1156595832B453C7BAC6A186F4811FA0A#2"]
        .assert_eq(&format!("{hash}"));
}

test_with_dbs!(
    range_query,
    range_query,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);

async fn range_query<S>(store: S)
where
    S: recon::Store<Key = EventId, Hash = Sha256a>,
{
    recon::Store::insert(
        &store,
        &ReconItem::new_key(&random_event_id(
            Some(1),
            Some("baeabeichhhmbhsic4maraneqf5gkhekgzcawhtpj3fh6opjtglznapz524"),
        )),
    )
    .await
    .unwrap();
    recon::Store::insert(
        &store,
        &ReconItem::new_key(&random_event_id(
            Some(2),
            Some("baeabeibmek7v4ljsu575ohgjhovdxhcw6p6oivgb55hzkeap5po7ghzqty"),
        )),
    )
    .await
    .unwrap();
    let ids = recon::Store::range(
        &store,
        &random_event_id_min(),
        &random_event_id_max(),
        0,
        usize::MAX,
    )
    .await
    .unwrap();
    expect![[r#"
            [
                EventId {
                    bytes: "ce010502b51217a029eb540d4f16d8429ae87f86ead3ca3c01010012204739d813c902e3011034902f4ca39146c88163cde9d94fe73d3332f2d03f3dd7",
                    network_id: Some(
                        2,
                    ),
                    separator: Some(
                        "b51217a029eb540d",
                    ),
                    controller: Some(
                        "4f16d8429ae87f86",
                    ),
                    stream_id: Some(
                        "ead3ca3c",
                    ),
                    event_height: Some(
                        1,
                    ),
                    cid: Some(
                        "baeabeichhhmbhsic4maraneqf5gkhekgzcawhtpj3fh6opjtglznapz524",
                    ),
                },
                EventId {
                    bytes: "ce010502b51217a029eb540d4f16d8429ae87f86ead3ca3c02010012202c22bf5e2d32a77fd71cc93baa3b9c56f3fce454c1ef4f95100febddf31f309e",
                    network_id: Some(
                        2,
                    ),
                    separator: Some(
                        "b51217a029eb540d",
                    ),
                    controller: Some(
                        "4f16d8429ae87f86",
                    ),
                    stream_id: Some(
                        "ead3ca3c",
                    ),
                    event_height: Some(
                        2,
                    ),
                    cid: Some(
                        "baeabeibmek7v4ljsu575ohgjhovdxhcw6p6oivgb55hzkeap5po7ghzqty",
                    ),
                },
            ]
        "#]]
        .assert_debug_eq(&ids.collect::<Vec<EventId>>());
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
    // Write three keys, two with values and one without
    let one_id = random_event_id(
        Some(1),
        Some("baeabeichhhmbhsic4maraneqf5gkhekgzcawhtpj3fh6opjtglznapz524"),
    );
    let two_id = random_event_id(
        Some(2),
        Some("baeabeibmek7v4ljsu575ohgjhovdxhcw6p6oivgb55hzkeap5po7ghzqty"),
    );
    let (_one_blocks, one_car) = build_car_file(2).await;
    let (_two_blocks, two_car) = build_car_file(3).await;
    recon::Store::insert(&store, &ReconItem::new(&one_id, Some(&one_car)))
        .await
        .unwrap();
    recon::Store::insert(&store, &ReconItem::new(&two_id, Some(&two_car)))
        .await
        .unwrap();
    // Insert new event without a value to ensure we skip it in the query
    recon::Store::insert(
        &store,
        &ReconItem::new(
            &random_event_id(
                Some(2),
                Some("baeabeicyxeqioadjgy6v6cpy62a3gngylax54sds7rols2b67yetzaw5r4"),
            ),
            None,
        ),
    )
    .await
    .unwrap();
    let values: Vec<(EventId, Vec<u8>)> = recon::Store::range_with_values(
        &store,
        &random_event_id_min(),
        &random_event_id_max(),
        0,
        usize::MAX,
    )
    .await
    .unwrap()
    .collect();

    assert_eq!(vec![(one_id, one_car), (two_id, two_car)], values);
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
    let id = random_event_id(Some(10), None);

    // first insert reports its a new key
    expect![
        r#"
            Ok(
                true,
            )
            "#
    ]
    .assert_debug_eq(&recon::Store::insert(&store, &ReconItem::new_key(&id)).await);

    // second insert of same key reports it already existed
    expect![
        r#"
            Ok(
                false,
            )
            "#
    ]
    .assert_debug_eq(&recon::Store::insert(&store, &ReconItem::new_key(&id)).await);
}

test_with_dbs!(
    double_insert_with_value,
    double_insert_with_value,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);
async fn double_insert_with_value<S>(store: S)
where
    S: recon::Store<Key = EventId, Hash = Sha256a>,
{
    let id = random_event_id(Some(10), None);
    let (_, car) = build_car_file(2).await;

    let item = &ReconItem::new_with_value(&id, &car);

    // do take the first one
    expect![
        r#"
            Ok(
                true,
            )
            "#
    ]
    .assert_debug_eq(&recon::Store::insert(&store, &item).await);

    // the second insert of same key with value reports it already exists.
    // Do not override values
    expect![[r#"
            Ok(
                false,
            )
        "#]]
    .assert_debug_eq(&recon::Store::insert(&store, item).await);
}

test_with_dbs!(
    update_missing_value,
    update_missing_value,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);
async fn update_missing_value<S>(store: S)
where
    S: recon::Store<Key = EventId, Hash = Sha256a>,
{
    let id = random_event_id(Some(10), None);
    let (_, car) = build_car_file(2).await;

    let item_without_value = &ReconItem::new_key(&id);
    let item_with_value = &ReconItem::new_with_value(&id, &car);

    // do take the first one
    expect![
        r#"
            Ok(
                true,
            )
            "#
    ]
    .assert_debug_eq(&recon::Store::insert(&store, item_without_value).await);

    // accept the second insert of same key with the value
    expect![[r#"
            Ok(
                false,
            )
        "#]]
    .assert_debug_eq(&recon::Store::insert(&store, item_with_value).await);
}

test_with_dbs!(
    first_and_last,
    first_and_last,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
        "delete from ceramic_one_interest",
    ]
);
async fn first_and_last<S>(store: S)
where
    S: recon::Store<Key = EventId, Hash = Sha256a> + Send + Sync,
{
    recon::Store::insert(
        &store,
        &ReconItem::new_key(&random_event_id(
            Some(10),
            Some("baeabeie2bcird7765t7646jcoatd72tfn2tscdaap7g6kvvy7k43s34aau"),
        )),
    )
    .await
    .unwrap();
    recon::Store::insert(
        &store,
        &ReconItem::new_key(&random_event_id(
            Some(11),
            Some("baeabeianftvrst5bja422dod6uf42pmwkwix6rprguanwsxylfut56e3ue"),
        )),
    )
    .await
    .unwrap();

    // Only one key in range
    let ret = recon::Store::first_and_last(
        &store,
        &event_id_builder().with_event_height(9).build_fencepost(),
        &event_id_builder().with_event_height(11).build_fencepost(),
    )
    .await
    .unwrap();
    expect![[r#"
            Some(
                (
                    EventId {
                        bytes: "ce010502b51217a029eb540d4f16d8429ae87f86ead3ca3c0a010012209a089111fffeecffee792270263fea656ea7210c007fcde556b8fab9b96f8005",
                        network_id: Some(
                            2,
                        ),
                        separator: Some(
                            "b51217a029eb540d",
                        ),
                        controller: Some(
                            "4f16d8429ae87f86",
                        ),
                        stream_id: Some(
                            "ead3ca3c",
                        ),
                        event_height: Some(
                            10,
                        ),
                        cid: Some(
                            "baeabeie2bcird7765t7646jcoatd72tfn2tscdaap7g6kvvy7k43s34aau",
                        ),
                    },
                    EventId {
                        bytes: "ce010502b51217a029eb540d4f16d8429ae87f86ead3ca3c0a010012209a089111fffeecffee792270263fea656ea7210c007fcde556b8fab9b96f8005",
                        network_id: Some(
                            2,
                        ),
                        separator: Some(
                            "b51217a029eb540d",
                        ),
                        controller: Some(
                            "4f16d8429ae87f86",
                        ),
                        stream_id: Some(
                            "ead3ca3c",
                        ),
                        event_height: Some(
                            10,
                        ),
                        cid: Some(
                            "baeabeie2bcird7765t7646jcoatd72tfn2tscdaap7g6kvvy7k43s34aau",
                        ),
                    },
                ),
            )
        "#]]
        .assert_debug_eq(&ret);

    // No keys in range
    let ret = recon::Store::first_and_last(
        &store,
        &event_id_builder().with_event_height(12).build_fencepost(),
        &event_id_builder().with_max_event_height().build_fencepost(),
    )
    .await
    .unwrap();
    expect![[r#"
            None
        "#]]
    .assert_debug_eq(&ret);

    // Two keys in range
    let ret = recon::Store::first_and_last(&store, &random_event_id_min(), &random_event_id_max())
        .await
        .unwrap();
    expect![[r#"
            Some(
                (
                    EventId {
                        bytes: "ce010502b51217a029eb540d4f16d8429ae87f86ead3ca3c0a010012209a089111fffeecffee792270263fea656ea7210c007fcde556b8fab9b96f8005",
                        network_id: Some(
                            2,
                        ),
                        separator: Some(
                            "b51217a029eb540d",
                        ),
                        controller: Some(
                            "4f16d8429ae87f86",
                        ),
                        stream_id: Some(
                            "ead3ca3c",
                        ),
                        event_height: Some(
                            10,
                        ),
                        cid: Some(
                            "baeabeie2bcird7765t7646jcoatd72tfn2tscdaap7g6kvvy7k43s34aau",
                        ),
                    },
                    EventId {
                        bytes: "ce010502b51217a029eb540d4f16d8429ae87f86ead3ca3c0b010012200d2ceb194fa14839ad0dc3f50bcd3d9655917f45f13500db4af859693ef89ba1",
                        network_id: Some(
                            2,
                        ),
                        separator: Some(
                            "b51217a029eb540d",
                        ),
                        controller: Some(
                            "4f16d8429ae87f86",
                        ),
                        stream_id: Some(
                            "ead3ca3c",
                        ),
                        event_height: Some(
                            11,
                        ),
                        cid: Some(
                            "baeabeianftvrst5bja422dod6uf42pmwkwix6rprguanwsxylfut56e3ue",
                        ),
                    },
                ),
            )
        "#]]
        .assert_debug_eq(&ret);
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
    let key = random_event_id(None, None);
    let (_, store_value) = build_car_file(3).await;
    recon::Store::insert(
        &store,
        &ReconItem::new_with_value(&key, store_value.as_slice()),
    )
    .await
    .unwrap();
    let value = recon::Store::value_for_key(&store, &key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hex::encode(store_value), hex::encode(value));
}

test_with_dbs!(
    keys_with_missing_value,
    keys_with_missing_value,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);

async fn keys_with_missing_value<S>(store: S)
where
    S: recon::Store<Key = EventId, Hash = Sha256a>,
{
    let key = random_event_id(
        Some(4),
        Some("baeabeigc5edwvc47ul6belpxk3lgddipri5hw6f347s6ur4pdzwceprqbu"),
    );
    recon::Store::insert(&store, &ReconItem::new(&key, None))
        .await
        .unwrap();
    let missing_keys = recon::Store::keys_with_missing_values(
        &store,
        (EventId::min_value(), EventId::max_value()).into(),
    )
    .await
    .unwrap();
    expect![[r#"
            [
                EventId {
                    bytes: "ce010502b51217a029eb540d4f16d8429ae87f86ead3ca3c0401001220c2e9076a8b9fa2fc122df756d6618d0f8a3a7b78bbe7e5ea478f1e6c223e300d",
                    network_id: Some(
                        2,
                    ),
                    separator: Some(
                        "b51217a029eb540d",
                    ),
                    controller: Some(
                        "4f16d8429ae87f86",
                    ),
                    stream_id: Some(
                        "ead3ca3c",
                    ),
                    event_height: Some(
                        4,
                    ),
                    cid: Some(
                        "baeabeigc5edwvc47ul6belpxk3lgddipri5hw6f347s6ur4pdzwceprqbu",
                    ),
                },
            ]
        "#]]
        .assert_debug_eq(&missing_keys);

    let (_, value) = build_car_file(2).await;
    recon::Store::insert(&store, &ReconItem::new(&key, Some(&value)))
        .await
        .unwrap();
    let missing_keys = recon::Store::keys_with_missing_values(
        &store,
        (EventId::min_value(), EventId::max_value()).into(),
    )
    .await
    .unwrap();
    expect![[r#"
                []
            "#]]
    .assert_debug_eq(&missing_keys);
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
    let key = random_event_id(None, None);
    let (blocks, store_value) = build_car_file(3).await;
    recon::Store::insert(
        &store,
        &ReconItem::new_with_value(&key, store_value.as_slice()),
    )
    .await
    .unwrap();
    let value = recon::Store::value_for_key(&store, &key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hex::encode(store_value), hex::encode(value));

    // Read each block from the CAR
    for block in blocks {
        let value = iroh_bitswap::Store::get(&store, &block.cid).await.unwrap();
        assert_eq!(block, value);
    }
}

// stores 3 keys with 3,5,10 block long CAR files
// each one takes n+1 blocks as it needs to store the root and all blocks so we expect 3+5+10+3=21 blocks
// but we use a delivered integer per event, so we expect it to increment by 1 for each event
async fn prep_highwater_tests(store: &dyn AccessModelStore) -> (EventId, EventId, EventId) {
    let key_a = random_event_id(None, None);
    let key_b = random_event_id(None, None);
    let key_c = random_event_id(None, None);
    for (x, key) in [3, 5, 10].into_iter().zip([&key_a, &key_b, &key_c]) {
        let (_blocks, store_value) = build_car_file(x).await;
        assert_eq!(_blocks.len(), x);
        store
            .insert(key.to_owned(), Some(store_value))
            .await
            .unwrap();
    }

    (key_a, key_b, key_c)
}

test_with_dbs!(
    keys_since_highwater_mark_all_global_counter,
    keys_since_highwater_mark_all_global_counter,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);
async fn keys_since_highwater_mark_all_global_counter<S>(store: S)
where
    S: AccessModelStore,
{
    let (key_a, key_b, key_c) = prep_highwater_tests(&store).await;

    let (hw, res) = store.keys_since_highwater_mark(0, 10).await.unwrap();
    assert_eq!(3, res.len());
    assert!(hw >= 4); // THIS IS THE GLOBAL COUNTER. we have 3 rows in the db we have a counter of 4 or more
    let exp = [key_a.clone(), key_b.clone(), key_c.clone()];
    assert_eq!(exp, res.as_slice());
}

test_with_dbs!(
    keys_since_highwater_mark_limit_1,
    keys_since_highwater_mark_limit_1,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);
async fn keys_since_highwater_mark_limit_1<S>(store: S)
where
    S: AccessModelStore,
{
    let (key_a, _key_b, _key_c) = prep_highwater_tests(&store).await;
    let (hw_og, res) = store.keys_since_highwater_mark(0, 1).await.unwrap();
    assert_eq!(1, res.len());
    assert!(hw_og >= 2); // other tests might be incrementing the count. but we should have at least 2 and it shouldn't change between calls
    let (hw, res) = store.keys_since_highwater_mark(0, 1).await.unwrap();
    assert_eq!(hw_og, hw);
    assert_eq!([key_a], res.as_slice());
}

test_with_dbs!(
    keys_since_highwater_mark_middle_start,
    keys_since_highwater_mark_middle_start,
    [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ]
);
async fn keys_since_highwater_mark_middle_start<S>(store: S)
where
    S: AccessModelStore,
{
    let (key_a, key_b, key_c) = prep_highwater_tests(&store).await;

    // starting at rowid 1 which is in the middle of key A should still return key A
    let (hw, res) = store.keys_since_highwater_mark(1, 2).await.unwrap();
    assert_eq!(2, res.len());
    assert!(hw >= 3);
    assert_eq!([key_a, key_b], res.as_slice());

    let (hw, res) = store.keys_since_highwater_mark(hw, 1).await.unwrap();
    assert_eq!(1, res.len());
    assert!(hw >= 4);
    assert_eq!([key_c], res.as_slice());

    let (hw, res) = store.keys_since_highwater_mark(hw, 1).await.unwrap();
    assert_eq!(0, res.len());
    assert!(hw >= 4); // previously returned 0
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
        err.contains("no rows returned by a query that expected to return at least one row"),
        "{}",
        err
    );
}
