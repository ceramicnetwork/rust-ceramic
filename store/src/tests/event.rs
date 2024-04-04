use std::{str::FromStr, sync::OnceLock};

use anyhow::Error;
use bytes::Bytes;
use cid::{Cid, CidGeneric};
use expect_test::expect;
use iroh_bitswap::Store;
use recon::{Key, ReconItem, Sha256a};
use test_log::test;
use tokio::sync::{Mutex, MutexGuard};

use super::*;

static LOCK: OnceLock<Mutex<()>> = OnceLock::new();

async fn get_lock() -> MutexGuard<'static, ()> {
    LOCK.get_or_init(|| Mutex::new(())).lock().await
}

async fn prep_test(store: &EventStoreSqlite<Sha256a>) -> MutexGuard<'static, ()> {
    let lock = get_lock().await;
    for stmt in [
        "delete from ceramic_one_event_block",
        "delete from ceramic_one_event",
        "delete from ceramic_one_block",
    ] {
        store.pool.run_statement(stmt).await.unwrap();
    }
    lock
}

#[test(tokio::test)]
async fn hash_range_query() {
    let mut store = new_store().await;
    let _l = prep_test(&store).await;
    recon::Store::insert(
        &mut store,
        ReconItem::new_key(&random_event_id(
            Some(1),
            Some("baeabeiazgwnti363jifhxaeaegbluw4ogcd2t5hsjaglo46wuwcgajqa5u"),
        )),
    )
    .await
    .unwrap();
    recon::Store::insert(
        &mut store,
        ReconItem::new_key(&random_event_id(
            Some(2),
            Some("baeabeihyl35xdlfju3zrkvy2exmnl6wics3rc5ppz7hwg7l7g4brbtnpny"),
        )),
    )
    .await
    .unwrap();
    let hash = recon::Store::hash_range(&mut store, &random_event_id_min(), &random_event_id_max())
        .await
        .unwrap();
    expect!["65C7A25327CC05C19AB5812103EEB8D1156595832B453C7BAC6A186F4811FA0A#2"]
        .assert_eq(&format!("{hash}"));
}

#[test(tokio::test)]
async fn range_query() {
    let mut store = new_store().await;
    let _l = prep_test(&store).await;
    recon::Store::insert(
        &mut store,
        ReconItem::new_key(&random_event_id(
            Some(1),
            Some("baeabeichhhmbhsic4maraneqf5gkhekgzcawhtpj3fh6opjtglznapz524"),
        )),
    )
    .await
    .unwrap();
    recon::Store::insert(
        &mut store,
        ReconItem::new_key(&random_event_id(
            Some(2),
            Some("baeabeibmek7v4ljsu575ohgjhovdxhcw6p6oivgb55hzkeap5po7ghzqty"),
        )),
    )
    .await
    .unwrap();
    let ids = recon::Store::range(
        &mut store,
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

#[test(tokio::test)]
async fn range_query_with_values() {
    let mut store = new_store().await;
    let _l = prep_test(&store).await;
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
    recon::Store::insert(&mut store, ReconItem::new(&one_id, Some(&one_car)))
        .await
        .unwrap();
    recon::Store::insert(&mut store, ReconItem::new(&two_id, Some(&two_car)))
        .await
        .unwrap();
    // Insert new event without a value to ensure we skip it in the query
    recon::Store::insert(
        &mut store,
        ReconItem::new(
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
        &mut store,
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

#[test(tokio::test)]
async fn double_insert() {
    let mut store = new_store().await;
    let _l = prep_test(&store).await;
    let id = random_event_id(Some(10), None);

    // first insert reports its a new key
    expect![
        r#"
            Ok(
                true,
            )
            "#
    ]
    .assert_debug_eq(&recon::Store::insert(&mut store, ReconItem::new_key(&id)).await);

    // second insert of same key reports it already existed
    expect![
        r#"
            Ok(
                false,
            )
            "#
    ]
    .assert_debug_eq(&recon::Store::insert(&mut store, ReconItem::new_key(&id)).await);
}

#[test(tokio::test)]
async fn double_insert_with_value() {
    let mut store = new_store().await;
    let _l = prep_test(&store).await;
    let id = random_event_id(Some(10), None);
    let (_, car) = build_car_file(2).await;

    let item = ReconItem::new_with_value(&id, &car);

    // do take the first one
    expect![
        r#"
            Ok(
                true,
            )
            "#
    ]
    .assert_debug_eq(&recon::Store::insert(&mut store, item.clone()).await);

    // the second insert of same key with value reports it already exists.
    // Do not override values
    expect![[r#"
            Ok(
                false,
            )
        "#]]
    .assert_debug_eq(&recon::Store::insert(&mut store, item).await);
}

#[test(tokio::test)]
async fn update_missing_value() {
    let mut store = new_store().await;
    let _l = prep_test(&store).await;
    let id = random_event_id(Some(10), None);
    let (_, car) = build_car_file(2).await;

    let item_without_value = ReconItem::new_key(&id);
    let item_with_value = ReconItem::new_with_value(&id, &car);

    // do take the first one
    expect![
        r#"
            Ok(
                true,
            )
            "#
    ]
    .assert_debug_eq(&recon::Store::insert(&mut store, item_without_value).await);

    // accept the second insert of same key with the value
    expect![[r#"
            Ok(
                false,
            )
        "#]]
    .assert_debug_eq(&recon::Store::insert(&mut store, item_with_value).await);
}

#[test(tokio::test)]
async fn first_and_last() {
    let mut store = new_store().await;
    let _l = prep_test(&store).await;
    store
        .pool
        .run_statement("delete from ceramic_one_event;")
        .await
        .unwrap();
    store
        .pool
        .run_statement("delete from ceramic_one_interest;")
        .await
        .unwrap();
    recon::Store::insert(
        &mut store,
        ReconItem::new_key(&random_event_id(
            Some(10),
            Some("baeabeie2bcird7765t7646jcoatd72tfn2tscdaap7g6kvvy7k43s34aau"),
        )),
    )
    .await
    .unwrap();
    recon::Store::insert(
        &mut store,
        ReconItem::new_key(&random_event_id(
            Some(11),
            Some("baeabeianftvrst5bja422dod6uf42pmwkwix6rprguanwsxylfut56e3ue"),
        )),
    )
    .await
    .unwrap();

    // Only one key in range
    let ret = recon::Store::first_and_last(
        &mut store,
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
        &mut store,
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
    let ret =
        recon::Store::first_and_last(&mut store, &random_event_id_min(), &random_event_id_max())
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

#[test(tokio::test)]
async fn store_value_for_key() {
    let mut store = new_store().await;
    let _l = prep_test(&store).await;
    let key = random_event_id(None, None);
    let (_, store_value) = build_car_file(3).await;
    recon::Store::insert(
        &mut store,
        ReconItem::new_with_value(&key, store_value.as_slice()),
    )
    .await
    .unwrap();
    let value = recon::Store::value_for_key(&mut store, &key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(hex::encode(store_value), hex::encode(value));
}
#[test(tokio::test)]
async fn keys_with_missing_value() {
    let mut store = new_store().await;
    let _l = prep_test(&store).await;
    let key = random_event_id(
        Some(4),
        Some("baeabeigc5edwvc47ul6belpxk3lgddipri5hw6f347s6ur4pdzwceprqbu"),
    );
    recon::Store::insert(&mut store, ReconItem::new(&key, None))
        .await
        .unwrap();
    let missing_keys = recon::Store::keys_with_missing_values(
        &mut store,
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
    recon::Store::insert(&mut store, ReconItem::new(&key, Some(&value)))
        .await
        .unwrap();
    let missing_keys = recon::Store::keys_with_missing_values(
        &mut store,
        (EventId::min_value(), EventId::max_value()).into(),
    )
    .await
    .unwrap();
    expect![[r#"
                []
            "#]]
    .assert_debug_eq(&missing_keys);
}

#[test(tokio::test)]
async fn read_value_as_block() {
    let mut store = new_store().await;
    let _l = prep_test(&store).await;
    let key = random_event_id(None, None);
    let (blocks, store_value) = build_car_file(3).await;
    recon::Store::insert(
        &mut store,
        ReconItem::new_with_value(&key, store_value.as_slice()),
    )
    .await
    .unwrap();
    let value = recon::Store::value_for_key(&mut store, &key)
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
async fn prep_highwater_tests(
    store: &mut EventStoreSqlite<Sha256a>,
) -> (EventId, EventId, EventId) {
    let key_a = random_event_id(None, None);
    let key_b = random_event_id(None, None);
    let key_c = random_event_id(None, None);
    for (x, key) in [3, 5, 10].into_iter().zip([&key_a, &key_b, &key_c]) {
        let (_blocks, store_value) = build_car_file(x).await;
        assert_eq!(_blocks.len(), x);
        recon::Store::insert(
            store,
            ReconItem::new_with_value(key, store_value.as_slice()),
        )
        .await
        .unwrap();
    }

    (key_a, key_b, key_c)
}

#[test(tokio::test)]
async fn keys_since_highwater_mark_all_global_counter() {
    let mut store1 = new_store().await;
    let _l = prep_test(&store1).await;
    let (key_a, key_b, key_c) = prep_highwater_tests(&mut store1).await;

    let (hw, res) = store1.new_keys_since_value(0, 10).await.unwrap();
    assert_eq!(3, res.len());
    assert!(hw >= 4); // THIS IS THE GLOBAL COUNTER. we have 3 rows in the db we have a counter of 4 or more
    let exp = [key_a.clone(), key_b.clone(), key_c.clone()];
    assert_eq!(exp, res.as_slice());

    // TODO: handle memory vs pg

    // drop(store1);
    // let mut store2 = new_store().await;

    // let (key1_a, key1_b, key1_c) = prep_highwater_tests(&mut store2).await;
    // let (hw, res) = store2.new_keys_since_value(0, 10).await.unwrap();
    // assert_eq!(3, res.len());
    // assert!(hw > 6); // THIS IS GLOBAL COUNTER. 3 rows in db, counter 7 or more depending on how many other tests are running

    // assert_eq!([key1_a, key1_b, key1_c], res.as_slice());
}

#[test(tokio::test)]
async fn keys_since_highwater_mark_limit_1() {
    let mut store: EventStoreSqlite<Sha256a> = new_local_store().await;
    let (key_a, _key_b, _key_c) = prep_highwater_tests(&mut store).await;

    let (hw, res) = store.new_keys_since_value(0, 1).await.unwrap();
    assert_eq!(1, res.len());
    assert_eq!(2, hw);
    assert_eq!([key_a], res.as_slice());
}

#[test(tokio::test)]
async fn keys_since_highwater_mark_middle_start() {
    let mut store: EventStoreSqlite<Sha256a> = new_local_store().await;
    let (key_a, key_b, key_c) = prep_highwater_tests(&mut store).await;

    // starting at rowid 1 which is in the middle of key A should still return key A
    let (hw, res) = store.new_keys_since_value(1, 2).await.unwrap();
    assert_eq!(2, res.len());
    assert_eq!(3, hw);
    assert_eq!([key_a, key_b], res.as_slice());

    let (hw, res) = store.new_keys_since_value(hw, 1).await.unwrap();
    assert_eq!(1, res.len());
    assert_eq!(4, hw);
    assert_eq!([key_c], res.as_slice());

    let (hw, res) = store.new_keys_since_value(hw, 1).await.unwrap();
    assert_eq!(0, res.len());
    assert_eq!(4, hw); // previously returned 0
}

#[tokio::test]
async fn test_store_block() {
    let blob: Bytes = hex::decode("0a050001020304").unwrap().into();
    let cid: CidGeneric<64> =
        Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line

    let store = new_store().await;
    let _l = prep_test(&store).await;

    let result = store.put_block(cid.hash(), &blob).await.unwrap();
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

#[tokio::test]
async fn test_double_store_block() {
    let blob: Bytes = hex::decode("0a050001020304").unwrap().into();
    let cid: CidGeneric<64> =
        Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line

    let store = new_store().await;
    let _l = prep_test(&store).await;

    let result = store.put_block(cid.hash(), &blob).await;
    // Assert that the block is new
    assert!(result.unwrap());

    // Try to put the block again
    let result = store.put_block(cid.hash(), &blob).await;
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

#[tokio::test]
async fn test_get_nonexistent_block() {
    let store = new_store().await;
    let _l = prep_test(&store).await;

    let cid = Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line

    let err = store.get(&cid).await.unwrap_err().to_string();
    assert!(
        err.contains("no rows returned by a query that expected to return at least one row"),
        "{}",
        err
    );
}
