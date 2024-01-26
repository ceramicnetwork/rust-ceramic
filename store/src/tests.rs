use super::*;

use std::str::FromStr;

use ceramic_core::{
    event_id::{Builder, WithInit},
    Network, SqlitePool,
};
use cid::Cid;
use expect_test::expect;
use iroh_car::{CarHeader, CarWriter};
use libipld::{ipld, prelude::Encode, Ipld};
use libipld_cbor::DagCborCodec;
use rand::Rng;
use test_log::test;

async fn new_store() -> Store {
    let conn = SqlitePool::connect("sqlite::memory:").await.unwrap();
    Store::new(conn).await.unwrap()
}

#[tokio::test]
async fn get_nonexistent_block() {
    let store = new_store().await;

    let cid = Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line

    let exists = iroh_bitswap::Store::has(&store, &cid).await.unwrap();
    assert_eq!(false, exists);
}

const MODEL_ID: &str = "k2t6wz4yhfp1r5pwi52gw89nzjbu53qk7m32o5iguw42c6knsaj0feuf927agb";
const CONTROLLER: &str = "did:key:z6Mkqtw7Pj5Lv9xc4PgUYAnwfaVoMC6FRneGWVr5ekTEfKVL";
const INIT_ID: &str = "baeabeiajn5ypv2gllvkk4muvzujvcnoen2orknxix7qtil2daqn6vu6khq";

// Return an builder for an event with the same network,model,controller,stream.
fn event_id_builder() -> Builder<WithInit> {
    EventId::builder()
        .with_network(&Network::DevUnstable)
        .with_sort_value(SORT_KEY, MODEL_ID)
        .with_controller(CONTROLLER)
        .with_init(&Cid::from_str(INIT_ID).unwrap())
}

// Generate an event for the same network,model,controller,stream
// The event and height are random when when its None.
fn random_event_id(height: Option<u64>, event: Option<&str>) -> EventId {
    event_id_builder()
        .with_event_height(height.unwrap_or_else(|| rand::thread_rng().gen()))
        .with_event(
            &event
                .map(|cid| Cid::from_str(cid).unwrap())
                .unwrap_or_else(|| random_cid()),
        )
        .build()
}
// The EventId that is the minumum of all possible random event ids
fn random_event_id_min() -> EventId {
    event_id_builder().with_min_event_height().build_fencepost()
}
// The EventId that is the maximum of all possible random event ids
fn random_event_id_max() -> EventId {
    event_id_builder().with_max_event_height().build_fencepost()
}

fn random_cid() -> Cid {
    let mut data = [0u8; 8];
    rand::Rng::fill(&mut ::rand::thread_rng(), &mut data);
    let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
    Cid::new_v1(0x00, hash)
}

async fn build_car_file(count: usize) -> (Vec<Block>, Vec<u8>) {
    let blocks: Vec<Block> = (0..count).map(|_| random_block()).collect();
    let root = ipld!( {
        "links": blocks.iter().map(|block| Ipld::Link(block.cid)).collect::<Vec<Ipld>>(),
    });
    let mut root_bytes = Vec::new();
    root.encode(DagCborCodec, &mut root_bytes).unwrap();
    let root_cid = Cid::new_v1(0x71, MultihashDigest::digest(&Code::Sha2_256, &root_bytes));
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

fn random_block() -> Block {
    let mut data = [0u8; 1024];
    rand::Rng::fill(&mut ::rand::thread_rng(), &mut data);
    let hash = ::multihash::MultihashDigest::digest(&::multihash::Code::Sha2_256, &data);
    Block {
        cid: Cid::new_v1(0x00, hash),
        data: data.to_vec().into(),
    }
}

#[test(tokio::test)]
async fn hash_range_query() {
    let mut store = new_store().await;
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
    recon::Store::insert(
        &mut store,
        ReconItem::new(
            &random_event_id(
                Some(2),
                Some("baeabeibmek7v4ljsu575ohgjhovdxhcw6p6oivgb55hzkeap5po7ghzqty"),
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
    let id = random_event_id(Some(10), None);

    // do take the first one
    expect![
        r#"
        Ok(
            true,
        )
        "#
    ]
    .assert_debug_eq(&recon::Store::insert(&mut store, ReconItem::new_key(&id)).await);

    // reject the second insert of same key
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
async fn first_and_last() {
    let mut store = new_store().await;
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
