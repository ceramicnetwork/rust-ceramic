use self::SqliteEventStore;

use super::*;

use std::{str::FromStr, time::Instant};

use ceramic_core::{
    event_id::{Builder, WithInit},
    EventId, Network,
};

use ceramic_metrics::init_local_tracing;
use cid::Cid;
use iroh_bitswap::Block;
use iroh_car::{CarHeader, CarWriter};
use libipld::{ipld, prelude::Encode, Ipld};
use libipld_cbor::DagCborCodec;
use multihash::{Code, MultihashDigest};
use rand::Rng;
use sqlx::Acquire;
use tracing::info;

pub(crate) async fn new_store() -> SqliteEventStore {
    let conn = SqlitePool::connect_in_memory().await.unwrap();
    SqliteEventStore::new(conn).await.unwrap()
}

// for the highwater tests that care about event ordering
pub(crate) async fn new_local_store() -> SqliteEventStore {
    let conn = SqlitePool::connect_in_memory().await.unwrap();
    SqliteEventStore::new_local(conn).await.unwrap()
}

#[tokio::test]
async fn get_nonexistent_block() {
    let store = new_store().await;

    let cid = Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line

    let exists = iroh_bitswap::Store::has(&store, &cid).await.unwrap();
    assert!(!exists);
}

const MODEL_ID: &str = "k2t6wz4yhfp1r5pwi52gw89nzjbu53qk7m32o5iguw42c6knsaj0feuf927agb";
const CONTROLLER: &str = "did:key:z6Mkqtw7Pj5Lv9xc4PgUYAnwfaVoMC6FRneGWVr5ekTEfKVL";
const INIT_ID: &str = "baeabeiajn5ypv2gllvkk4muvzujvcnoen2orknxix7qtil2daqn6vu6khq";
const SORT_KEY: &str = "model";

// Return an builder for an event with the same network,model,controller,stream.
pub(crate) fn event_id_builder() -> Builder<WithInit> {
    EventId::builder()
        .with_network(&Network::DevUnstable)
        .with_sort_value(SORT_KEY, MODEL_ID)
        .with_controller(CONTROLLER)
        .with_init(&Cid::from_str(INIT_ID).unwrap())
}

// Generate an event for the same network,model,controller,stream
// The event and height are random when when its None.
pub(crate) fn random_event_id(height: Option<u64>, event: Option<&str>) -> EventId {
    event_id_builder()
        .with_event_height(height.unwrap_or_else(|| rand::thread_rng().gen()))
        .with_event(
            &event
                .map(|cid| Cid::from_str(cid).unwrap())
                .unwrap_or_else(random_cid),
        )
        .build()
}
// The EventId that is the minumum of all possible random event ids
pub(crate) fn random_event_id_min() -> EventId {
    event_id_builder().with_min_event_height().build_fencepost()
}
// The EventId that is the maximum of all possible random event ids
pub(crate) fn random_event_id_max() -> EventId {
    event_id_builder().with_max_event_height().build_fencepost()
}

pub(crate) fn random_cid() -> Cid {
    let mut data = [0u8; 8];
    rand::Rng::fill(&mut ::rand::thread_rng(), &mut data);
    let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
    Cid::new_v1(0x00, hash)
}

pub(crate) async fn build_car_file(count: usize) -> (Vec<Block>, Vec<u8>) {
    let blocks: Vec<Block> = (0..count).map(|_| random_block()).collect();
    let root = ipld!( {
        "links": blocks.iter().map(|block| Ipld::Link(block.cid)).collect::<Vec<Ipld>>(),
    });
    let mut root_bytes = Vec::new();
    root.encode(DagCborCodec, &mut root_bytes).unwrap();
    let root_cid = Cid::new_v1(
        DagCborCodec.into(),
        MultihashDigest::digest(&Code::Sha2_256, &root_bytes),
    );
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

pub(crate) fn random_block() -> Block {
    let mut data = [0u8; 1024];
    rand::Rng::fill(&mut ::rand::thread_rng(), &mut data);
    let hash = ::multihash::MultihashDigest::digest(&::multihash::Code::Sha2_256, &data);
    Block {
        cid: Cid::new_v1(0x00, hash),
        data: data.to_vec().into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn channel_test() {
    let _ = init_local_tracing();
    let sql = SqlitePool::connect_in_memory().await.unwrap();

    let store = SqliteEventStore::new(sql).await.unwrap();

    {
        let mut conn = store.store.pool.writer().acquire().await.unwrap();
        let mut tx = conn.begin().await.unwrap();
        sqlx::query("CREATE TABLE test (id integer primary key, value INTEGER)")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let (data_tx, mut data_rx) = tokio::sync::mpsc::channel(1024);
    let mut futures = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let tx = data_tx.clone();
        futures.spawn(async move {
            for i in 0..100_000 {
                tx.send(i).await.unwrap();
            }
        });
    }

    drop(data_tx);
    let (res_tx, res_rx) = tokio::sync::oneshot::channel();
    futures.spawn(async move {
        let mut conn = store.pool.writer().acquire().await.unwrap();
        let start = Instant::now();
        while let Some(val) = data_rx.recv().await {
            let mut tx = conn.begin().await.unwrap();
            sqlx::query("INSERT INTO test (value) VALUES (?)")
                .bind(val)
                .execute(&mut *tx)
                .await
                .unwrap();
            let _ = tx.commit().await;
        }
        let elapsed = start.elapsed();
        res_tx.send(elapsed).unwrap();
    });

    let start = Instant::now();
    while let Some(_f) = futures.join_next().await {
        // can we wait all?
    }
    let elapsed2 = start.elapsed();
    let elapsed = res_rx.await.unwrap();
    info!("Elapsed: {:?}", elapsed);
    info!("Elapsed: {:?}", elapsed2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn locker_test() {
    let _ = init_local_tracing();
    let sql = SqlitePool::connect_in_memory().await.unwrap();
    let store = SqliteEventStore::new(sql).await.unwrap();

    {
        let mut conn = store.pool.writer().acquire().await.unwrap();
        let mut tx = conn.begin().await.unwrap();
        sqlx::query("CREATE TABLE test (id integer primary key, value INTEGER)")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    let mut futures = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let writer = store.pool.writer().to_owned();
        futures.spawn(async move {
            for i in 0..100_000 {
                let mut tx = writer.begin().await.unwrap();
                sqlx::query("INSERT INTO test (value) VALUES (?)")
                    .bind(i)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                let _ = tx.commit().await;
            }
        });
    }
    let start = Instant::now();
    while let Some(_f) = futures.join_next().await {
        // can we wait all?
    }
    let elapsed = start.elapsed();

    info!("Elapsed: {:?}", elapsed);
}
