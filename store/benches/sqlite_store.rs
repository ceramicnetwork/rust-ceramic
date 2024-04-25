use ceramic_core::{EventId, Network};
use ceramic_store::{ModelStore, SqlitePool};
use cid::Cid;
use criterion2::{criterion_group, criterion_main, BatchSize, Criterion};
use multihash::{Code, MultihashDigest};
use rand::RngCore;
use recon::{ReconItem, Sha256a, Store};
use std::path::PathBuf;

struct ModelSetup {
    store: ModelStore<Sha256a>,
    events: Vec<(EventId, Vec<u8>)>,
}

enum ModelType {
    Small,
    Large,
}

fn generate_event_id(data: &[u8]) -> EventId {
    let cid = Cid::new_v1(
        0x55, //RAW
        Code::Sha2_256.digest(&data),
    );
    EventId::new(
        &Network::Mainnet,
        "model",
        "kh4q0ozorrgaq2mezktnrmdwleo1d",
        "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9",
        &cid,
        1,
        &cid,
    )
}

const INSERTION_COUNT: usize = 10_000;

async fn model_setup(dir: PathBuf, tpe: ModelType, cnt: usize) -> ModelSetup {
    let mut events = Vec::with_capacity(cnt);
    for _ in 0..cnt {
        let mut data = match tpe {
            ModelType::Small => {
                vec![0u8; 256]
            }
            ModelType::Large => {
                vec![0u8; 1024 * 4]
            }
        };
        rand::thread_rng().fill_bytes(&mut data);
        let event_id = generate_event_id(&data);
        let cid = event_id.cid().unwrap();
        let header = iroh_car::CarHeader::V1(iroh_car::CarHeaderV1::from(vec![cid.clone()]));
        let writer = tokio::io::BufWriter::new(Vec::with_capacity(1024 * 1024));
        let mut writer = iroh_car::CarWriter::new(header, writer);
        writer.write(cid, data.as_slice()).await.unwrap();
        let data = writer.finish().await.unwrap().into_inner();
        events.push((event_id, data));
    }

    let path = dir.join(format!("{}.db", uuid::Uuid::new_v4()));
    let pool = SqlitePool::connect(format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    let store = ModelStore::new(pool).await.unwrap();
    ModelSetup { store, events }
}

async fn model_routine(input: ModelSetup) {
    let futs = input.events.into_iter().map(|(event_id, data)| {
        let mut store = input.store.clone();
        async move {
            let event = ReconItem::new(&event_id, Some(data.as_slice()));
            store.insert(event).await
        }
    });
    futures::future::join_all(futs).await;
}

fn small_model_inserts(c: &mut Criterion) {
    let exec = tokio::runtime::Runtime::new().unwrap();
    let dir = exec.block_on(async move { tmpdir::TmpDir::new("ceramic_store").await.unwrap() });
    let mut group = c.benchmark_group("small model inserts");
    let path = dir.to_path_buf();
    group.bench_function("sqlite store", move |b| {
        b.to_async(&exec).iter_batched_async_setup(
            // setup
            || async { model_setup(path.clone(), ModelType::Small, INSERTION_COUNT).await },
            // routine
            |input| async { model_routine(input).await },
            // batch size
            BatchSize::PerIteration,
        )
    });
    group.finish();
    let exec = tokio::runtime::Runtime::new().unwrap();
    exec.block_on(async move {
        dir.close().await.unwrap();
        drop(dir);
    });
}

fn large_model_inserts(c: &mut Criterion) {
    let exec = tokio::runtime::Runtime::new().unwrap();
    let dir = exec.block_on(tmpdir::TmpDir::new("ceramic_store")).unwrap();
    let mut group = c.benchmark_group("large model inserts");
    group.bench_function("sqlite store", |b| {
        b.to_async(&exec).iter_batched_async_setup(
            // setup
            || async { model_setup(dir.to_path_buf(), ModelType::Large, INSERTION_COUNT).await },
            // routine
            |input| async { model_routine(input).await },
            // batch size
            BatchSize::PerIteration,
        )
    });
    group.finish();
    let exec = tokio::runtime::Runtime::new().unwrap();
    exec.block_on(async move {
        dir.close().await.unwrap();
        drop(dir);
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10).warm_up_time(std::time::Duration::from_secs(30)).measurement_time(std::time::Duration::from_secs(60));
    targets = small_model_inserts, large_model_inserts
}

criterion_main!(benches);