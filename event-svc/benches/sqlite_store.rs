use std::str::FromStr;

use ceramic_core::{Cid, DidDocument, EventId, Network, NodeId, StreamId};
use ceramic_event::unvalidated::{
    self,
    signed::{self, Signer},
    Builder,
};
use ceramic_event_svc::store::{EventAccess, EventInsertable};
use ceramic_sql::sqlite::SqlitePool;
use criterion2::{criterion_group, criterion_main, BatchSize, Criterion};
use ipld_core::ipld::Ipld;
use itertools::{self, Itertools};
use rand::RngCore;

const INSERT_BATCH_SIZE: usize = 10;

struct ModelSetup {
    pool: SqlitePool,
    events: Vec<EventInsertable>,
}

enum ModelType {
    Small,
    Large,
}

async fn generate_init_event(
    model: &StreamId,
    data: &[u8],
    signer: impl Signer,
) -> (EventId, Cid, unvalidated::Event<Ipld>) {
    let data = ipld_core::ipld!({
        "raw": data,
    });
    let init = Builder::init()
        .with_controller("controller".to_string())
        .with_sep("sep".to_string(), model.to_vec())
        .with_data(data)
        .build();
    let signed = signed::Event::from_payload(unvalidated::Payload::Init(init), signer).unwrap();
    let cid = signed.envelope_cid();
    let id = EventId::new(
        &Network::DevUnstable,
        "model",
        &model.to_vec(),
        "did:key:z6MkgSV3tAuw7gUWqKCUY7ae6uWNxqYgdwPhUJbJhF9EFXm9",
        cid,
        cid,
    );

    (id, *cid, unvalidated::Event::from(signed))
}

const INSERTION_COUNT: usize = 10_000;

async fn model_setup(tpe: ModelType, cnt: usize) -> ModelSetup {
    let informant = NodeId::random().0;
    let mut events = Vec::with_capacity(cnt);
    let signer = signed::JwkSigner::new(
        DidDocument::new("did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw"),
        "810d51e02cb63066b7d2d2ec67e05e18c29b938412050bdd3c04d878d8001f3c",
    )
    .await
    .unwrap();
    let model =
        StreamId::from_str("k2t6wz4ylx0qr6v7dvbczbxqy7pqjb0879qx930c1e27gacg3r8sllonqt4xx9")
            .unwrap();
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

        let (order_key, cid, event) = generate_init_event(&model, &data, signer.clone()).await;
        events
            .push(EventInsertable::try_new(order_key, cid, true, event, Some(informant)).unwrap());
    }

    let pool = SqlitePool::connect_in_memory().await.unwrap();
    ModelSetup { pool, events }
}

async fn model_routine(input: ModelSetup) {
    let futs = input.events.into_iter().chunks(INSERT_BATCH_SIZE);
    let futs = futs.into_iter().map(|batch| {
        let store = input.pool.clone();
        let set = batch.into_iter().collect::<Vec<_>>();
        async move { EventAccess::insert_many(&store, set.iter()).await }
    });
    futures::future::join_all(futs).await;
}

fn small_model_inserts(c: &mut Criterion) {
    let exec = tokio::runtime::Runtime::new().unwrap();
    let dir =
        exec.block_on(async move { tmpdir::TmpDir::new("event_service_store").await.unwrap() });
    let mut group = c.benchmark_group("small model inserts");
    group.bench_function("sqlite store", move |b| {
        b.to_async(&exec).iter_batched_async_setup(
            // setup
            || async { model_setup(ModelType::Small, INSERTION_COUNT).await },
            // routine
            |input| async { model_routine(input).await },
            // batch size
            BatchSize::SmallInput,
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
    let dir = exec
        .block_on(tmpdir::TmpDir::new("event_service_store"))
        .unwrap();
    let mut group = c.benchmark_group("large model inserts");
    group.bench_function("sqlite store", |b| {
        b.to_async(&exec).iter_batched_async_setup(
            // setup
            || async { model_setup(ModelType::Large, INSERTION_COUNT).await },
            // routine
            |input| async { model_routine(input).await },
            // batch size
            BatchSize::SmallInput,
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
