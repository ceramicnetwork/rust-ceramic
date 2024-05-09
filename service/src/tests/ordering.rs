use ceramic_core::{DagCborEncoded, EventId};
use ceramic_event::{
    unvalidated::{Additional, Builder, Controllers, Payload, Sep},
    EventBytes,
};
use cid::Cid;
use ipld_core::{ipld, ipld::Ipld};
use iroh_car::{CarHeader, CarWriter};
use multihash_codetable::{Code, MultihashDigest};
use recon::ReconItem;

use crate::{
    tests::{assert_deliverable, gen_rand_bytes},
    CeramicEventService,
};

use super::{random_cid, random_event_id};

async fn setup_service() -> CeramicEventService {
    let _ = ceramic_metrics::init_local_tracing();
    let conn = ceramic_store::SqlitePool::connect_in_memory()
        .await
        .unwrap();
    CeramicEventService::new(conn).await.unwrap()
}

async fn build_event<T>(event: Payload<T>) -> (EventId, Vec<u8>)
where
    T: serde::Serialize,
{
    let commit: DagCborEncoded = DagCborEncoded::new(&event).unwrap();
    let root_cid = Cid::new_v1(0x71, Code::Sha2_256.digest(commit.as_ref()));
    let mut car = Vec::new();
    let roots: Vec<Cid> = vec![root_cid];
    let mut writer = CarWriter::new(CarHeader::V1(roots.into()), &mut car);
    writer.write(root_cid, commit).await.unwrap();

    writer.finish().await.unwrap();
    let event_id = random_event_id(Some(&root_cid.to_string()));
    (event_id, car)
}

async fn init_event(model: Cid) -> Payload<()> {
    let init = Builder::default()
        .with_sep("sep".to_string())
        .init()
        .with_controller("controller".to_string())
        .with_additional(
            "model".to_string(),
            ceramic_event::unvalidated::Value::from(EventBytes::from(model.to_bytes())),
        )
        .build()
        .await
        .unwrap();
    Payload::Init(init)
}

async fn data_event(model: Cid, init_id: Cid, prev: Cid, data: Ipld) -> Payload<Ipld> {
    let data = Builder::default()
        .data(init_id, prev, data)
        .with_sep("sep".to_string())
        .with_additional(
            "model".to_string(),
            ceramic_event::unvalidated::Value::from(EventBytes::from(model.to_bytes())),
        )
        .build()
        .await
        .expect("failed to build event");
    Payload::Data(data)
}

// builds init -> data -> data that are a stream (will be a different stream each call)
async fn get_events() -> [(EventId, Vec<u8>); 3] {
    let model = random_cid();

    let data = gen_rand_bytes::<50>();
    let data2 = gen_rand_bytes::<50>();

    let data = ipld!({
        "radius": 1,
        "red": 2,
        "green": 3,
        "blue": 4,
        "raw": data.as_slice(),
    });

    let data2 = ipld!({
        "radius": 1,
        "red": 2,
        "green": 3,
        "blue": 4,
        "raw": data2.as_slice(),
    });

    let (event_id, car) = build_event(init_event(model).await).await;
    let init_cid = event_id.cid().unwrap();
    let (data_id, data_car) = build_event(data_event(model, init_cid, init_cid, data).await).await;
    let (data_id_2, data_car_2) =
        build_event(data_event(model, init_cid, data_id.cid().unwrap(), data2).await).await;

    [
        (event_id, car),
        (data_id, data_car),
        (data_id_2, data_car_2),
    ]
}

#[tokio::test]
async fn test_init_event_delivered() {
    let store = setup_service().await;
    let events = get_events().await;
    let init = &events[0];
    let new = store
        .insert_events_from_carfiles(&[ReconItem::new(&init.0, &init.1)], true)
        .await
        .unwrap();
    let new = new.keys.into_iter().filter(|k| *k).count();
    assert_eq!(new, 1);
    assert_deliverable(&store.pool, &init.0.cid().unwrap()).await;
}

#[tokio::test]
async fn test_missing_prev_error_history_required() {
    let store = setup_service().await;
    let events = get_events().await;
    let data = &events[1];
    let new = store
        .insert_events_from_carfiles(&[ReconItem::new(&data.0, &data.1)], true)
        .await;
    match new {
        Ok(_) => panic!("should have errored"),
        Err(e) => {
            match e {
                crate::Error::InvalidArgument { .. } => {}
                e => {
                    panic!("unexpected error: {:?}", e);
                }
            };
        }
    };
}
