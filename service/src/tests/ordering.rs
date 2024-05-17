use ceramic_api::AccessModelStore;
use ceramic_core::EventId;
use ceramic_event::unvalidated::{
    self,
    signed::{self},
    Builder,
};
use cid::Cid;
use ipld_core::{ipld, ipld::Ipld};
use recon::ReconItem;

use crate::{
    tests::{check_deliverable, gen_rand_bytes, signer},
    CeramicEventService,
};

use super::{random_cid, random_event_id};

async fn setup_service() -> CeramicEventService {
    let _ = ceramic_metrics::init_local_tracing();
    let conn = ceramic_store::SqlitePool::connect_in_memory()
        .await
        .unwrap();
    CeramicEventService::new_without_undelivered(conn)
        .await
        .unwrap()
}

async fn init_event(model: Cid, signer: &signed::JwkSigner) -> signed::Event<Ipld> {
    let init = Builder::init()
        .with_controller("controller".to_string())
        .with_sep("sep".to_string(), model.to_bytes())
        .build();
    signed::Event::from_payload(unvalidated::Payload::Init(init), signer.to_owned()).unwrap()
}

async fn data_event(
    _model: Cid,
    init_id: Cid,
    prev: Cid,
    data: Ipld,
    signer: &signed::JwkSigner,
) -> signed::Event<Ipld> {
    let commit = Builder::data()
        .with_id(init_id)
        .with_prev(prev)
        .with_data(data)
        .build();

    signed::Event::from_payload(unvalidated::Payload::Data(commit), signer.to_owned()).unwrap()
}

// builds init -> data -> data that are a stream (will be a different stream each call)
async fn get_events() -> [(EventId, Vec<u8>); 3] {
    let model = random_cid();
    let signer = Box::new(signer().await);

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

    let init = init_event(model, &signer).await;
    let (event_id, car) = (
        random_event_id(Some(&init.envelope_cid().to_string())),
        init.encode_car().await.unwrap(),
    );

    let init_cid = event_id.cid().unwrap();
    let data = data_event(model, init_cid, init_cid, data, &signer).await;
    let cid = data.envelope_cid();
    let (data_id, data_car) = (
        random_event_id(Some(&data.envelope_cid().to_string())),
        data.encode_car().await.unwrap(),
    );
    let data2 = data_event(model, init_cid, cid, data2, &signer).await;
    let (data_id_2, data_car_2) = (
        random_event_id(Some(&data2.envelope_cid().to_string())),
        data2.encode_car().await.unwrap(),
    );

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
    check_deliverable(&store.pool, &init.0.cid().unwrap(), true).await;
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
        Ok(v) => panic!("should have errored: {:?}", v),
        Err(e) => {
            match e {
                crate::Error::InvalidArgument { error } => {
                    // yes fragile, but we want to make sure it's not a parsing error or something unexpected
                    assert!(error
                        .to_string()
                        .contains("Missing required `prev` event CIDs"));
                }
                e => {
                    panic!("unexpected error: {:?}", e);
                }
            };
        }
    };
}

#[tokio::test]
async fn test_prev_exists_history_required() {
    let store = setup_service().await;
    let events = get_events().await;
    let init: &(EventId, Vec<u8>) = &events[0];
    let data = &events[1];
    let new = store
        .insert_events_from_carfiles(&[ReconItem::new(&init.0, &init.1)], true)
        .await
        .unwrap();
    let new = new.keys.into_iter().filter(|k| *k).count();
    assert_eq!(1, new);
    check_deliverable(&store.pool, &init.0.cid().unwrap(), true).await;
    let new = store
        .insert_events_from_carfiles(&[ReconItem::new(&data.0, &data.1)], true)
        .await
        .unwrap();
    let new = new.keys.into_iter().filter(|k| *k).count();
    assert_eq!(1, new);
    check_deliverable(&store.pool, &data.0.cid().unwrap(), true).await;
}

#[tokio::test]
async fn test_prev_in_same_write_history_required() {
    let store = setup_service().await;
    let events = get_events().await;
    let init: &(EventId, Vec<u8>) = &events[0];
    let data = &events[1];
    let new = store
        .insert_events_from_carfiles(
            &[
                ReconItem::new(&data.0, &data.1),
                ReconItem::new(&init.0, &init.1),
            ],
            true,
        )
        .await
        .unwrap();
    let new = new.keys.into_iter().filter(|k| *k).count();
    assert_eq!(2, new);
    check_deliverable(&store.pool, &init.0.cid().unwrap(), true).await;
    check_deliverable(&store.pool, &data.0.cid().unwrap(), true).await;
}

#[tokio::test]
async fn test_missing_prev_pending_recon() {
    let store = setup_service().await;
    let events = get_events().await;
    let data = &events[1];
    let new = store
        .insert_events_from_carfiles(&[ReconItem::new(&data.0, &data.1)], false)
        .await
        .unwrap();
    let new = new.keys.into_iter().filter(|k| *k).count();
    assert_eq!(new, 1);
    check_deliverable(&store.pool, &data.0.cid().unwrap(), false).await;

    let (_, delivered) = store
        .events_since_highwater_mark(0, i64::MAX)
        .await
        .unwrap();
    assert_eq!(0, delivered.len());

    let data = &events[2];

    let new = store
        .insert_events_from_carfiles(&[ReconItem::new(&data.0, &data.1)], false)
        .await
        .unwrap();
    let new = new.keys.into_iter().filter(|k| *k).count();
    assert_eq!(new, 1);
    check_deliverable(&store.pool, &data.0.cid().unwrap(), false).await;

    let (_, delivered) = store
        .events_since_highwater_mark(0, i64::MAX)
        .await
        .unwrap();
    assert_eq!(0, delivered.len());
    // now we add the init and we should see init, data 1 (first stored), data 2 (second stored) as highwater returns
    let data = &events[0];
    let new = store
        .insert_events_from_carfiles(&[ReconItem::new(&data.0, &data.1)], false)
        .await
        .unwrap();
    let new = new.keys.into_iter().filter(|k| *k).count();
    assert_eq!(new, 1);
    check_deliverable(&store.pool, &data.0.cid().unwrap(), true).await;

    // This happens out of band, so give it a moment to make sure everything is updated
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let (_, delivered) = store
        .events_since_highwater_mark(0, i64::MAX)
        .await
        .unwrap();
    assert_eq!(3, delivered.len());

    let expected = vec![
        events[0].0.cid().unwrap(),
        events[1].0.cid().unwrap(),
        events[2].0.cid().unwrap(),
    ];
    assert_eq!(expected, delivered);
}
