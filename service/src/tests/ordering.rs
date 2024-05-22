use ceramic_api::EventStore;
use ceramic_core::EventId;
use recon::ReconItem;

use crate::{
    tests::{check_deliverable, get_events},
    CeramicEventService,
};

async fn setup_service() -> CeramicEventService {
    let _ = ceramic_metrics::init_local_tracing();
    let conn = ceramic_store::SqlitePool::connect_in_memory()
        .await
        .unwrap();
    CeramicEventService::new_without_undelivered(conn)
        .await
        .unwrap()
}

#[tokio::test]
async fn test_init_event_delivered() {
    let store = setup_service().await;
    let events = get_events().await;
    let init = &events[0];
    let new = store
        .insert_events_from_carfiles_local_history(&[ReconItem::new(&init.0, &init.1)])
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
        .insert_events_from_carfiles_local_history(&[ReconItem::new(&data.0, &data.1)])
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
        .insert_events_from_carfiles_local_history(&[ReconItem::new(&init.0, &init.1)])
        .await
        .unwrap();
    let new = new.keys.into_iter().filter(|k| *k).count();
    assert_eq!(1, new);
    check_deliverable(&store.pool, &init.0.cid().unwrap(), true).await;
    let new = store
        .insert_events_from_carfiles_local_history(&[ReconItem::new(&data.0, &data.1)])
        .await
        .unwrap();
    let new = new.keys.into_iter().filter(|k| *k).count();
    assert_eq!(1, new);
    check_deliverable(&store.pool, &data.0.cid().unwrap(), true).await;

    let (_, delivered) = store
        .events_since_highwater_mark(0, i64::MAX)
        .await
        .unwrap();
    assert_eq!(2, delivered.len());

    let expected = vec![init.0.cid().unwrap(), data.0.cid().unwrap()];
    assert_eq!(expected, delivered);
}

#[tokio::test]
async fn test_prev_in_same_write_history_required() {
    let store = setup_service().await;
    let events = get_events().await;
    let init: &(EventId, Vec<u8>) = &events[0];
    let data = &events[1];
    let new = store
        .insert_events_from_carfiles_local_history(&[
            ReconItem::new(&data.0, &data.1),
            ReconItem::new(&init.0, &init.1),
        ])
        .await
        .unwrap();
    let new = new.keys.into_iter().filter(|k| *k).count();
    assert_eq!(2, new);
    check_deliverable(&store.pool, &init.0.cid().unwrap(), true).await;
    check_deliverable(&store.pool, &data.0.cid().unwrap(), true).await;
    let (_, delivered) = store
        .events_since_highwater_mark(0, i64::MAX)
        .await
        .unwrap();
    assert_eq!(2, delivered.len());

    let expected = vec![init.0.cid().unwrap(), data.0.cid().unwrap()];
    assert_eq!(expected, delivered);
}

#[tokio::test]
async fn test_missing_prev_pending_recon() {
    let store = setup_service().await;
    let events = get_events().await;
    let data = &events[1];
    let new = store
        .insert_events_from_carfiles_remote_history(&[ReconItem::new(&data.0, &data.1)])
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
        .insert_events_from_carfiles_remote_history(&[ReconItem::new(&data.0, &data.1)])
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
        .insert_events_from_carfiles_remote_history(&[ReconItem::new(&data.0, &data.1)])
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
