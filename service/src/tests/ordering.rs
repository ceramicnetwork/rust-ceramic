use ceramic_api::AccessModelStore;
use ceramic_core::EventId;
use rand::seq::SliceRandom;
use rand::thread_rng;
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

async fn add_and_assert_new_recon_event(store: &CeramicEventService, item: ReconItem<'_, EventId>) {
    tracing::trace!("inserted event: {}", item.key.cid().unwrap());
    let new = store
        .insert_events_from_carfiles_remote_history(&[item])
        .await
        .unwrap();
    let new = new.keys.into_iter().filter(|k| *k).count();
    assert_eq!(1, new);
}

async fn add_and_assert_new_local_event(store: &CeramicEventService, item: ReconItem<'_, EventId>) {
    let new = store
        .insert_events_from_carfiles_local_history(&[item])
        .await
        .unwrap();
    let new = new.keys.into_iter().filter(|k| *k).count();
    assert_eq!(1, new);
}

#[tokio::test]
async fn test_init_event_delivered() {
    let store = setup_service().await;
    let events = get_events().await;
    let init = &events[0];
    add_and_assert_new_local_event(&store, ReconItem::new(&init.0, &init.1)).await;
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
                        .contains("Failed to discover history for all events"));
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
    add_and_assert_new_local_event(&store, ReconItem::new(&init.0, &init.1)).await;
    check_deliverable(&store.pool, &init.0.cid().unwrap(), true).await;

    add_and_assert_new_local_event(&store, ReconItem::new(&data.0, &data.1)).await;
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
    add_and_assert_new_recon_event(&store, ReconItem::new(&data.0, &data.1)).await;
    check_deliverable(&store.pool, &data.0.cid().unwrap(), false).await;

    let (_, delivered) = store
        .events_since_highwater_mark(0, i64::MAX)
        .await
        .unwrap();
    assert_eq!(0, delivered.len());

    let data = &events[2];
    add_and_assert_new_recon_event(&store, ReconItem::new(&data.0, &data.1)).await;

    check_deliverable(&store.pool, &data.0.cid().unwrap(), false).await;

    let (_, delivered) = store
        .events_since_highwater_mark(0, i64::MAX)
        .await
        .unwrap();
    assert_eq!(0, delivered.len());
    // now we add the init and we should see init, data 1 (first stored), data 2 (second stored) as highwater returns
    let data = &events[0];
    add_and_assert_new_recon_event(&store, ReconItem::new(&data.0, &data.1)).await;
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

#[tokio::test]
async fn missing_prev_pending_recon_should_deliver_without_stream_update() {
    let store = setup_service().await;
    let events = get_events().await;

    let data = &events[0];
    add_and_assert_new_recon_event(&store, ReconItem::new(&data.0, &data.1)).await;
    check_deliverable(&store.pool, &data.0.cid().unwrap(), true).await;

    // now we add the second event, it should quickly become deliverable
    let data = &events[1];
    add_and_assert_new_recon_event(&store, ReconItem::new(&data.0, &data.1)).await;
    check_deliverable(&store.pool, &data.0.cid().unwrap(), false).await;
    // This happens out of band, so give it a moment to make sure everything is updated
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let (_, delivered) = store
        .events_since_highwater_mark(0, i64::MAX)
        .await
        .unwrap();
    assert_eq!(2, delivered.len());

    let expected = vec![events[0].0.cid().unwrap(), events[1].0.cid().unwrap()];
    assert_eq!(expected, delivered);
}

#[tokio::test]
async fn multiple_streams_missing_prev_recon_should_deliver_without_stream_update() {
    let store = setup_service().await;
    let stream_1 = get_events().await;
    let stream_2 = get_events().await;

    let s1_init = &stream_1[0];
    let s1_2 = &stream_1[1];
    let s1_3 = &stream_1[2];

    let s2_init = &stream_2[0];
    let s2_2 = &stream_2[1];
    let s2_3 = &stream_2[2];

    // store the first event in both streams.
    // we could do insert as a list, but to make sure we have the ordering we expect at the end we do them one by one
    add_and_assert_new_recon_event(&store, ReconItem::new(&s1_init.0, &s1_init.1)).await;
    check_deliverable(&store.pool, &s1_init.0.cid().unwrap(), true).await;

    add_and_assert_new_recon_event(&store, ReconItem::new(&s2_init.0, &s2_init.1)).await;
    check_deliverable(&store.pool, &s2_init.0.cid().unwrap(), true).await;

    // now we add the third event for both and they should be stuck in pending
    add_and_assert_new_recon_event(&store, ReconItem::new(&s1_3.0, &s1_3.1)).await;
    check_deliverable(&store.pool, &s1_3.0.cid().unwrap(), false).await;

    add_and_assert_new_recon_event(&store, ReconItem::new(&s2_3.0, &s2_3.1)).await;
    check_deliverable(&store.pool, &s2_3.0.cid().unwrap(), false).await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (_, delivered) = store
        .events_since_highwater_mark(0, i64::MAX)
        .await
        .unwrap();
    assert_eq!(2, delivered.len());

    // now we add the second event for stream 1, it should unlock the first stream completely but not the second
    add_and_assert_new_recon_event(&store, ReconItem::new(&s1_2.0, &s1_2.1)).await;

    // this _could_ be deliverable immediately if we checked but for now we just send to the other task,
    // so `check_deliverable` could return true or false depending on timing (but probably false).
    // as this is an implementation detail and we'd prefer true, we just use HW ordering to make sure it's been delivered
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let (_, delivered) = store
        .events_since_highwater_mark(0, i64::MAX)
        .await
        .unwrap();
    assert_eq!(4, delivered.len());
    let expected: Vec<cid::CidGeneric<64>> = vec![
        s1_init.0.cid().unwrap(),
        s2_init.0.cid().unwrap(),
        s1_2.0.cid().unwrap(),
        s1_3.0.cid().unwrap(),
    ];
    assert_eq!(expected, delivered);

    // now discover the second event for the second stream and everything will be stored
    add_and_assert_new_recon_event(&store, ReconItem::new(&s2_2.0, &s2_2.1)).await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (_, delivered) = store
        .events_since_highwater_mark(0, i64::MAX)
        .await
        .unwrap();
    let expected = vec![
        s1_init.0.cid().unwrap(),
        s2_init.0.cid().unwrap(),
        s1_2.0.cid().unwrap(),
        s1_3.0.cid().unwrap(),
        s2_2.0.cid().unwrap(),
        s2_3.0.cid().unwrap(),
    ];
    assert_eq!(expected, delivered);
}

async fn validate_all_delivered(store: &CeramicEventService, expected_delivered: usize) {
    loop {
        let (_, delivered) = store
            .events_since_highwater_mark(0, i64::MAX)
            .await
            .unwrap();
        let total = delivered.len();
        if total < expected_delivered {
            tracing::trace!(
                "found {} delivered, waiting for {}",
                total,
                expected_delivered
            );
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        } else {
            break;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recon_lots_of_streams() {
    // adds 101 events to 10 streams, mixes up the event order for each stream, inserts half
    // the events for each stream before mixing up the stream order and inserting the rest
    let per_stream = 100;
    let num_streams = 10;
    let store = setup_service().await;
    let mut streams = Vec::new();
    let mut all_cids = Vec::new();
    let expected = per_stream * num_streams;
    for _ in 0..num_streams {
        let mut events = crate::tests::get_n_events(per_stream - 1).await;
        let cids = events
            .iter()
            .map(|e| e.0.cid().unwrap())
            .collect::<Vec<_>>();
        all_cids.extend(cids);
        assert_eq!(per_stream, events.len());
        events.shuffle(&mut thread_rng());
        streams.push(events);
    }
    let mut total_added = 0;

    assert_eq!(expected, all_cids.len());
    tracing::debug!(?all_cids, "starting test");
    for stream in streams.iter_mut() {
        while let Some(event) = stream.pop() {
            if stream.len() > per_stream / 2 {
                total_added += 1;
                add_and_assert_new_recon_event(&store, ReconItem::new(&event.0, &event.1)).await;
            } else {
                total_added += 1;
                add_and_assert_new_recon_event(&store, ReconItem::new(&event.0, &event.1)).await;
                break;
            }
        }
    }
    streams.shuffle(&mut thread_rng());
    for stream in streams.iter_mut() {
        while let Some(event) = stream.pop() {
            total_added += 1;
            add_and_assert_new_recon_event(&store, ReconItem::new(&event.0, &event.1)).await;
        }
    }
    // first just make sure they were all inserted (not delivered yet)
    for (i, cid) in all_cids.iter().enumerate() {
        let (exists, _delivered) =
            ceramic_store::CeramicOneEvent::delivered_by_cid(&store.pool, cid)
                .await
                .unwrap();
        assert!(exists, "idx: {}. missing cid: {}", i, cid);
    }
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    assert_eq!(expected, total_added);
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        validate_all_delivered(&store, expected),
    )
    .await
    .unwrap();

    let (_, delivered) = store
        .events_since_highwater_mark(0, i64::MAX)
        .await
        .unwrap();

    assert_eq!(expected, delivered.len());
    // now we check that all the events are deliverable
    for cid in all_cids.iter() {
        check_deliverable(&store.pool, cid, true).await;
    }
}
