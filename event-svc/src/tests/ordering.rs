use std::collections::HashMap;

use ceramic_api::{EventDataResult, EventStore, IncludeEventData};
use ceramic_core::EventId;
use rand::seq::SliceRandom;
use rand::thread_rng;
use recon::ReconItem;
use test_log::test;

use crate::{
    event::DeliverableRequirement,
    tests::{check_deliverable, get_events},
    CeramicEventService,
};

async fn setup_service() -> CeramicEventService {
    let conn = crate::store::SqlitePool::connect_in_memory().await.unwrap();

    CeramicEventService::new_with_event_validation(conn)
        .await
        .unwrap()
}

async fn add_and_assert_new_recon_event(store: &CeramicEventService, item: ReconItem<EventId>) {
    tracing::trace!("inserted event: {}", item.key.cid().unwrap());
    let new = recon::Store::insert_many(store, &[item]).await.unwrap();
    assert!(new.included_new_key());
}

async fn add_and_assert_new_local_event(store: &CeramicEventService, item: ReconItem<EventId>) {
    let new = store
        .insert_events(&[item], DeliverableRequirement::Immediate)
        .await
        .unwrap();
    let new = new.store_result.count_new_keys();
    assert_eq!(1, new);
}

async fn get_delivered_cids(store: &CeramicEventService) -> Vec<ceramic_core::Cid> {
    let (_, delivered) = store
        .events_since_highwater_mark(0, i64::MAX, IncludeEventData::Full)
        .await
        .unwrap();
    delivered.iter().map(|e| e.id).collect()
}

#[test(tokio::test)]
async fn test_init_event_delivered() {
    let store = setup_service().await;
    let events = get_events().await;
    let init = &events[0];
    add_and_assert_new_local_event(&store, init.to_owned()).await;
    check_deliverable(&store.pool, &init.key.cid().unwrap(), true).await;
}

#[test(tokio::test)]
async fn test_missing_prev_history_required_not_inserted() {
    let store = setup_service().await;
    let events = get_events().await;
    let data = &events[1];

    let new = store
        .insert_events(&[data.to_owned()], DeliverableRequirement::Immediate)
        .await
        .unwrap();
    assert!(new.store_result.inserted.is_empty());
    assert_eq!(1, new.rejected.len());
}

#[test(tokio::test)]
async fn test_prev_exists_history_required() {
    let store = setup_service().await;
    let events = get_events().await;
    let init = &events[0];
    let data = &events[1];
    add_and_assert_new_local_event(&store, init.clone()).await;
    check_deliverable(&store.pool, &init.key.cid().unwrap(), true).await;

    add_and_assert_new_local_event(&store, data.clone()).await;
    check_deliverable(&store.pool, &data.key.cid().unwrap(), true).await;

    let delivered = get_delivered_cids(&store).await;
    assert_eq!(2, delivered.len());

    let expected = vec![init.key.cid().unwrap(), data.key.cid().unwrap()];
    assert_eq!(expected, delivered);
}

#[test(tokio::test)]
async fn test_prev_in_same_write_history_required() {
    let store = setup_service().await;
    let events = get_events().await;
    let init = &events[0];
    let data = &events[1];
    let new = store
        .insert_events(
            &[init.to_owned(), data.to_owned()],
            DeliverableRequirement::Immediate,
        )
        .await
        .unwrap();
    let new = new.store_result.count_new_keys();
    assert_eq!(2, new);
    check_deliverable(&store.pool, &init.key.cid().unwrap(), true).await;
    check_deliverable(&store.pool, &data.key.cid().unwrap(), true).await;
    let delivered = get_delivered_cids(&store).await;

    assert_eq!(2, delivered.len());

    let expected = vec![init.key.cid().unwrap(), data.key.cid().unwrap()];
    assert_eq!(expected, delivered);
}

#[test(tokio::test)]
async fn test_missing_prev_pending_recon() {
    let store = setup_service().await;
    let events = get_events().await;
    let data = &events[1];
    add_and_assert_new_recon_event(&store, data.to_owned()).await;
    check_deliverable(&store.pool, &data.key.cid().unwrap(), false).await;

    let delivered = get_delivered_cids(&store).await;
    assert_eq!(0, delivered.len());

    let data = &events[2];
    add_and_assert_new_recon_event(&store, data.to_owned()).await;

    check_deliverable(&store.pool, &data.key.cid().unwrap(), false).await;

    let delivered = get_delivered_cids(&store).await;

    assert_eq!(0, delivered.len());
    // now we add the init and we should see init, data 1 (first stored), data 2 (second stored) as highwater returns
    let data = &events[0];
    add_and_assert_new_recon_event(&store, data.to_owned()).await;
    check_deliverable(&store.pool, &data.key.cid().unwrap(), true).await;

    // This happens out of band, so give it a moment to make sure everything is updated
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let delivered = get_delivered_cids(&store).await;

    assert_eq!(3, delivered.len());

    let expected = vec![
        events[0].key.cid().unwrap(),
        events[1].key.cid().unwrap(),
        events[2].key.cid().unwrap(),
    ];
    assert_eq!(expected, delivered);
}

#[test(tokio::test)]
async fn missing_prev_pending_recon_should_deliver_without_stream_update() {
    let store = setup_service().await;
    let events = get_events().await;

    let data = &events[0];
    add_and_assert_new_recon_event(&store, data.to_owned()).await;
    check_deliverable(&store.pool, &data.key.cid().unwrap(), true).await;

    // now we add the second event, it should quickly become deliverable
    let data = &events[1];
    add_and_assert_new_recon_event(&store, data.to_owned()).await;
    // This happens out of band, so give it a moment to make sure everything is updated
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let delivered = get_delivered_cids(&store).await;
    assert_eq!(2, delivered.len());

    let expected = vec![events[0].key.cid().unwrap(), events[1].key.cid().unwrap()];
    assert_eq!(expected, delivered);
}

#[test(test(tokio::test))]
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
    add_and_assert_new_recon_event(&store, s1_init.to_owned()).await;
    check_deliverable(&store.pool, &s1_init.key.cid().unwrap(), true).await;

    add_and_assert_new_recon_event(&store, s2_init.to_owned()).await;
    check_deliverable(&store.pool, &s2_init.key.cid().unwrap(), true).await;

    // now we add the third event for both and they should be stuck in pending
    add_and_assert_new_recon_event(&store, s1_3.to_owned()).await;
    check_deliverable(&store.pool, &s1_3.key.cid().unwrap(), false).await;

    add_and_assert_new_recon_event(&store, s2_3.to_owned()).await;
    check_deliverable(&store.pool, &s2_3.key.cid().unwrap(), false).await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let delivered = get_delivered_cids(&store).await;
    assert_eq!(2, delivered.len());

    // now we add the second event for stream 1, it should unlock the first stream completely but not the second
    add_and_assert_new_recon_event(&store, s1_2.to_owned()).await;

    // this _could_ be deliverable immediately if we checked but for now we just send to the other task,
    // so `check_deliverable` could return true or false depending on timing (but probably false).
    // as this is an implementation detail and we'd prefer true, we just use HW ordering to make sure it's been delivered
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let delivered = get_delivered_cids(&store).await;
    assert_eq!(4, delivered.len());
    let expected: Vec<cid::CidGeneric<64>> = vec![
        s1_init.key.cid().unwrap(),
        s2_init.key.cid().unwrap(),
        s1_2.key.cid().unwrap(),
        s1_3.key.cid().unwrap(),
    ];
    assert_eq!(expected, delivered);

    // now discover the second event for the second stream and everything will be stored
    add_and_assert_new_recon_event(&store, s2_2.to_owned()).await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let delivered = get_delivered_cids(&store).await;

    let expected = vec![
        s1_init.key.cid().unwrap(),
        s2_init.key.cid().unwrap(),
        s1_2.key.cid().unwrap(),
        s1_3.key.cid().unwrap(),
        s2_2.key.cid().unwrap(),
        s2_3.key.cid().unwrap(),
    ];
    assert_eq!(expected, delivered);
}

async fn validate_all_delivered(store: &CeramicEventService, expected_delivered: usize) {
    loop {
        let (_, delivered) = store
            .events_since_highwater_mark(0, i64::MAX, IncludeEventData::None)
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

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn recon_lots_of_streams() {
    // adds `per_stream` events to `num_streams` streams, mixes up the event order for each stream, inserts half
    // the events for each stream before mixing up the stream order and inserting the rest
    // took like a minute on my machine to run 100 streams with 1000 events each, mostly inserting :( since ordering only gets 5 seconds
    let per_stream = 100;
    let num_streams = 10;
    let store = setup_service().await;
    let expected = per_stream * num_streams;
    let mut streams = Vec::new();
    let mut all_cids = Vec::new();
    let mut expected_stream_order = Vec::new();
    let mut cid_to_stream_map = HashMap::new();

    for i in 0..num_streams {
        let mut events = crate::tests::get_n_events(per_stream).await;
        let cids = events
            .iter()
            .map(|e| e.key.cid().unwrap())
            .collect::<Vec<_>>();
        cids.iter().for_each(|cid| {
            cid_to_stream_map.insert(*cid, i);
        });
        expected_stream_order.push(cids.clone());
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
                add_and_assert_new_recon_event(&store, event.to_owned()).await;
            } else {
                total_added += 1;
                add_and_assert_new_recon_event(&store, event.to_owned()).await;
                break;
            }
        }
    }
    streams.shuffle(&mut thread_rng());
    for stream in streams.iter_mut() {
        while let Some(event) = stream.pop() {
            total_added += 1;
            add_and_assert_new_recon_event(&store, event.to_owned()).await;
        }
    }
    // first just make sure they were all inserted (not delivered yet)
    for (i, cid) in all_cids.iter().enumerate() {
        let (exists, _delivered) =
            crate::store::CeramicOneEvent::deliverable_by_cid(&store.pool, cid)
                .await
                .unwrap();
        assert!(exists, "idx: {}. missing cid: {}", i, cid);
    }

    assert_eq!(expected, total_added);
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        validate_all_delivered(&store, expected),
    )
    .await
    .unwrap();

    let (_, delivered) = store
        .events_since_highwater_mark(0, i64::MAX, IncludeEventData::None)
        .await
        .unwrap();

    assert_eq!(expected, delivered.len());
    let mut streams_at_the_end = Vec::new();
    for _ in 0..num_streams {
        streams_at_the_end.push(Vec::with_capacity(per_stream));
    }
    for EventDataResult { id: cid, .. } in delivered {
        let stream = cid_to_stream_map.get(&cid).unwrap();
        let stream = streams_at_the_end.get_mut(*stream).unwrap();
        stream.push(cid);
    }
    // now we check that all the events are deliverable
    for cid in all_cids.iter() {
        check_deliverable(&store.pool, cid, true).await;
    }
    // and make sure the events were delivered for each stream streams in the same order as they were at the start
    for (i, stream) in expected_stream_order.iter().enumerate() {
        assert_eq!(*stream, streams_at_the_end[i]);
    }
}
