use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use crate::store::{EventAccess, EventInsertable};
use crate::Result;
use ceramic_core::Cid;

/// Groups the events into lists of those with a delivered prev and those without. This can be used to return an error if the event is required to have history.
/// The events will be marked as deliverable so that they can be passed directly to the store to be persisted. It assumes init events have already been marked deliverable.
///
/// The job of this function is different than the `ordering_task` module. That module recurses indefinitely attempting to build history. This
/// will only traverse a single prev outside of the initial set of events (that is, to the database). This is important because we don't want to
/// allow API users to write an event they haven't yet seen the prev for, and for recon we can allow the other task to sort it out.
///
/// The `missing_history` set means that the prev is not deliverable or in the `candidate_events` vec with a deliverable prev. For example,
/// with new events [C, D] (in any order), if D.prev = C, C.prev = B, then C and D are deliverable if B is in the database as deliverable.
/// Given the same situation, where B is not yet deliverable, but B.prev = A and A is deliverable (shouldn't happen, but as an example), we
/// *could* mark B deliverable and then C and D, but we DO NOT want to do this here to prevent API users from writing events that they haven't seen.
pub(crate) struct OrderEvents {
    deliverable: Vec<EventInsertable>,
    missing_history: Vec<EventInsertable>,
}

impl OrderEvents {
    pub fn deliverable(&self) -> &[EventInsertable] {
        &self.deliverable
    }

    pub fn missing_history(&self) -> &[EventInsertable] {
        &self.missing_history
    }
}

impl OrderEvents {
    /// Finds only those deliverable compared to the in memory set
    pub async fn find_deliverable_in_memory(
        candidate_events: Vec<EventInsertable>,
    ) -> Result<Self> {
        Self::find_deliverable_internal(None, candidate_events).await
    }

    /// Uses the in memory set and the database to try to follow prev chains and mark deliverable
    pub async fn find_currently_deliverable(
        event_access: Arc<EventAccess>,
        candidate_events: Vec<EventInsertable>,
    ) -> Result<Self> {
        Self::find_deliverable_internal(Some(event_access), candidate_events).await
    }

    /// Builds deliverable events, using the db pool if provided
    async fn find_deliverable_internal(
        event_access: Option<Arc<EventAccess>>,
        candidate_events: Vec<EventInsertable>,
    ) -> Result<Self> {
        let mut new_cids: HashMap<Cid, bool> = HashMap::with_capacity(candidate_events.len());

        let (mut deliverable, mut remaining_candidates): (Vec<_>, Vec<_>) =
            candidate_events.into_iter().partition(|e| {
                // all init events are deliverable and should already be marked as such
                if e.event().is_init() {
                    debug_assert!(e.deliverable())
                }
                new_cids.insert(*e.cid(), e.deliverable());
                e.deliverable()
            });

        if remaining_candidates.is_empty() {
            return Ok(OrderEvents {
                deliverable,
                missing_history: remaining_candidates,
            });
        }

        let mut undelivered_prevs_in_memory = VecDeque::with_capacity(remaining_candidates.len());
        let mut missing_history = Vec::with_capacity(remaining_candidates.len());

        while let Some(mut event) = remaining_candidates.pop() {
            match event.event().prev() {
                None => {
                    unreachable!("Init events should have been filtered out since they're always deliverable");
                }
                Some(prev) => {
                    if let Some(in_mem_is_deliverable) = new_cids.get(prev) {
                        if *in_mem_is_deliverable {
                            event.set_deliverable(true);
                            *new_cids.get_mut(event.cid()).expect("CID must exist") = true;
                            deliverable.push(event);
                        } else {
                            undelivered_prevs_in_memory.push_back(event);
                        }
                    } else if let Some(event_access) = &event_access {
                        let (_exists, prev_deliverable) =
                            event_access.deliverable_by_cid(prev).await?;
                        if prev_deliverable {
                            event.set_deliverable(true);
                            *new_cids.get_mut(event.cid()).expect("CID must exist") = true;
                            deliverable.push(event);
                        } else {
                            missing_history.push(event);
                        }
                    } else {
                        missing_history.push(event);
                    }
                }
            }
        }

        // We continually loop through the set adding events to the deliverable list until nothing changes.
        // If our prev is in this list, we won't find it until it's added to the deliverable set. This means
        // we may loop through multiple times putting things back in the queue, but it should be a short list
        // and it will shrink every time we move something to the deliverable set, so it should be acceptable.
        // We can't quite get rid of this loop because we may have discovered our prev's prev from the database in the previous pass.
        let max_iterations = undelivered_prevs_in_memory.len();
        let mut iteration = 0;
        while let Some(mut event) = undelivered_prevs_in_memory.pop_front() {
            iteration += 1;
            match event.event().prev() {
                None => {
                    unreachable!("Init events should have been filtered out of the in memory set");
                }
                Some(prev) => {
                    if new_cids.get(prev).is_some_and(|v| *v) {
                        *new_cids.get_mut(event.cid()).expect("CID must exist") = true;
                        event.set_deliverable(true);
                        deliverable.push(event);
                        // reset the iteration count since we made changes. once it doesn't change for a loop through the queue we're done
                        iteration = 0;
                    } else {
                        undelivered_prevs_in_memory.push_back(event);
                    }
                }
            }
            if iteration >= max_iterations {
                missing_history.extend(undelivered_prevs_in_memory);
                break;
            }
        }

        Ok(OrderEvents {
            deliverable,
            missing_history,
        })
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use ceramic_core::EventId;
    use ceramic_sql::sqlite::SqlitePool;
    use rand::seq::SliceRandom;
    use rand::thread_rng;
    use recon::ReconItem;
    use test_log::test;

    use super::*;

    use crate::{
        event::validator::{UnvalidatedEvent, ValidatedEvent},
        tests::get_n_events,
    };

    async fn get_2_streams() -> (
        Vec<ReconItem<EventId>>,
        Vec<ReconItem<EventId>>,
        Vec<EventInsertable>,
    ) {
        let stream_2 = get_n_events(10).await;
        let stream_1 = get_n_events(10).await;
        let mut to_insert = Vec::with_capacity(10);
        for event in stream_1.iter().chain(stream_2.iter()) {
            let insertable = ValidatedEvent::into_insertable(
                ValidatedEvent::from_unvalidated_unchecked(
                    UnvalidatedEvent::try_from(event).unwrap(),
                ),
                None,
            );
            to_insert.push(insertable);
        }
        (stream_1, stream_2, to_insert)
    }

    /// Asserts the events are deliverable and returns IDs for events in stream_1 as the first value and things in stream_2 as the second
    fn split_deliverable_order_by_stream(
        stream_1: &[ReconItem<EventId>],
        stream_2: &[ReconItem<EventId>],
        events: &[EventInsertable],
    ) -> (Vec<EventId>, Vec<EventId>) {
        let mut after_1 = Vec::with_capacity(stream_1.len());
        let mut after_2 = Vec::with_capacity(stream_2.len());
        for event in events {
            assert!(event.deliverable());
            if stream_1.iter().any(|e| e.key == *event.order_key()) {
                after_1.push(event.order_key().clone());
            } else {
                after_2.push(event.order_key().clone());
            }
        }

        (after_1, after_2)
    }

    /// Takes the given events from Recon and turns them into two vectors of insertable events.
    async fn get_insertable_events(
        events: &[ReconItem<EventId>],
        first_vec_count: usize,
    ) -> (Vec<EventInsertable>, Vec<EventInsertable>) {
        let mut insertable = Vec::with_capacity(first_vec_count);
        let mut remaining = Vec::with_capacity(events.len() - first_vec_count);
        for (i, event) in events.iter().enumerate() {
            let new = ValidatedEvent::into_insertable(
                ValidatedEvent::from_unvalidated_unchecked(
                    UnvalidatedEvent::try_from(event).unwrap(),
                ),
                None,
            );
            if i < first_vec_count {
                insertable.push(new);
            } else {
                remaining.push(new)
            }
        }

        (insertable, remaining)
    }

    #[test(tokio::test)]
    async fn out_of_order_streams_valid() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let event_access = Arc::new(EventAccess::try_new(pool).await.unwrap());

        let (stream_1, stream_2, mut to_insert) = get_2_streams().await;
        to_insert.shuffle(&mut thread_rng());

        let ordered = OrderEvents::find_currently_deliverable(event_access, to_insert)
            .await
            .unwrap();
        assert!(
            ordered.missing_history.is_empty(),
            "Missing history: len={} {:?}",
            ordered.missing_history.len(),
            ordered.missing_history
        );
        let (after_1, after_2) =
            split_deliverable_order_by_stream(&stream_1, &stream_2, ordered.deliverable());

        assert_eq!(
            stream_1.into_iter().map(|e| e.key).collect::<Vec<_>>(),
            after_1
        );
        assert_eq!(
            stream_2.into_iter().map(|e| e.key).collect::<Vec<_>>(),
            after_2
        );
    }

    #[test(tokio::test)]
    async fn missing_history_in_memory() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let event_access = Arc::new(EventAccess::try_new(pool).await.unwrap());

        let (stream_1, stream_2, mut to_insert) = get_2_streams().await;
        // if event 2 is missing from stream_1, we will sort stream_2 but stream_1 will be "missing history" after the init event
        to_insert.remove(1);
        to_insert.shuffle(&mut thread_rng());

        let ordered = OrderEvents::find_currently_deliverable(event_access, to_insert)
            .await
            .unwrap();
        assert_eq!(
            8,
            ordered.missing_history.len(),
            "Missing history: {:?}",
            ordered.missing_history
        );
        let (after_1, after_2) =
            split_deliverable_order_by_stream(&stream_1, &stream_2, ordered.deliverable());

        assert_eq!(vec![stream_1[0].key.clone()], after_1);
        assert_eq!(
            stream_2.into_iter().map(|e| e.key).collect::<Vec<_>>(),
            after_2
        );
    }

    #[test(tokio::test)]
    async fn missing_history_not_recursed() {
        // this test validates that even though it's possible to build the history as deliverable, we don't do it here
        // so that an API write that had never seen event 2, would not able to write event 3 or after
        // the recon ordering task would sort this and mark all deliverable
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let event_access = Arc::new(EventAccess::try_new(pool).await.unwrap());

        let stream_1 = get_n_events(10).await;
        let (to_insert, mut remaining) = get_insertable_events(&stream_1, 3).await;
        event_access.insert_many(to_insert.iter()).await.unwrap();

        remaining.shuffle(&mut thread_rng());

        let ordered = OrderEvents::find_currently_deliverable(event_access, remaining)
            .await
            .unwrap();
        assert_eq!(
            7,
            ordered.missing_history.len(),
            "Missing history: {:?}",
            ordered.missing_history
        );
    }

    #[test(tokio::test)]
    async fn database_deliverable_is_valid() {
        // this test validates we can order in memory events with each other if one of them has a prev
        // in the database that is deliverable, in which case the entire chain is deliverable
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let event_access = Arc::new(EventAccess::try_new(pool).await.unwrap());

        let stream_1 = get_n_events(10).await;
        let (mut to_insert, mut remaining) = get_insertable_events(&stream_1, 3).await;
        for item in to_insert.as_mut_slice() {
            item.set_deliverable(true)
        }

        event_access.insert_many(to_insert.iter()).await.unwrap();

        let expected = remaining
            .iter()
            .map(|i| i.order_key().clone())
            .collect::<Vec<_>>();
        remaining.shuffle(&mut thread_rng());

        let ordered = OrderEvents::find_currently_deliverable(event_access, remaining)
            .await
            .unwrap();
        assert!(
            ordered.missing_history.is_empty(),
            "Missing history: {:?}",
            ordered.missing_history
        );
        let after = ordered
            .deliverable
            .iter()
            .map(|e| e.order_key().clone())
            .collect::<Vec<_>>();
        assert_eq!(expected, after);
    }
}
