use std::collections::{HashMap, VecDeque};

use ceramic_store::{CeramicOneEvent, EventInsertable, SqlitePool};
use cid::Cid;

use crate::Result;

use super::service::EventMetadata;

pub(crate) struct OrderEvents {
    pub(crate) deliverable: Vec<(EventInsertable, EventMetadata)>,
    pub(crate) missing_history: Vec<(EventInsertable, EventMetadata)>,
}

impl OrderEvents {
    /// Groups the events into lists of those with a delivered prev and those without. This can be used to return an error if the event is required to have history.
    /// The events will be marked as deliverable so that they can be passed directly to the store to be persisted.
    /// Will look up the prev from the database if needed to check if it's deliverable (could possibly change this for recon and allow the ordering task to handle it?)
    pub async fn try_new(
        pool: &SqlitePool,
        mut candidate_events: Vec<(EventInsertable, EventMetadata)>,
    ) -> Result<Self> {
        let mut new_cids: HashMap<Cid, bool> = HashMap::from_iter(
            candidate_events
                .iter()
                .map(|(e, _)| (e.cid(), e.body.deliverable())),
        );
        let mut deliverable = Vec::with_capacity(candidate_events.len());
        candidate_events.retain(|(e, h)| {
            if e.deliverable() {
                deliverable.push((e.clone(), h.clone()));
                false
            } else {
                true
            }
        });
        if candidate_events.is_empty() {
            return Ok(OrderEvents {
                deliverable,
                missing_history: Vec::new(),
            });
        }

        let mut undelivered_prevs_in_memory = VecDeque::with_capacity(candidate_events.len());
        let mut missing_history = Vec::with_capacity(candidate_events.len());

        while let Some((mut event, header)) = candidate_events.pop() {
            match header.prev() {
                None => {
                    unreachable!("Init events should have been filtered out since they're always deliverable");
                }
                Some(prev) => {
                    if let Some(in_mem) = new_cids.get(&prev) {
                        if *in_mem {
                            event.body.set_deliverable(true);
                            *new_cids.get_mut(&event.cid()).expect("CID must exist") = true;
                            deliverable.push((event, header));
                        } else {
                            undelivered_prevs_in_memory.push_back((event, header));
                        }
                    } else {
                        let (_exists, prev_deliverable) =
                            CeramicOneEvent::deliverable_by_cid(pool, &prev).await?;
                        if prev_deliverable {
                            event.body.set_deliverable(true);
                            *new_cids.get_mut(&event.cid()).expect("CID must exist") = true;
                            deliverable.push((event, header));
                        } else {
                            missing_history.push((event, header));
                        }
                    }
                }
            }
        }

        // We continually loop through the set adding events to the deliverable list until nothing changes.
        // If our prev is in this list, we won't find it until it's added to the deliverable set. This means
        // we may loop through multiple times putting things back in the queue, but it should be a short list
        // and it will shrink every time we move something to the deliverable set, so it should be acceptable.
        // We can't quite get rid of this loop because we may have discovered our prev's prev from the database in the previous pass.
        while let Some((mut event, header)) = undelivered_prevs_in_memory.pop_front() {
            let mut made_changes = false;
            match header.prev() {
                None => {
                    unreachable!("Init events should have been filtered out of the in memory set");
                }
                Some(prev) => {
                    if new_cids.get(&prev).map_or(false, |v| *v) {
                        *new_cids.get_mut(&event.cid()).expect("CID must exist") = true;
                        event.body.set_deliverable(true);
                        deliverable.push((event, header));
                        made_changes = true;
                    } else {
                        undelivered_prevs_in_memory.push_back((event, header));
                    }
                }
            }
            if !made_changes {
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
