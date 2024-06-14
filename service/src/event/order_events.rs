use std::collections::HashSet;

use ceramic_store::{CeramicOneEvent, EventInsertable, SqlitePool};
use cid::Cid;

use crate::Result;

pub(crate) struct OrderEvents {
    pub(crate) deliverable: Vec<EventInsertable>,
    pub(crate) missing_history: Vec<EventInsertable>,
}

impl OrderEvents {
    /// Groups the events into lists those with a delivered prev and those without. This can be used to return an error if the event is required to have history.
    /// The events will be marked as deliverable so that they can be passed directly to the store to be persisted.
    pub async fn try_new(
        pool: &SqlitePool,
        mut candidate_events: Vec<EventInsertable>,
    ) -> Result<Self> {
        // move all the init events to the front so we make sure to add them first and get the deliverable order correct
        let new_cids: HashSet<Cid> = HashSet::from_iter(candidate_events.iter().map(|e| e.cid()));
        let mut deliverable = Vec::with_capacity(candidate_events.len());
        candidate_events.retain(|e| {
            if e.deliverable() {
                deliverable.push(e.clone());
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

        let mut prevs_in_memory = Vec::with_capacity(candidate_events.len());
        let mut missing_history = Vec::with_capacity(candidate_events.len());

        while let Some(mut event) = candidate_events.pop() {
            match &event.prev() {
                None => {
                    unreachable!("Init events should have been filtered out since they're always deliverable");
                }
                Some(prev) => {
                    if new_cids.contains(prev) {
                        prevs_in_memory.push(event.clone());
                        continue;
                    } else {
                        let (_exists, prev_deliverable) =
                            CeramicOneEvent::deliverable_by_cid(pool, prev).await?;
                        if prev_deliverable {
                            event.set_deliverable(true);
                            deliverable.push(event);
                        } else {
                            // technically, we may have the "rosetta stone" event in memory that could unlock this chain, if we loaded everything and recursed,
                            // but the immediate prev is not in this set and has not been delivered to the client yet, so they shouldn't have known how to
                            // construct this event so we'll consider this missing history. This can be used to return an error if the event is required to have history.
                            missing_history.push(event);
                        }
                    }
                }
            }
        }

        // We add the events to the deliverable list until nothing changes.
        // It should be a small set and it will shrink each loop, so continually looping is acceptable.
        loop {
            let mut made_changes = false;
            while let Some(mut event) = prevs_in_memory.pop() {
                match &event.prev() {
                    None => {
                        unreachable!(
                            "Init events should have been filtered out of the in memory set"
                        );
                    }
                    Some(prev) => {
                        // a hashset would be better loopkup but we're not going to have that many events so hashing
                        // for a handful of lookups and then convert back to a vec probably isn't worth it.
                        if deliverable.iter().any(|e| e.cid() == *prev) {
                            event.set_deliverable(true);
                            deliverable.push(event);
                            made_changes = true;
                        } else {
                            prevs_in_memory.push(event);
                        }
                    }
                }
            }
            if !made_changes {
                missing_history.extend(prevs_in_memory);
                break;
            }
        }

        Ok(OrderEvents {
            deliverable,
            missing_history,
        })
    }
}
