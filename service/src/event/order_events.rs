use std::collections::HashSet;

use ceramic_store::{CeramicOneEvent, EventInsertable, SqlitePool};
use cid::Cid;

use crate::Result;

use super::service::EventHeader;

pub(crate) struct OrderEvents {
    pub(crate) deliverable: Vec<(EventInsertable, EventHeader)>,
    pub(crate) missing_history: Vec<(EventInsertable, EventHeader)>,
}

impl OrderEvents {
    /// Groups the events into lists of those with a delivered prev and those without. This can be used to return an error if the event is required to have history.
    /// The events will be marked as deliverable so that they can be passed directly to the store to be persisted.
    /// Will look up the prev from the database if needed to check if it's deliverable (could possibly change this for recon and allow the ordering task to handle it?)
    pub async fn try_new(
        pool: &SqlitePool,
        mut candidate_events: Vec<(EventInsertable, EventHeader)>,
    ) -> Result<Self> {
        let new_cids: HashSet<Cid> =
            HashSet::from_iter(candidate_events.iter().map(|(e, _)| e.cid()));
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

        let mut prevs_in_memory = Vec::with_capacity(candidate_events.len());
        let mut missing_history = Vec::with_capacity(candidate_events.len());

        while let Some((mut event, header)) = candidate_events.pop() {
            match header.prev() {
                None => {
                    unreachable!("Init events should have been filtered out since they're always deliverable");
                }
                Some(prev) => {
                    if new_cids.contains(&prev) {
                        prevs_in_memory.push((event, header));
                        continue;
                    } else {
                        let (_exists, prev_deliverable) =
                            CeramicOneEvent::deliverable_by_cid(pool, &prev).await?;
                        if prev_deliverable {
                            event.body.set_deliverable(true);
                            deliverable.push((event, header));
                        } else {
                            // technically, we may have the "rosetta stone" event in memory that could unlock this chain, if we loaded everything and recursed,
                            // but the immediate prev is not in this set and has not been delivered to the client yet, so they shouldn't have known how to
                            // construct this event so we'll consider this missing history. This can be used to return an error if the event is required to have history.
                            missing_history.push((event, header));
                        }
                    }
                }
            }
        }

        // We add the events to the deliverable list until nothing changes
        while let Some((mut event, header)) = prevs_in_memory.pop() {
            let mut made_changes = false;
            match header.prev() {
                None => {
                    unreachable!("Init events should have been filtered out of the in memory set");
                }
                Some(prev) => {
                    // a hashset would be better loopkup but we're not going to have that many events so hashing
                    // for a handful of lookups and then convert back to a vec probably isn't worth it.
                    if deliverable.iter().any(|(e, _)| e.cid() == prev) {
                        event.body.set_deliverable(true);
                        deliverable.push((event, header));
                        made_changes = true;
                    } else {
                        prevs_in_memory.push((event, header));
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
