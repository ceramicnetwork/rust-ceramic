use std::collections::HashMap;

use ceramic_core::EventId;
use ceramic_event::unvalidated;
use ceramic_store::{CeramicOneEvent, EventInsertable, EventInsertableBody, SqlitePool};
use cid::Cid;
use ipld_core::ipld::Ipld;
use recon::{InsertResult, ReconItem};
use tracing::{info, trace, warn};

use super::ordering_task::{
    DeliverableMetadata, DeliverableTask, DeliveredEvent, DiscoveredEvent, OrderingState,
    OrderingTask,
};

use crate::{Error, Result};

/// The max number of events we can have pending for delivery in the channel before we start dropping them.
/// This quickly becomes a bottleneck with very long streams and high throughput.
pub(crate) const PENDING_EVENTS_CHANNEL_DEPTH: usize = 10_000;

#[derive(Debug)]
/// A database store that verifies the bytes it stores are valid Ceramic events.
/// Implements the [`recon::Store`], [`iroh_bitswap::Store`], and [`ceramic_api::AccessModelStore`] traits for [`ceramic_core::EventId`].
pub struct CeramicEventService {
    pub(crate) pool: SqlitePool,
    delivery_task: DeliverableTask,
}

impl CeramicEventService {
    /// Create a new CeramicEventStore
    pub async fn new(pool: SqlitePool) -> Result<Self> {
        CeramicOneEvent::init_delivered_order(&pool).await?;

        let delivery_task =
            OrderingTask::run(pool.clone(), PENDING_EVENTS_CHANNEL_DEPTH, true).await;

        Ok(Self {
            pool,
            delivery_task,
        })
    }

    /// Skip loading all undelivered events from the database on startup (for testing)
    #[cfg(test)]
    pub(crate) async fn new_without_undelivered(pool: SqlitePool) -> Result<Self> {
        CeramicOneEvent::init_delivered_order(&pool).await?;

        let delivery_task =
            OrderingTask::run(pool.clone(), PENDING_EVENTS_CHANNEL_DEPTH, false).await;

        Ok(Self {
            pool,
            delivery_task,
        })
    }

    /// merge_from_sqlite takes the filepath to a sqlite file.
    /// If the file dose not exist the ATTACH DATABASE command will create it.
    /// This function assumes that the database contains a table named blocks with cid, bytes columns.
    pub async fn merge_from_sqlite(&self, input_ceramic_db_filename: &str) -> Result<()> {
        self.pool
            .merge_blocks_from_sqlite(input_ceramic_db_filename)
            .await?;
        Ok(())
    }

    /// Backup the database to a filepath output_ceramic_db_filename.
    pub async fn backup_to_sqlite(&self, output_ceramic_db_filename: &str) -> Result<()> {
        self.pool
            .backup_to_sqlite(output_ceramic_db_filename)
            .await?;
        Ok(())
    }

    /// This function is used to parse the event from the carfile and return the insertable event and the previous cid pointer.
    /// Probably belongs in the event crate.
    pub(crate) async fn parse_event_carfile(
        event_cid: cid::Cid,
        carfile: &[u8],
    ) -> Result<(EventInsertableBody, Option<DeliverableMetadata>)> {
        let insertable = EventInsertableBody::try_from_carfile(event_cid, carfile).await?;
        let ev_block = insertable.block_for_cid(&insertable.cid())?;

        let event_ipld: unvalidated::RawEvent<Ipld> =
            serde_ipld_dagcbor::from_slice(&ev_block.bytes).map_err(|e| {
                Error::new_invalid_arg(
                    anyhow::anyhow!(e).context("event block is not valid event format"),
                )
            })?;

        let meta = match event_ipld {
            unvalidated::RawEvent::Time(t) => Some(DeliverableMetadata {
                stream_cid: t.id(),
                prev: t.prev(),
            }),
            unvalidated::RawEvent::Signed(signed) => {
                let link = signed.link().ok_or_else(|| {
                    Error::new_invalid_arg(anyhow::anyhow!("event should have a link"))
                })?;
                let link = insertable.block_for_cid(&link).map_err(|e| {
                    Error::new_invalid_arg(
                        anyhow::anyhow!(e).context("prev CID missing from carfile"),
                    )
                })?;
                let payload: unvalidated::Payload<Ipld> =
                    serde_ipld_dagcbor::from_slice(&link.bytes).map_err(|e| {
                        Error::new_invalid_arg(
                            anyhow::anyhow!(e).context("Failed to follow event link"),
                        )
                    })?;

                match payload {
                    unvalidated::Payload::Data(d) => Some(DeliverableMetadata {
                        stream_cid: *d.id(),
                        prev: *d.prev(),
                    }),
                    unvalidated::Payload::Init(_init) => None,
                }
            }
            unvalidated::RawEvent::Unsigned(_init) => None,
        };
        Ok((insertable, meta))
    }

    #[tracing::instrument(skip(self, items), level = tracing::Level::DEBUG, fields(items = items.len()))]
    /// This function is used to insert events from a carfile requiring that the history is local to the node.
    /// This is likely used in API contexts when a user is trying to insert events. Events discovered from
    /// peers can come in any order and we will discover the prev chain over time. Use
    /// `insert_events_from_carfiles_remote_history` for that case.
    pub(crate) async fn insert_events_from_carfiles_local_history<'a>(
        &self,
        items: &[recon::ReconItem<'a, EventId>],
    ) -> Result<recon::InsertResult> {
        if items.is_empty() {
            return Ok(InsertResult::default());
        }

        let mut processor = InsertEventProcessor::default();
        let mut insert_after_history_check: Vec<DiscoveredEvent> = Vec::with_capacity(items.len());
        let mut init_events = Vec::with_capacity(items.len());
        let mut insertable_candidates = Vec::with_capacity(items.len());

        for item in items {
            let (mut insertable, maybe_prev) = processor.parse_item(item).await?;
            if let Some(meta) = maybe_prev {
                let cid = insertable.cid();
                insertable_candidates.push((insertable, meta.stream_cid));
                insert_after_history_check.push(DiscoveredEvent::new(cid, meta));
            } else {
                insertable.set_deliverable(true);
                init_events.push(insertable);
            }
        }

        // This is a bit awkward as we store some events and can still fail. Is it better to accept a partial write or none?
        // We do this first, as the OrderingState struct currently has no concept of and event without a prev, and just assumes it should
        // order everything into a chain and find missing events, and if the init event can't be found nothing should be done.
        let mut res = if !init_events.is_empty() {
            CeramicOneEvent::insert_many(&self.pool, &init_events[..]).await?
        } else {
            InsertResult::default()
        };

        if !insert_after_history_check.is_empty() {
            trace!(local_events_checking=%insert_after_history_check.len(), "checking local history");

            let deliverable =
                OrderingState::verify_all_deliverable(&self.pool, insert_after_history_check)
                    .await?;

            for ev in deliverable {
                if let Some((to_add, stream_cid)) =
                    insertable_candidates.iter().find(|c| c.0.cid() == ev)
                {
                    processor.mark_event_deliverable_now(to_add.to_owned(), *stream_cid);
                } else {
                    info!(cid=%ev, "Failed to find required event CID to mark deliverable");
                    return Err(Error::new_app(anyhow::anyhow!("Failed to find required CID to mark deliverable after verifying local history. Should be unreachable.")));
                }
            }
            let new = CeramicOneEvent::insert_many(&self.pool, &processor.insert_now[..]).await?;
            res.keys.extend(new.keys);
        }
        Ok(res)
    }

    #[tracing::instrument(skip(self, items), level = tracing::Level::DEBUG, fields(items = items.len()))]
    /// This function is used to insert events from a carfile WITHOUT requiring that the history is local to the node.
    /// This is used in recon contexts when we are discovering events from peers in a recon but not ceramic order and
    /// don't have the complete order. To enforce that the history is local, e.g. in API contexts, use
    /// `insert_events_from_carfiles_local_history`.
    pub(crate) async fn insert_events_from_carfiles_remote_history<'a>(
        &self,
        items: &[recon::ReconItem<'a, EventId>],
    ) -> Result<recon::InsertResult> {
        if items.is_empty() {
            return Ok(InsertResult::default());
        }

        let mut processor = InsertEventProcessor::default();

        for item in items {
            let (insertable, maybe_prev) = processor.parse_item(item).await?;
            if let Some(meta) = maybe_prev {
                processor.mark_event_deliverable_later(insertable, meta);
            } else {
                let stream_cid = insertable.cid();
                processor.mark_event_deliverable_now(insertable, stream_cid);
            }
        }
        let res = CeramicOneEvent::insert_many(&self.pool, &processor.insert_now[..]).await?;

        self.notify_ordering_task(processor).await?;
        Ok(res)
    }

    async fn notify_ordering_task(&self, ordering: InsertEventProcessor) -> Result<()> {
        for ev in ordering.background_task_deliverable {
            trace!(cid=%ev.0, prev=%ev.1.prev, init=%ev.1.stream_cid, "sending to delivery task");
            if let Err(e) = self
                .delivery_task
                .tx
                .try_send(DiscoveredEvent::new(ev.0, ev.1))
            {
                match e {
                    tokio::sync::mpsc::error::TrySendError::Full(e) => {
                        // we should only be doing this during recon, in which case we can rediscover events.
                        // the delivery task will start picking up these events once it's drained since they are stored in the db
                        warn!(event=?e, limit=%PENDING_EVENTS_CHANNEL_DEPTH, "Delivery task full. Dropping event and will not be able to mark deliverable until queue drains");
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        warn!("Delivery task closed. shutting down");
                        return Err(Error::new_fatal(anyhow::anyhow!("Delivery task closed")));
                    }
                }
            }
        }
        for new in ordering.notify_task_new {
            trace!(event=?new, "notifying delivery task of inserted event");
            if let Err(e) = self.delivery_task.tx_new.try_send(new) {
                match e {
                    tokio::sync::mpsc::error::TrySendError::Full(ev) => {
                        // we should only be doing this during recon, in which case we can rediscover events.
                        // the delivery task will start picking up these events once it's drained since they are stored in the db
                        warn!(attempt=?ev, limit=%PENDING_EVENTS_CHANNEL_DEPTH, "Notify new task full");
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        warn!("Delivery task closed. shutting down");
                        return Err(Error::new_fatal(anyhow::anyhow!("Delivery task closed")));
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct InsertEventProcessor {
    insert_now: Vec<EventInsertable>,
    notify_task_new: Vec<DeliveredEvent>,
    background_task_deliverable: HashMap<cid::Cid, DeliverableMetadata>,
}

impl InsertEventProcessor {
    async fn parse_item<'a>(
        &self,
        item: &ReconItem<'a, EventId>,
    ) -> Result<(EventInsertable, Option<DeliverableMetadata>)> {
        let cid = item.key.cid().ok_or_else(|| {
            Error::new_invalid_arg(anyhow::anyhow!("EventID is missing a CID: {}", item.key))
        })?;
        // we want to end a conversation if any of the events aren't ceramic events and not store them
        // this includes making sure the key matched the body cid
        let (insertable_body, maybe_prev) =
            CeramicEventService::parse_event_carfile(cid, item.value).await?;
        let insertable = EventInsertable::try_new(item.key.to_owned(), insertable_body)?;
        Ok((insertable, maybe_prev))
    }

    fn mark_event_deliverable_later(
        &mut self,
        insertable: EventInsertable,
        meta: DeliverableMetadata,
    ) {
        self.background_task_deliverable
            .insert(insertable.cid(), meta);
        self.insert_now.push(insertable);
    }

    fn mark_event_deliverable_now(&mut self, mut ev: EventInsertable, init_cid: Cid) {
        ev.set_deliverable(true);
        self.notify_task_new
            .push(DeliveredEvent::new(ev.cid(), init_cid));
        self.insert_now.push(ev);
    }
}
