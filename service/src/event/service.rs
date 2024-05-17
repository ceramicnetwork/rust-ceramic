use std::collections::{HashMap, HashSet};

use ceramic_core::EventId;
use ceramic_event::unvalidated;
use ceramic_store::{CeramicOneEvent, EventInsertable, EventInsertableBody, SqlitePool};
use cid::Cid;
use ipld_core::ipld::Ipld;
use recon::{InsertResult, ReconItem};
use tracing::{trace, warn};

use super::ordering_task::{
    DeliverableEvent, DeliverableMetadata, DeliverableTask, DeliveredEvent, OrderingState,
    OrderingTask, StreamEvents,
};

use crate::{Error, Result};

/// The max number of events we can have pending for delivery in the channel before we start dropping them.
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
    #[allow(dead_code)] // used in tests
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
        let ev_block = insertable.block_for_cid(&insertable.cid)?;

        trace!(count=%insertable.blocks.len(), cid=%event_cid, "parsing event blocks");
        let event_ipld: unvalidated::RawEvent<Ipld> =
            serde_ipld_dagcbor::from_slice(&ev_block.bytes).map_err(|e| {
                Error::new_invalid_arg(
                    anyhow::anyhow!(e).context("event block is not valid event format"),
                )
            })?;

        let maybe_init_prev = match event_ipld {
            unvalidated::RawEvent::Time(t) => Some((t.id(), t.prev())),
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
                    unvalidated::Payload::Data(d) => Some((*d.id(), *d.prev())),
                    unvalidated::Payload::Init(_init) => None,
                }
            }
            unvalidated::RawEvent::Unsigned(_init) => None,
        };
        let meta = maybe_init_prev.map(|(cid, prev)| DeliverableMetadata {
            init_cid: cid,
            prev,
        });
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

        let ordering =
            InsertEventOrdering::discover_deliverable_local_history(items, &self.pool).await?;
        self.process_events(ordering).await
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

        let ordering = InsertEventOrdering::discover_deliverable_remote_history(items).await?;
        self.process_events(ordering).await
    }

    async fn process_events(&self, ordering: InsertEventOrdering) -> Result<recon::InsertResult> {
        let res = CeramicOneEvent::insert_many(&self.pool, &ordering.insert_now[..]).await?;

        for ev in ordering.background_task_deliverable {
            trace!(cid=%ev.0, prev=%ev.1.prev, init=%ev.1.init_cid, "sending to delivery task");
            if let Err(e) = self
                .delivery_task
                .tx
                .try_send(DeliverableEvent::new(ev.0, ev.1, None))
            {
                match e {
                    tokio::sync::mpsc::error::TrySendError::Full(e) => {
                        // we should only be doing this during recon, in which case we can rediscover events.
                        // the delivery task will start picking up these events once it's drained since they are stored in the db
                        warn!(cid=%e.cid, meta=?e.meta, limit=%PENDING_EVENTS_CHANNEL_DEPTH, "Delivery task full. Dropping event and will not be able to mark deliverable until queue drains");
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        warn!("Delivery task closed. shutting down");
                        return Err(Error::new_fatal(anyhow::anyhow!("Delivery task closed")));
                    }
                }
            }
        }
        for new in ordering.notify_task_new {
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
        Ok(res)
    }
}

struct InsertEventOrdering {
    insert_now: Vec<EventInsertable>,
    notify_task_new: Vec<DeliveredEvent>,
    background_task_deliverable: HashMap<cid::Cid, DeliverableMetadata>,
}

impl InsertEventOrdering {
    /// This will mark events as deliverable if their prev exists locally. Otherwise they will be
    /// sorted into the bucket for the background task to process.
    pub(crate) async fn discover_deliverable_remote_history<'a>(
        items: &[ReconItem<'a, EventId>],
    ) -> Result<Self> {
        let mut result = Self {
            insert_now: Vec::with_capacity(items.len()),
            notify_task_new: Vec::with_capacity(items.len()),
            background_task_deliverable: HashMap::new(),
        };

        for item in items {
            let (insertable, maybe_prev) = Self::parse_item(item).await?;
            if let Some(meta) = maybe_prev {
                result.mark_event_deliverable_later(insertable, meta);
            } else {
                let init_cid = insertable.body.cid;
                result.mark_event_deliverable_now(insertable, init_cid);
            }
        }

        Ok(result)
    }

    /// This will error if any of the events doesn't have its prev on the local node (in the database/memory or in this batch).
    pub(crate) async fn discover_deliverable_local_history<'a>(
        items: &[ReconItem<'a, EventId>],
        pool: &SqlitePool,
    ) -> Result<Self> {
        let mut result = Self {
            insert_now: Vec::with_capacity(items.len()),
            notify_task_new: Vec::with_capacity(items.len()),
            background_task_deliverable: HashMap::new(),
        };

        let mut insert_after_history_check: Vec<(DeliverableMetadata, EventInsertable)> =
            Vec::with_capacity(items.len());

        for item in items {
            let (insertable, maybe_prev) = Self::parse_item(item).await?;
            if let Some(meta) = maybe_prev {
                insert_after_history_check.push((meta, insertable));
            } else {
                let init_cid = insertable.body.cid;
                result.mark_event_deliverable_now(insertable, init_cid);
            }
        }

        trace!(local_events_checking=%insert_after_history_check.len(), "checking local history");
        result
            .verify_history_inline(pool, insert_after_history_check)
            .await?;
        Ok(result)
    }

    async fn parse_item<'a>(
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
            .insert(insertable.body.cid, meta);
        self.insert_now.push(insertable);
    }

    fn mark_event_deliverable_now(&mut self, mut ev: EventInsertable, init_cid: Cid) {
        ev.deliverable(true);
        self.notify_task_new
            .push(DeliveredEvent::new(ev.body.cid, init_cid));
        self.insert_now.push(ev);
    }

    async fn verify_history_inline(
        &mut self,
        pool: &SqlitePool,
        to_check: Vec<(DeliverableMetadata, EventInsertable)>,
    ) -> Result<()> {
        if to_check.is_empty() {
            return Ok(());
        }

        let incoming_deliverable_cids: HashSet<Cid> = self
            .insert_now
            .iter()
            .filter_map(|e| {
                if e.body.deliverable {
                    Some(e.body.cid)
                } else {
                    None
                }
            })
            .collect();

        // ideally, this map would be per stream, but we are just processing all of them together for now
        let mut to_check_map = StreamEvents::new();

        let required_to_find = to_check.len();
        let mut found_in_batch = 0;
        let mut insert_if_greenlit = HashMap::with_capacity(required_to_find);

        for (meta, ev) in to_check {
            if incoming_deliverable_cids.contains(&meta.prev) {
                trace!(new=%ev.body.cid, prev=%meta.prev, "prev event being added in same batch");
                found_in_batch += 1;
                self.mark_event_deliverable_now(ev, meta.init_cid);
            } else {
                trace!(new=%ev.body.cid, prev=%meta.prev, "will check for prev event in db");

                let _new = to_check_map.add_event(DeliverableEvent::new(
                    ev.body.cid,
                    meta.to_owned(),
                    None,
                ));
                insert_if_greenlit.insert(ev.body.cid, (meta, ev));
            }
        }

        if to_check_map.is_empty() {
            return Ok(());
        }

        let deliverable =
            OrderingState::discover_deliverable_events(pool, &mut to_check_map).await?;
        if deliverable.len() != required_to_find - found_in_batch {
            let missing = insert_if_greenlit
                .values()
                .filter_map(|(_, ev)| {
                    if !deliverable.contains(&ev.body.cid) {
                        Some(ev.body.cid)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            tracing::info!(?missing, ?deliverable, "Missing required `prev` event CIDs");

            Err(Error::new_invalid_arg(anyhow::anyhow!(
                "Missing required `prev` event CIDs: {:?}",
                missing
            )))
        } else {
            // we need to use the deliverable list's order because we might depend on something in the same batch, and that function will
            // ensure we have a queue in the correct order. So we follow the order and use our insert_if_greenlit map to get the details.
            for cid in deliverable {
                if let Some((meta, insertable)) = insert_if_greenlit.remove(&cid) {
                    self.mark_event_deliverable_now(insertable, meta.init_cid);
                } else {
                    warn!(%cid, "Didn't find event to insert in memory when it was expected");
                }
            }
            Ok(())
        }
    }
}
