use std::collections::HashMap;

use ceramic_core::EventId;
use ceramic_event::unvalidated;
use ceramic_store::{CeramicOneEvent, EventInsertable, EventInsertableBody, SqlitePool};
use ipld_core::ipld::Ipld;
use recon::InsertResult;
use tracing::warn;

use super::ordering_task::{
    DeliverableEvent, DeliverableMetadata, DeliverableTask, OrderingState, OrderingTask,
    StreamEvents,
};

use crate::{Error, Result};

/// The max number of events we can have pending for delivery before we start dropping them
pub(crate) const PENDING_DELIVERABLE_EVENTS: usize = 10000;

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

        let delivery_task = OrderingTask::run(pool.clone(), PENDING_DELIVERABLE_EVENTS).await;

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

        let event_ipld: unvalidated::Event<Ipld> = serde_ipld_dagcbor::from_slice(&ev_block.bytes)
            .map_err(|e| {
                Error::new_invalid_arg(
                    anyhow::anyhow!(e).context("event block is not valid event format"),
                )
            })?;

        let maybe_init_prev = match event_ipld {
            unvalidated::Event::Time(t) => Some((t.id(), t.prev())),
            unvalidated::Event::Signed(signed) => {
                if let Some(prev) = signed.link() {
                    let link = insertable.block_for_cid(&prev).map_err(|e| {
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
                        unvalidated::Payload::Data(d) => {
                            Some((d.id().to_owned(), d.prev().to_owned()))
                        }
                        unvalidated::Payload::Init(_init) => None,
                    }
                } else {
                    None
                }
            }
            unvalidated::Event::Unsigned(_init) => None,
        };
        let meta = maybe_init_prev.map(|(cid, prev)| DeliverableMetadata {
            init_cid: cid,
            prev,
        });
        Ok((insertable, meta))
    }

    pub(crate) async fn insert_events_from_carfiles<'a>(
        &self,
        items: &[recon::ReconItem<'a, EventId>],
        must_have_history: bool,
    ) -> Result<recon::InsertResult> {
        if items.is_empty() {
            return Ok(InsertResult::default());
        }

        let mut insert_now = Vec::with_capacity(items.len());
        let mut insert_after_history_check = if must_have_history {
            Vec::with_capacity(items.len())
        } else {
            Vec::default()
        };
        let mut check_deliverable_later = HashMap::new();
        for item in items {
            let cid = item.key.cid().ok_or_else(|| {
                Error::new_invalid_arg(anyhow::anyhow!("EventID is missing a CID: {}", item.key))
            })?;
            // we want to end a conversation if any of the events aren't ceramic events and not store them
            // this includes making sure the key matched the body cid
            let (insertable_body, maybe_prev) = Self::parse_event_carfile(cid, item.value).await?;
            let mut insertable = EventInsertable::try_new(item.key.to_owned(), insertable_body)?;
            if let Some(meta) = maybe_prev {
                if must_have_history {
                    insert_after_history_check.push((meta, insertable));
                } else {
                    check_deliverable_later.insert(insertable.body.cid, meta);
                    insert_now.push(insertable);
                }
            } else {
                insertable.body.deliverable = true;
                insert_now.push(insertable);
            }
        }

        if !insert_after_history_check.is_empty() {
            // ideally, this map would be per stream, but we are just processing all of them together for now
            let mut stream_map = StreamEvents::new();

            insert_after_history_check.iter().for_each(|(meta, ev)| {
                let _new =
                    stream_map.add_event(DeliverableEvent::new(ev.body.cid, meta.to_owned(), None));
            });

            let deliverable =
                OrderingState::discover_deliverable_events(&self.pool, &mut stream_map).await?;
            if deliverable.len() != insert_after_history_check.len() {
                let missing = insert_after_history_check
                    .iter()
                    .filter_map(|(_, ev)| {
                        if !deliverable.contains(&ev.body.cid) {
                            Some(ev.body.cid)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                return Err(Error::new_app(anyhow::anyhow!(
                    "Missing required `prev` event CIDs: {:?}",
                    missing
                )));
            } else {
                for (_, mut insertable) in insert_after_history_check {
                    insertable.body.deliverable = true;
                    insert_now.push(insertable);
                }
            }
        }
        let res = CeramicOneEvent::insert_many(&self.pool, &insert_now[..]).await?;

        for ev in check_deliverable_later {
            if let Err(e) = self
                .delivery_task
                .tx
                .try_send(DeliverableEvent::new(ev.0, ev.1, None))
            {
                match e {
                    tokio::sync::mpsc::error::TrySendError::Full(e) => {
                        // we should only be doing this during recon, in which case we can rediscover events.
                        // the delivery task will start picking up these events once it's drained since they are stored in the db
                        warn!(cid=%e.cid, meta=?e.meta, limit=%PENDING_DELIVERABLE_EVENTS, "Delivery task full. Dropping event and will not be able to mark deliverable until queue drains");
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
