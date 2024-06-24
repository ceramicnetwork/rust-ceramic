use async_trait::async_trait;
use ceramic_core::{EventId, Network};
use ceramic_event::unvalidated;
use ceramic_store::{CeramicOneEvent, EventInsertable, EventInsertableBody, SqlitePool};
use cid::Cid;
use futures::Stream;
use ipld_core::ipld::Ipld;
use tracing::{trace, warn};

use super::{
    migration::Migrator,
    order_events::OrderEvents,
    ordering_task::{DeliverableTask, OrderingTask},
};

use crate::{Error, Result};

/// How many events to select at once to see if they've become deliverable when we have downtime
/// Used at startup and occassionally in case we ever dropped something
/// We keep the number small for now as we may need to traverse many prevs for each one of these and load them into memory.
const DELIVERABLE_EVENTS_BATCH_SIZE: u32 = 1000;

/// How many batches of undelivered events are we willing to process on start up?
/// To avoid an infinite loop. It's going to take a long time to process `DELIVERABLE_EVENTS_BATCH_SIZE * MAX_ITERATIONS` events
const MAX_ITERATIONS: usize = 100_000_000;

/// The max number of events we can have pending for delivery in the channel before we start dropping them.
/// This is currently 304 bytes per event, so this is 3 MB of data
const PENDING_EVENTS_CHANNEL_DEPTH: usize = 1_000_000;

#[derive(Debug)]
/// A database store that verifies the bytes it stores are valid Ceramic events.
/// Implements the [`recon::Store`], [`iroh_bitswap::Store`], and [`ceramic_api::EventStore`] traits for [`ceramic_core::EventId`].
pub struct CeramicEventService {
    pub(crate) pool: SqlitePool,
    delivery_task: DeliverableTask,
}
/// An object that represents an IPFS block where the data can be loaded async.
#[async_trait]
pub trait Block {
    /// Report the CID of the block.
    fn cid(&self) -> Cid;
    /// Asynchronously load the block data.
    /// This data should not be cached in memory as block data is accessed randomly.
    async fn data(&self) -> anyhow::Result<Vec<u8>>;
}
pub type BoxedBlock = Box<dyn Block>;

impl CeramicEventService {
    /// Create a new CeramicEventStore
    pub async fn new(pool: SqlitePool) -> Result<Self> {
        CeramicOneEvent::init_delivered_order(&pool).await?;

        let _updated = OrderingTask::process_all_undelivered_events(
            &pool,
            MAX_ITERATIONS,
            DELIVERABLE_EVENTS_BATCH_SIZE,
        )
        .await?;

        let delivery_task = OrderingTask::run(pool.clone(), PENDING_EVENTS_CHANNEL_DEPTH).await;

        Ok(Self {
            pool,
            delivery_task,
        })
    }

    /// Skip loading all undelivered events from the database on startup (for testing)
    #[cfg(test)]
    pub(crate) async fn new_without_undelivered(pool: SqlitePool) -> Result<Self> {
        CeramicOneEvent::init_delivered_order(&pool).await?;

        let delivery_task = OrderingTask::run(pool.clone(), PENDING_EVENTS_CHANNEL_DEPTH).await;

        Ok(Self {
            pool,
            delivery_task,
        })
    }
    pub async fn migrate_from_ipfs(
        &self,
        network: Network,
        blocks: impl Stream<Item = anyhow::Result<BoxedBlock>>,
    ) -> Result<()> {
        let migrator = Migrator::new(network, blocks)
            .await
            .map_err(Error::new_fatal)?;
        migrator
            .migrate(&self.pool)
            .await
            .map_err(Error::new_fatal)?;
        Ok(())
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

    /// Currently only verifies that the event parses into a valid ceramic event, determining whether it's
    /// immediately deliverable because it's an init event or it needs review.
    /// In the future, we will need to do more event validation (verify all EventID pieces, hashes, signatures, etc).
    pub(crate) async fn validate_discovered_event(
        event_id: ceramic_core::EventId,
        carfile: &[u8],
    ) -> Result<(EventInsertable, EventMetadata)> {
        let event_cid = event_id.cid().ok_or_else(|| {
            Error::new_app(anyhow::anyhow!("EventId missing CID. EventID={}", event_id))
        })?;

        let (cid, parsed_event) = unvalidated::Event::<Ipld>::decode_car(carfile, false)
            .await
            .map_err(Error::new_app)?;

        if event_cid != cid {
            return Err(Error::new_app(anyhow::anyhow!(
                "EventId CID ({}) does not match the body CID ({})",
                event_cid,
                cid
            )));
        }

        let metadata = EventMetadata::from(parsed_event);
        let mut body = EventInsertableBody::try_from_carfile(cid, carfile).await?;
        body.set_deliverable(matches!(metadata, EventMetadata::Init { .. }));

        Ok((EventInsertable::try_new(event_id, body)?, metadata))
    }

    #[tracing::instrument(skip(self, items), level = tracing::Level::DEBUG, fields(items = items.len()))]
    /// This function is used to insert events from a carfile requiring that the history is local to the node.
    /// This is likely used in API contexts when a user is trying to insert events. Events discovered from
    /// peers can come in any order and we will discover the prev chain over time. Use
    /// `insert_events_from_carfiles_remote_history` for that case.
    pub(crate) async fn insert_events_from_carfiles_local_api<'a>(
        &self,
        items: &[recon::ReconItem<'a, EventId>],
    ) -> Result<InsertResult> {
        self.insert_events(items, true).await
    }

    #[tracing::instrument(skip(self, items), level = tracing::Level::DEBUG, fields(items = items.len()))]
    /// This function is used to insert events from a carfile WITHOUT requiring that the history is local to the node.
    /// This is used in recon contexts when we are discovering events from peers in a recon but not ceramic order and
    /// don't have the complete order. To enforce that the history is local, e.g. in API contexts, use
    /// `insert_events_from_carfiles_local_history`.
    pub(crate) async fn insert_events_from_carfiles_recon<'a>(
        &self,
        items: &[recon::ReconItem<'a, EventId>],
    ) -> Result<recon::InsertResult> {
        let res = self.insert_events(items, false).await?;
        let mut keys = vec![false; items.len()];
        // we need to put things back in the right order that the recon trait expects, even though we don't really care about the result
        for (i, item) in items.iter().enumerate() {
            let new_key = res
                .store_result
                .inserted
                .iter()
                .find(|e| e.order_key == *item.key)
                .map_or(false, |e| e.new_key); // TODO: should we error if it's not in this set
            keys[i] = new_key;
        }
        Ok(recon::InsertResult::new(keys))
    }

    async fn insert_events<'a>(
        &self,
        items: &[recon::ReconItem<'a, EventId>],
        history_required: bool,
    ) -> Result<InsertResult> {
        if items.is_empty() {
            return Ok(InsertResult::default());
        }

        let mut to_insert = Vec::with_capacity(items.len());

        for event in items {
            let insertable =
                Self::validate_discovered_event(event.key.to_owned(), event.value).await?;
            to_insert.push(insertable);
        }

        let ordered = OrderEvents::try_new(&self.pool, to_insert).await?;

        let missing_history = ordered
            .missing_history
            .iter()
            .map(|(e, _)| e.order_key.clone())
            .collect();

        let to_insert_with_metadata = if history_required {
            ordered.deliverable
        } else {
            ordered
                .deliverable
                .into_iter()
                .chain(ordered.missing_history)
                .collect()
        };

        let to_insert = to_insert_with_metadata
            .iter()
            .map(|(e, _)| e.clone())
            .collect::<Vec<_>>();

        let res = CeramicOneEvent::insert_many(&self.pool, &to_insert[..]).await?;

        // api writes shouldn't have any missed pieces that need ordering so we don't send those and return early
        if history_required {
            return Ok(InsertResult {
                store_result: res,
                missing_history,
            });
        }

        let to_send = res
            .inserted
            .iter()
            .filter(|i| i.new_key)
            .collect::<Vec<_>>();

        for ev in to_send {
            if let Some((ev, metadata)) = to_insert_with_metadata
                .iter()
                .find(|(i, _)| i.order_key == ev.order_key)
            {
                let discovered = DiscoveredEvent {
                    cid: ev.cid(),
                    known_deliverable: ev.deliverable(),
                    metadata: metadata.to_owned(),
                };
                trace!(?discovered, "sending delivered to ordering task");
                if let Err(_e) = self.delivery_task.tx_inserted.send(discovered).await {
                    warn!("Delivery task closed. shutting down");
                    return Err(Error::new_fatal(anyhow::anyhow!("Delivery task closed")));
                }
            } else {
                tracing::error!(event_id=%ev.order_key, "Missing header for inserted event should be unreachable!");
                debug_assert!(false); // panic in debug mode
                continue;
            }
        }

        Ok(InsertResult {
            store_result: res,
            missing_history,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Default)]
pub struct InsertResult {
    pub(crate) store_result: ceramic_store::InsertResult,
    pub(crate) missing_history: Vec<EventId>,
}

impl From<InsertResult> for Vec<ceramic_api::EventInsertResult> {
    fn from(res: InsertResult) -> Self {
        let mut api_res =
            Vec::with_capacity(res.store_result.inserted.len() + res.missing_history.len());
        for ev in res.store_result.inserted {
            api_res.push(ceramic_api::EventInsertResult::new_ok(ev.order_key));
        }
        for ev in res.missing_history {
            api_res.push(ceramic_api::EventInsertResult::new_failed(
                ev,
                "Failed to insert event as `prev` event was missing".to_owned(),
            ));
        }
        api_res
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredEvent {
    pub cid: ceramic_core::Cid,
    pub known_deliverable: bool,
    pub metadata: EventMetadata,
}

impl DiscoveredEvent {
    pub(crate) fn stream_cid(&self) -> ceramic_core::Cid {
        match self.metadata {
            EventMetadata::Init => self.cid,
            EventMetadata::Data { stream_cid, .. } | EventMetadata::Time { stream_cid, .. } => {
                stream_cid
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// An event header wrapper for use in the store crate.
/// TODO: replace this with something from the event crate
pub(crate) enum EventMetadata {
    /// The init CID and stream CID are the same
    Init,
    Data {
        stream_cid: ceramic_core::Cid,
        prev: ceramic_core::Cid,
    },
    Time {
        stream_cid: ceramic_core::Cid,
        prev: ceramic_core::Cid,
    },
}

impl From<unvalidated::Event<Ipld>> for EventMetadata {
    fn from(value: unvalidated::Event<Ipld>) -> Self {
        match value {
            unvalidated::Event::Time(t) => EventMetadata::Time {
                stream_cid: t.id(),
                prev: t.prev(),
            },

            unvalidated::Event::Signed(signed) => match signed.payload() {
                unvalidated::Payload::Data(d) => EventMetadata::Data {
                    stream_cid: *d.id(),
                    prev: *d.prev(),
                },
                unvalidated::Payload::Init(_init) => EventMetadata::Init,
            },
            unvalidated::Event::Unsigned(_init) => EventMetadata::Init,
        }
    }
}

impl EventMetadata {
    pub(crate) fn prev(&self) -> Option<ceramic_core::Cid> {
        match self {
            EventMetadata::Init { .. } => None,
            EventMetadata::Data { prev, .. } | EventMetadata::Time { prev, .. } => Some(*prev),
        }
    }
}
