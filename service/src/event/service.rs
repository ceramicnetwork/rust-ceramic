use std::collections::HashSet;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliverableRequirement {
    /// Must be ordered immediately and is rejected if not currently deliverable. The appropriate setting
    /// for API writes as we cannot create an event without its history.
    Immediate,
    /// This will be ordered as soon as its dependencies are discovered. Can be written in the meantime
    /// and will consume memory tracking the event until it can be ordered. The approprate setting for recon
    /// discovered events.
    Asap,
    /// This currently means the event will be ordered on next system startup. An appropriate setting while
    /// migrating data from an IPFS datastore.
    Lazy,
}

impl CeramicEventService {
    /// Create a new CeramicEventStore
    pub async fn new(pool: SqlitePool, process_undelivered: bool) -> Result<Self> {
        CeramicOneEvent::init_delivered_order(&pool).await?;

        if process_undelivered {
            let _updated = OrderingTask::process_all_undelivered_events(
                &pool,
                MAX_ITERATIONS,
                DELIVERABLE_EVENTS_BATCH_SIZE,
            )
            .await?;
        }

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
        let migrator = Migrator::new(self, network, blocks)
            .await
            .map_err(Error::new_fatal)?;
        migrator.migrate().await.map_err(Error::new_fatal)?;
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

    /// Currently only verifies that the event parses into a valid ceramic event.
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
        let body = EventInsertableBody::try_from_carfile(cid, carfile).await?;

        Ok((EventInsertable::try_new(event_id, body)?, metadata))
    }

    pub(crate) async fn insert_events<'a>(
        &self,
        items: &[recon::ReconItem<'a, EventId>],
        source: DeliverableRequirement,
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
            .missing_history()
            .iter()
            .map(|(e, _)| e.order_key.clone())
            .collect();

        // api writes shouldn't have any missed history so we don't insert those events and
        // we can skip notifying the ordering task because it's impossible to be waiting on them
        let store_result = match source {
            DeliverableRequirement::Immediate => {
                let to_insert = ordered.deliverable().iter().map(|(e, _)| e);
                CeramicOneEvent::insert_many(&self.pool, to_insert).await?
            }
            DeliverableRequirement::Lazy | DeliverableRequirement::Asap => {
                let to_insert = ordered
                    .deliverable()
                    .iter()
                    .map(|(e, _)| e)
                    .chain(ordered.missing_history().iter().map(|(e, _)| e));

                let store_result = CeramicOneEvent::insert_many(&self.pool, to_insert).await?;

                if matches!(source, DeliverableRequirement::Asap) {
                    self.notify_ordering_task(&ordered, &store_result).await?;
                }

                store_result
            }
        };

        Ok(InsertResult {
            store_result,
            missing_history,
        })
    }

    async fn notify_ordering_task(
        &self,
        ordered: &OrderEvents,
        store_result: &ceramic_store::InsertResult,
    ) -> Result<()> {
        let new = store_result
            .inserted
            .iter()
            .filter_map(|i| if i.new_key { i.order_key.cid() } else { None })
            .collect::<HashSet<_>>();

        for (ev, metadata) in ordered
            .deliverable()
            .iter()
            .chain(ordered.missing_history().iter())
        {
            if new.contains(&ev.cid()) {
                self.send_discovered_event(DiscoveredEvent {
                    cid: ev.cid(),
                    known_deliverable: ev.deliverable(),
                    metadata: metadata.to_owned(),
                })
                .await?;
            }
        }

        Ok(())
    }

    async fn send_discovered_event(&self, discovered: DiscoveredEvent) -> Result<()> {
        trace!(?discovered, "sending delivered to ordering task");
        if let Err(_e) = self.delivery_task.tx_inserted.send(discovered).await {
            warn!("Delivery task closed. shutting down");
            Err(Error::new_fatal(anyhow::anyhow!("Delivery task closed")))
        } else {
            Ok(())
        }
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
