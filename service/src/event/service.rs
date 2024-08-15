use anyhow::anyhow;
use std::collections::HashSet;

use async_trait::async_trait;
use cid::Cid;
use futures::stream::BoxStream;
use ipld_core::ipld::Ipld;
use recon::ReconItem;
use tracing::{trace, warn};

use ceramic_anchor_service::AnchorClient;
use ceramic_core::{EventId, Network};
use ceramic_event::{
    anchor::{AnchorRequest, TimeEventBatch},
    unvalidated::{self, EventMetadata},
};
use ceramic_store::{CeramicOneEvent, EventInsertable, EventInsertableBody, SqlitePool};

use super::{
    migration::Migrator,
    order_events::OrderEvents,
    ordering_task::{DeliverableTask, OrderingTask},
};

use crate::{Error, Result};

/// How many events to select at once to see if they've become deliverable when we have downtime
/// Used at startup and occasionally in case we ever dropped something
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
    node_did: Option<String>,
    delivery_task: DeliverableTask,
}
/// An object that represents a set of blocks that can produce a stream of all blocks and lookup a
/// block based on CID.
#[async_trait]
pub trait BlockStore {
    /// Produce a stream of all blocks in the store
    fn blocks(&self) -> BoxStream<'static, anyhow::Result<(Cid, Vec<u8>)>>;
    /// Asynchronously load the block data.
    /// This data should not be cached in memory as block data is accessed randomly.
    async fn block_data(&self, cid: &Cid) -> anyhow::Result<Option<Vec<u8>>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliverableRequirement {
    /// Must be ordered immediately and is rejected if not currently deliverable. The appropriate setting
    /// for API writes as we cannot create an event without its history.
    Immediate,
    /// This will be ordered as soon as its dependencies are discovered. Can be written in the meantime
    /// and will consume memory tracking the event until it can be ordered. The appropriate setting for recon
    /// discovered events.
    Asap(String),
    /// This currently means the event will be ordered on next system startup. An appropriate setting while
    /// migrating data from an IPFS datastore.
    Lazy,
}

impl CeramicEventService {
    /// Create a new CeramicEventStore
    pub async fn new(pool: SqlitePool, node_did: Option<String>) -> Result<Self> {
        CeramicOneEvent::init_delivered_order(&pool).await?;

        let delivery_task = OrderingTask::run(pool.clone(), PENDING_EVENTS_CHANNEL_DEPTH).await;

        Ok(Self {
            pool,
            node_did,
            delivery_task,
        })
    }

    /// Returns the number of undelivered events that were updated
    pub async fn process_all_undelivered_events(&self) -> Result<usize> {
        OrderingTask::process_all_undelivered_events(
            &self.pool,
            MAX_ITERATIONS,
            DELIVERABLE_EVENTS_BATCH_SIZE,
        )
        .await
    }

    pub async fn migrate_from_ipfs(&self, network: Network, blocks: impl BlockStore) -> Result<()> {
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
    pub(crate) async fn parse_discovered_event(
        item: &ReconItem<EventId>,
        source: Option<String>,
    ) -> Result<(EventInsertable, EventMetadata)> {
        let event_cid = item.key.cid().ok_or_else(|| {
            Error::new_app(anyhow::anyhow!("EventId missing CID. EventID={}", item.key))
        })?;

        let (cid, parsed_event) =
            unvalidated::Event::<Ipld>::decode_car(item.value.as_slice(), false)
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
        let body = EventInsertableBody::try_from_carfile(
            metadata.stream_cid(),
            cid,
            item.value.as_slice(),
            source,
        )
        .await?;

        Ok((
            EventInsertable::try_new(item.key.to_owned(), body)?,
            metadata,
        ))
    }

    pub(crate) async fn insert_events(
        &self,
        items: &[ReconItem<EventId>],
        source: DeliverableRequirement,
    ) -> Result<InsertResult> {
        let source_node_did = match &source {
            DeliverableRequirement::Immediate | DeliverableRequirement::Lazy => &self.node_did,
            DeliverableRequirement::Asap(node_did) => &Some(node_did.to_owned()),
        };
        let mut to_insert = Vec::new();
        let mut invalid = Vec::new();
        for event in items {
            match Self::parse_discovered_event(event, source_node_did.clone()).await {
                Ok(insertable) => to_insert.push(insertable),
                Err(err) => invalid.push(InvalidItem::InvalidFormat {
                    key: event.key.clone(),
                    reason: err.to_string(),
                }),
            }
        }

        let ordered = OrderEvents::try_new(&self.pool, to_insert).await?;

        // api writes shouldn't have any missed history so we don't insert those events and
        // we can skip notifying the ordering task because it's impossible to be waiting on them
        let store_result = match source {
            DeliverableRequirement::Immediate => {
                let to_insert = ordered.deliverable().iter().map(|(e, _)| e);
                invalid.extend(ordered.missing_history().iter().map(|(e, _)| {
                    InvalidItem::RequiresHistory {
                        key: e.order_key.clone(),
                    }
                }));
                CeramicOneEvent::insert_many(&self.pool, to_insert).await?
            }
            DeliverableRequirement::Lazy | DeliverableRequirement::Asap(_) => {
                let to_insert = ordered
                    .deliverable()
                    .iter()
                    .map(|(e, _)| e)
                    .chain(ordered.missing_history().iter().map(|(e, _)| e));

                let store_result = CeramicOneEvent::insert_many(&self.pool, to_insert).await?;

                if matches!(source, DeliverableRequirement::Asap(_)) {
                    self.notify_ordering_task(&ordered, &store_result).await?;
                }

                store_result
            }
        };

        Ok(InsertResult {
            store_result,
            rejected: invalid,
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
            if new.contains(&ev.body.cid) {
                self.send_discovered_event(DiscoveredEvent {
                    cid: ev.body.cid,
                    known_deliverable: ev.body.deliverable,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidItem {
    InvalidFormat {
        key: EventId,
        reason: String,
    },
    #[allow(dead_code)]
    InvalidSignature {
        key: EventId,
        reason: String,
    },
    /// For recon, this is any event where we haven't found the init event
    /// For API, this is anything where we don't have prev locally
    RequiresHistory {
        key: EventId,
    },
}

#[async_trait]
impl AnchorClient for CeramicEventService {
    async fn get_anchor_requests(&self) -> anyhow::Result<Vec<AnchorRequest>> {
        // We must have a node DID to use here. Return an error if we don't.
        let node_did = self
            .node_did
            .clone()
            .ok_or_else(|| Error::new_app(anyhow!("node DID required to get anchor requests")))?;

        // Fetch event CIDs from the events table that have a source == our node DID and are still unanchored
        Ok(
            CeramicOneEvent::unanchored_events_by_source(&self.pool, node_did, 10000)
                .await
                .map_err(|e| Error::new_app(anyhow!("could not fetch unanchored events: {}", e)))?,
        )
    }

    async fn put_time_events(&self, batch: TimeEventBatch) -> anyhow::Result<()> {
        warn!("time events: {:?}", batch);
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Default)]
pub struct InsertResult {
    pub rejected: Vec<InvalidItem>,
    pub(crate) store_result: ceramic_store::InsertResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredEvent {
    pub cid: Cid,
    pub known_deliverable: bool,
    pub metadata: EventMetadata,
}
