use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use super::{
    migration::Migrator,
    order_events::OrderEvents,
    ordering_task::{DeliverableTask, OrderingTask},
    validator::{EventValidator, UnvalidatedEvent, ValidatedEvent, ValidatedEvents},
};
use async_trait::async_trait;
use ceramic_core::{EventId, Network, NodeId, SerializeExt};
use ceramic_flight::{ConclusionData, ConclusionEvent, ConclusionInit, ConclusionTime};
use ceramic_sql::sqlite::SqlitePool;
use cid::Cid;
use futures::stream::BoxStream;
use ipld_core::ipld::Ipld;
use itertools::Itertools;
use recon::ReconItem;
use tracing::{trace, warn};

use crate::event::validator::EthRpcProvider;
use crate::store::{CeramicOneEvent, EventInsertable, EventRowDelivered};
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

/// The max number of events that can be waiting on a previous event before we can validate them
/// that are allowed to live in memory. This is possible during recon conversations where we discover things out of order,
/// but we don't expect the queue to get very deep as we should discover events close together.
/// At 1 KB/event, this would be around 100 MB. If the queue does fill up, the current behavior is to drop the events and
/// they may or may not be discovered in a future conversation.
const PENDING_VALIDATION_QUEUE_DEPTH: usize = 100_000;

#[derive(Debug)]
/// A database store that verifies the bytes it stores are valid Ceramic events.
/// Implements the [`recon::Store`], [`iroh_bitswap::Store`], and [`ceramic_api::EventStore`] traits for [`ceramic_core::EventId`].
pub struct EventService {
    pub(crate) pool: SqlitePool,
    validate_events: bool,
    delivery_task: DeliverableTask,
    event_validator: EventValidator,
    pending_writes: Arc<Mutex<HashMap<Cid, Vec<UnvalidatedEvent>>>>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliverableRequirement {
    /// Must be ordered immediately and is rejected if not currently deliverable. The appropriate setting
    /// for API writes as we cannot create an event without its history.
    Immediate,
    /// This will be ordered as soon as its dependencies are discovered. Can be written in the meantime
    /// and will consume memory tracking the event until it can be ordered. The appropriate setting for recon
    /// discovered events.
    Asap,
    /// This currently means the event will be ordered on next system startup. An appropriate setting while
    /// migrating data from an IPFS datastore.
    Lazy,
}

impl EventService {
    /// Create a new CeramicEventStore.
    ///
    /// When process_undelivered_events is true this blocks until all undelivered events have been
    /// processed.
    pub async fn try_new(
        pool: SqlitePool,
        process_undelivered_events: bool,
        validate_events: bool,
        ethereum_rpc_providers: Vec<EthRpcProvider>,
    ) -> Result<Self> {
        CeramicOneEvent::init_delivered_order(&pool).await?;

        let delivery_task = OrderingTask::run(pool.clone(), PENDING_EVENTS_CHANNEL_DEPTH).await;

        let event_validator = EventValidator::try_new(pool.clone(), ethereum_rpc_providers).await?;

        let svc = Self {
            pool,
            validate_events,
            event_validator,
            delivery_task,
            pending_writes: Arc::new(Mutex::new(HashMap::default())),
        };
        if process_undelivered_events {
            svc.process_all_undelivered_events().await?;
        }
        Ok(svc)
    }

    /// Create a new CeramicEventStore with event validation enabled
    /// This is likely temporary and only used in tests to avoid adding the bool now and then deleting it
    /// in the next pass.. but it's basically same same but different.
    #[allow(dead_code)]
    pub(crate) async fn new_with_event_validation(pool: SqlitePool) -> Result<Self> {
        Self::try_new(pool, false, true, vec![]).await
    }

    /// Currently, we track events when the [`ValidationRequirement`] allows. Right now, this applies to
    /// recon discovered events when the init event isn't yet known to the node due to out of order sync
    /// but might apply to other cases in the future.
    fn track_pending(&self, pending: Vec<UnvalidatedEvent>) {
        let mut map = self.pending_writes.lock().unwrap();
        if map.len() + pending.len() >= PENDING_VALIDATION_QUEUE_DEPTH {
            // We don't free the memory but we drop all the events and start filling things up again.
            // We will discover them again in the future, or we won't which is fine since we didn't
            // know what to do with them anyway. It's possible we drop things an in progress conversation
            // could have found, but again, we'll find them in the future and they should be closer together.
            map.clear();
        }
        for ev in pending {
            match map.entry(*ev.event.stream_cid()) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().push(ev);
                }
                std::collections::hash_map::Entry::Vacant(vacant) => {
                    vacant.insert(vec![ev]);
                }
            }
        }
    }

    /// Given the incoming events, see if any of them are the init event that events were
    /// 'pending on' and return all previously pending events that can now be validated.  
    fn remove_unblocked_from_pending_q(&self, new: &[UnvalidatedEvent]) -> Vec<UnvalidatedEvent> {
        let new_init_cids = new
            .iter()
            .flat_map(|e| if e.event.is_init() { Some(e.cid) } else { None });
        let mut map: MutexGuard<'_, HashMap<Cid, Vec<UnvalidatedEvent>>> =
            self.pending_writes.lock().unwrap();
        let mut to_add = Vec::new(); // no clue on capacity
        for new_cid in new_init_cids {
            if let Some(unblocked) = map.remove(&new_cid) {
                to_add.extend(unblocked)
            }
        }

        to_add
    }

    /// Returns the number of undelivered events that were updated
    async fn process_all_undelivered_events(&self) -> Result<usize> {
        OrderingTask::process_all_undelivered_events(
            &self.pool,
            MAX_ITERATIONS,
            DELIVERABLE_EVENTS_BATCH_SIZE,
        )
        .await
    }

    /// Migrate a collection of blocks into the event service.
    pub async fn migrate_from_ipfs(
        &self,
        network: Network,
        blocks: impl BlockStore,
        log_tile_docs: bool,
    ) -> Result<()> {
        let migrator = Migrator::new(self, network, blocks, log_tile_docs)
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

    async fn validate_events(
        &self,
        items: &[ReconItem<EventId>],
        validation_req: Option<&ValidationRequirement>,
    ) -> Result<ValidatedEvents> {
        let mut parsed_events = Vec::with_capacity(items.len());
        let mut invalid_events = Vec::with_capacity(items.len() / 4); // assume most of the events are valid
        for event in items {
            match UnvalidatedEvent::try_from(event) {
                Ok(insertable) => parsed_events.push(insertable),
                Err(err) => invalid_events.push(ValidationError::InvalidFormat {
                    key: event.key.clone(),
                    reason: err.to_string(),
                }),
            }
        }

        let pending_to_insert = self.remove_unblocked_from_pending_q(&parsed_events);
        let to_validate = parsed_events.into_iter().chain(pending_to_insert).collect();

        // use the requested validation or None if it's disabled
        let validation_requirement = if self.validate_events {
            validation_req
        } else {
            None
        };

        let ValidatedEvents {
            valid,
            unvalidated,
            invalid,
        } = self
            .event_validator
            .validate_events(validation_requirement, to_validate)
            .await?;
        invalid_events.extend(invalid);

        Ok(ValidatedEvents {
            valid,
            unvalidated,
            invalid: invalid_events,
        })
    }

    pub(crate) async fn insert_events(
        &self,
        items: &[ReconItem<EventId>],
        deliverable_req: DeliverableRequirement,
        informant: Option<NodeId>,
        validation_req: Option<ValidationRequirement>,
    ) -> Result<InsertResult> {
        let ValidatedEvents {
            valid,
            unvalidated,
            mut invalid,
        } = self.validate_events(items, validation_req.as_ref()).await?;

        let to_insert: Vec<EventInsertable> = valid
            .into_iter()
            .map(|e| ValidatedEvent::into_insertable(e, informant))
            .collect();

        let pending_count = unvalidated.len();
        // validation should have converted things to errors/unvalidated appropriately
        // so we can always track pending if anything was returned
        if !unvalidated.is_empty() {
            self.track_pending(unvalidated);
        }

        let (new, existed) = self
            .persist_events(to_insert, deliverable_req, &mut invalid)
            .await?;

        Ok(InsertResult::new(new, existed, invalid, pending_count))
    }

    /// Persists events to disk and notifies the background ordering task when appropriate
    /// Returns two vectors of Event IDs representing (new, existed)
    async fn persist_events(
        &self,
        to_insert: Vec<EventInsertable>,
        deliverable_req: DeliverableRequirement,
        invalid: &mut Vec<ValidationError>,
    ) -> Result<(Vec<EventId>, Vec<EventId>)> {
        match deliverable_req {
            DeliverableRequirement::Immediate => {
                let ordered =
                    OrderEvents::find_currently_deliverable(&self.pool, to_insert).await?;
                let to_insert = ordered.deliverable().iter();
                invalid.extend(ordered.missing_history().iter().map(|e| {
                    ValidationError::RequiresHistory {
                        key: e.order_key().clone(),
                    }
                }));
                let store_result = CeramicOneEvent::insert_many(&self.pool, to_insert).await?;
                Ok(Self::partition_store_result(store_result))
            }
            DeliverableRequirement::Asap => {
                let ordered = OrderEvents::find_deliverable_in_memory(to_insert).await?;
                let to_insert = ordered
                    .deliverable()
                    .iter()
                    .chain(ordered.missing_history().iter());

                let store_result = CeramicOneEvent::insert_many(&self.pool, to_insert).await?;

                self.notify_ordering_task(&store_result).await?;

                Ok(Self::partition_store_result(store_result))
            }
            DeliverableRequirement::Lazy => {
                let store_result =
                    CeramicOneEvent::insert_many(&self.pool, to_insert.iter()).await?;

                Ok(Self::partition_store_result(store_result))
            }
        }
    }

    /// Returns two vectors of Event IDs representing (new, existed)
    fn partition_store_result(store: crate::store::InsertResult) -> (Vec<EventId>, Vec<EventId>) {
        store.inserted.into_iter().partition_map(|e| {
            let key = e.inserted.order_key().clone();
            if e.new_key {
                itertools::Either::Left(key)
            } else {
                itertools::Either::Right(key)
            }
        })
    }

    pub(crate) async fn transform_raw_events_to_conclusion_events(
        &self,
        event: EventRowDelivered,
    ) -> Result<ConclusionEvent> {
        let EventRowDelivered {
            cid: event_cid,
            event,
            delivered,
        } = event;
        let stream_cid = event.stream_cid();
        let init_event = self.get_event_by_cid(stream_cid).await?;
        let init = ConclusionInit::try_from(init_event).map_err(|e| {
            Error::new_app(anyhow::anyhow!(
                "Malformed event found in the database: {}",
                e
            ))
        })?;

        match event {
            ceramic_event::unvalidated::Event::Time(time_event) => {
                Ok(ConclusionEvent::Time(ConclusionTime {
                    event_cid,
                    init,
                    previous: vec![*time_event.prev()],
                    index: delivered as u64,
                }))
            }
            ceramic_event::unvalidated::Event::Signed(signed_event) => {
                match signed_event.payload() {
                    ceramic_event::unvalidated::Payload::Data(data) => {
                        Ok(ConclusionEvent::Data(ConclusionData {
                            event_cid,
                            init,
                            previous: vec![*data.prev()],
                            data: data.data().to_json_bytes().map_err(|e| {
                                Error::new_app(anyhow::anyhow!(
                                    "Failed to serialize IPLD data: {}",
                                    e
                                ))
                            })?,
                            index: delivered as u64,
                        }))
                    }
                    ceramic_event::unvalidated::Payload::Init(init_event) => {
                        Ok(ConclusionEvent::Data(ConclusionData {
                            event_cid,
                            init,
                            previous: vec![],
                            data: init_event.data().to_json_bytes().map_err(|e| {
                                Error::new_app(anyhow::anyhow!(
                                    "Failed to serialize IPLD data: {}",
                                    e
                                ))
                            })?,
                            index: delivered as u64,
                        }))
                    }
                }
            }
            ceramic_event::unvalidated::Event::Unsigned(unsigned_event) => {
                Ok(ConclusionEvent::Data(ConclusionData {
                    event_cid,
                    init,
                    previous: vec![],
                    data: unsigned_event
                        .payload()
                        .data()
                        .to_json_bytes()
                        .map_err(|e| {
                            Error::new_app(anyhow::anyhow!("Failed to serialize IPLD data: {}", e))
                        })?,
                    index: 0,
                }))
            }
        }
    }

    // Helper method to get an event by its CID
    async fn get_event_by_cid(&self, cid: &Cid) -> Result<ceramic_event::unvalidated::Event<Ipld>> {
        let data_bytes = CeramicOneEvent::value_by_cid(&self.pool, cid)
            .await?
            .ok_or_else(|| Error::new_fatal(anyhow::anyhow!("Event not found for CID: {}", cid)))?;

        let (_, event) = ceramic_event::unvalidated::Event::<Ipld>::decode_car(
            std::io::Cursor::new(data_bytes),
            false,
        )
        .map_err(|e| Error::new_fatal(anyhow::anyhow!("Failed to decode CAR data: {}", e)))?;

        Ok(event)
    }

    pub(crate) async fn fetch_events_since_highwater_mark(
        &self,
        highwater_mark: i64,
        limit: i64,
    ) -> Result<Vec<EventRowDelivered>> {
        let (_, data) =
            CeramicOneEvent::new_events_since_value_with_data(&self.pool, highwater_mark, limit)
                .await?;
        Ok(data)
    }

    async fn notify_ordering_task(
        &self,
        store_result: &crate::store::InsertResult<'_>,
    ) -> Result<()> {
        for ev in store_result.inserted.iter() {
            self.send_discovered_event(DiscoveredEvent {
                cid: *ev.inserted.cid(),
                prev: ev.inserted.event().prev().copied(),
                id: *ev.inserted.event().stream_cid(),
                known_deliverable: ev.inserted.deliverable(),
            })
            .await?;
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
pub enum ValidationError {
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
    /// A time event proof was invalid for some reason
    InvalidTimeProof {
        key: EventId,
        reason: String,
    },
}

#[derive(Debug, PartialEq, Eq, Default)]
pub struct InsertResult {
    pub rejected: Vec<ValidationError>,
    pub new: Vec<EventId>,
    pub existed: Vec<EventId>,
    pub pending_count: usize,
}

impl InsertResult {
    pub fn new(
        new: Vec<EventId>,
        existed: Vec<EventId>,
        rejected: Vec<ValidationError>,
        pending_count: usize,
    ) -> Self {
        Self {
            rejected,
            new,
            existed,
            pending_count,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiscoveredEvent {
    /// The Cid of this event.
    pub(crate) cid: Cid,
    /// The prev event that this event builds on.
    pub(crate) prev: Option<Cid>,
    /// The Cid of the init event that identifies the stream this event belongs to.
    pub(crate) id: Cid,
    /// Whether this event is known to already be deliverable.
    pub(crate) known_deliverable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationRequirement {
    /// Whether we should check the signature is currently valid or simply whether it was once valid
    pub check_exp: bool,
    /// Whether events without a known init event should be considered invalid or may be "pended" until init events are discovered.
    /// If true, we fail validation if we can't find the init event to validate the signature.
    /// If false: the init event may not yet be known to the node and we'll store it in memory until we discover it.
    pub require_local_init: bool,
}

impl ValidationRequirement {
    /// Creates the expected validation requirements for a local write
    pub fn new_local() -> Self {
        Self {
            check_exp: true,
            require_local_init: true,
        }
    }

    /// Creates the expected validation requirements for a recon write
    pub fn new_recon() -> Self {
        Self {
            check_exp: false,
            require_local_init: false,
        }
    }
}
