use std::collections::HashSet;

use async_trait::async_trait;
use ceramic_core::{DidDocument, EventId, Network};
use ceramic_event::unvalidated;
use ceramic_store::{CeramicOneEvent, EventInsertable, EventInsertableBody, SqlitePool};
use cid::Cid;
use futures::stream::BoxStream;
use ipld_core::ipld::Ipld;
use tracing::{trace, warn};


use super::{
    migration::Migrator,
    order_events::OrderEvents,
    ordering_task::{DeliverableTask, OrderingTask}, validated::EventValidator,
};
use crate::{Error, Result};
use crate::event::validated::ValidateEvent;

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

impl CeramicEventService {
    /// Create a new CeramicEventStore
    pub async fn new(pool: SqlitePool) -> Result<Self> {
        CeramicOneEvent::init_delivered_order(&pool).await?;

        let delivery_task = OrderingTask::run(pool.clone(), PENDING_EVENTS_CHANNEL_DEPTH).await;

        Ok(Self {
            pool,
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
    /// TODO : in the future we want this method to take in an invalid event and give back a validated event
    /// DOC: Currently only verifies that the event parses into a valid ceramic event.
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
        println!("event_id: {:?}", event_id);
        let validator = EventValidator {
            signer: DidDocument::new("did:example:123"), // Replace with actual DidDocument
        };
        
        let res = validator.validate_event(&parsed_event).await;
        println!("res: {:?}", res);
        if event_cid != cid {
            return Err(Error::new_app(anyhow::anyhow!(
                "EventId CID ({}) does not match the body CID ({})",
                event_cid,
                cid
            )));
        }

        // TODO: add an event to be validated to the queue

        let metadata = EventMetadata::from(parsed_event);
        let body = EventInsertableBody::try_from_carfile(cid, carfile).await?;

        Ok((EventInsertable::try_new(event_id, body)?, metadata))
    }

    /// DOC: Insert events into the database and notify the ordering task
    /// If the events are deliverable, they will be inserted into the database and the ordering task will be notified.
    /// If the events are not deliverable, they will be inserted into the database and the ordering task will be notified when they become deliverable.
    pub(crate) async fn insert_events<'a>(
        &self,
        items: &[recon::ReconItem<'a, EventId>],
        source: DeliverableRequirement,
    ) -> Result<InsertResult> {
        if items.is_empty() {
            return Ok(InsertResult::default());
        }

        let mut to_insert = Vec::with_capacity(items.len());

        /// DOC: Validate the events first
        for event in items {
            let insertable =
                Self::validate_discovered_event(event.key.to_owned(), event.value).await?;
            to_insert.push(insertable);
        }

        /// DOC: Order the events
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

pub(crate) fn decode_multibase_data(value: &str) -> Result<Vec<u8>> {
    Ok(multibase::decode(value)
        .map_err(|err| {
            Error::new_app(anyhow::anyhow!("Invalid event data: multibase error: {err}"))
        })?
        .1)
}

pub const SIGNED_INIT_EVENT_CAR: &str = "
        uO6Jlcm9vdHOB2CpYJgABhQESII6AYH_8-NniumDaEPcbPId6ZQAMFfViEDLxGVnVBNOOZ3ZlcnNpb24
        B0QEBcRIgEXCWI0EH1zQcSl57SUKRoo0WwhL6nMo8kLKfvgNaG0OiZGRhdGGhZXN0ZXBoGQFNZmhlYWR
        lcqRjc2VwZW1vZGVsZW1vZGVsWCjOAQIBhQESIKDoMqM144vTQLQ6DwKZvzxRWg_DPeTNeRCkPouTHo1
        YZnVuaXF1ZUxEpvE6skELu2qFaN5rY29udHJvbGxlcnOBeDhkaWQ6a2V5Ono2TWt0QnluQVBMckV5ZVM
        3cFZ0aGJpeVNjbWZ1OG41Vjdib1hneHlvNXEzU1pSUroCAYUBEiCOgGB__PjZ4rpg2hD3GzyHemUADBX
        1YhAy8RlZ1QTTjqJncGF5bG9hZFgkAXESIBFwliNBB9c0HEpee0lCkaKNFsIS-pzKPJCyn74DWhtDanN
        pZ25hdHVyZXOBomlwcm90ZWN0ZWRYgXsiYWxnIjoiRWREU0EiLCJraWQiOiJkaWQ6a2V5Ono2TWt0Qnl
        uQVBMckV5ZVM3cFZ0aGJpeVNjbWZ1OG41Vjdib1hneHlvNXEzU1pSUiN6Nk1rdEJ5bkFQTHJFeWVTN3B
        WdGhiaXlTY21mdThuNVY3Ym9YZ3h5bzVxM1NaUlIifWlzaWduYXR1cmVYQCQDjlx8fT8rbTR4088HtOE
        27LJMc38DSuf1_XtK14hDp1Q6vhHqnuiobqp5EqNOp0vNFCCzwgG-Dsjmes9jJww";

pub const SIGNED_INIT_EVENT_CID: &str =
        "bagcqcerar2aga7747dm6fota3iipogz4q55gkaamcx2weebs6emvtvie2oha";

pub const DATA_EVENT_ID: &str =
        "ce010500aa5773c7d75777e1deb6cb4af0e69eebd504d38e0185011220275d0719794a4d9eec8db4a735fd9032dfd238fa5af210d4aa9b337590882943";
    
#[tokio::test]
async fn test_validate_signed_init_event_no_cacao() {
    let event_data = SIGNED_INIT_EVENT_CAR
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>();
   

    let decoded_data = decode_multibase_data(&event_data).unwrap();

    let (cid, parsed_event) = unvalidated::Event::<Ipld>::decode_car(decoded_data.as_slice(), false)
        .await
        .unwrap();

    let validator = EventValidator {
        signer: DidDocument::new("did:example:123"), // Replace with actual DidDocument
    };

    let res = validator.validate_event(&parsed_event).await;
    // Assert that the result is Ok
    assert!(res.is_ok(), "Validation should succeed");

    // Extract the event ID based on the ValidatedEvent variant
    let validated_event_id = match res.unwrap() {
        ValidatedEvent::Init(init_event) => init_event.event.envelope_cid(),
        _ => panic!("Invalid event type"),
    };

    // Assert that the validated event ID matches the original CID
    assert_eq!(validated_event_id, cid, "Validated event ID should match the original CID");
    println!("res: {:?}", validated_event_id);
}