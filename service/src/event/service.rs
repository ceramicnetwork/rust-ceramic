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

/// The max number of events we can have pending for delivery in the channel before we start dropping them.
pub(crate) const PENDING_EVENTS_CHANNEL_DEPTH: usize = 10_000;

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
    /// This function is used to parse the event from the carfile and return the insertable event and the previous cid pointer.
    /// Probably belongs in the event crate.
    pub(crate) async fn parse_event_carfile_order_key(
        event_id: EventId,
        carfile: &[u8],
    ) -> Result<(EventInsertable, EventMetadata)> {
        let mut insertable = EventInsertable::try_from_carfile(event_id, carfile).await?;

        let header = Self::parse_event_body(&mut insertable.body).await?;
        Ok((insertable, header))
    }

    pub(crate) async fn parse_event_carfile_cid(
        cid: ceramic_core::Cid,
        carfile: &[u8],
    ) -> Result<InsertableBodyWithMeta> {
        let mut body = EventInsertableBody::try_from_carfile(cid, carfile).await?;

        let header = Self::parse_event_body(&mut body).await?;
        Ok(InsertableBodyWithMeta { body, header })
    }

    pub(crate) async fn parse_event_body(body: &mut EventInsertableBody) -> Result<EventMetadata> {
        let cid = body.cid(); // purely for convenience writing out the match
        let ev_block = body.block_for_cid(&cid)?;

        trace!(count=%body.blocks().len(), %cid, "parsing event blocks");
        let event_ipld: unvalidated::RawEvent<Ipld> =
            serde_ipld_dagcbor::from_slice(&ev_block.bytes).map_err(|e| {
                Error::new_invalid_arg(
                    anyhow::anyhow!(e).context("event block is not valid event format"),
                )
            })?;

        let (deliverable, header) = match event_ipld {
            unvalidated::RawEvent::Time(t) => (
                false,
                EventMetadata::Time {
                    cid,
                    stream_cid: t.id(),
                    prev: t.prev(),
                },
            ),
            unvalidated::RawEvent::Signed(signed) => {
                let link = signed.link().ok_or_else(|| {
                    Error::new_invalid_arg(anyhow::anyhow!("event should have a link"))
                })?;
                let link = body
                    .blocks()
                    .iter()
                    .find(|b| b.cid() == link)
                    .ok_or_else(|| {
                        Error::new_invalid_arg(anyhow::anyhow!("prev CID missing from carfile"))
                    })?;
                let payload: unvalidated::Payload<Ipld> =
                    serde_ipld_dagcbor::from_slice(&link.bytes).map_err(|e| {
                        Error::new_invalid_arg(
                            anyhow::anyhow!(e).context("Failed to follow event link"),
                        )
                    })?;

                match payload {
                    unvalidated::Payload::Data(d) => (
                        false,
                        EventMetadata::Data {
                            cid,
                            stream_cid: *d.id(),
                            prev: *d.prev(),
                        },
                    ),
                    unvalidated::Payload::Init(_init) => (true, EventMetadata::Init { cid }),
                }
            }
            unvalidated::RawEvent::Unsigned(_init) => (true, EventMetadata::Init { cid }),
        };
        body.set_deliverable(deliverable);
        Ok(header)
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
                Self::parse_event_carfile_order_key(event.key.to_owned(), event.value).await?;
            to_insert.push(insertable);
        }

        let ordered = OrderEvents::try_new(&self.pool, to_insert).await?;

        let missing_history = ordered
            .missing_history
            .iter()
            .map(|(e, _)| e.order_key.clone())
            .collect();

        let to_insert_with_header = if history_required {
            ordered.deliverable
        } else {
            ordered
                .deliverable
                .into_iter()
                .chain(ordered.missing_history)
                .collect()
        };

        let to_insert = to_insert_with_header
            .iter()
            .map(|(e, _)| e.clone())
            .collect::<Vec<_>>();

        // need to make a better interface around events. needing to know about the stream in some places but
        // not in the store makes it inconvenient to map back and forth
        let res = CeramicOneEvent::insert_many(&self.pool, &to_insert[..]).await?;

        // api writes shouldn't have any missed pieces that need ordering so we don't send those
        if !history_required {
            let to_send = res
                .inserted
                .iter()
                .filter(|i| i.new_key)
                .collect::<Vec<_>>();

            for ev in to_send {
                if let Some((ev, header)) = to_insert_with_header
                    .iter()
                    .find(|(i, _)| i.order_key == ev.order_key)
                {
                    let new = InsertableBodyWithMeta {
                        body: ev.body.clone(),
                        header: header.to_owned(),
                    };
                    trace!(event=?ev, "sending delivered to ordering task");
                    if let Err(e) = self.delivery_task.tx_inserted.try_send(new) {
                        match e {
                            tokio::sync::mpsc::error::TrySendError::Full(e) => {
                                warn!(event=?e, limit=%PENDING_EVENTS_CHANNEL_DEPTH, "Delivery task full. Dropping event and will not be able to mark deliverable new stream event arrives or process is restarted");
                            }
                            tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                                warn!("Delivery task closed. shutting down");
                                return Err(Error::new_fatal(anyhow::anyhow!(
                                    "Delivery task closed"
                                )));
                            }
                        }
                    }
                } else {
                    tracing::error!(event_id=%ev.order_key, "Missing header for inserted event should be unreachable!");
                    debug_assert!(false); // panic in debug mode
                    continue;
                }
            }
        }

        Ok(InsertResult {
            store_result: res,
            missing_history,
        })
    }

    pub(crate) async fn load_by_cid(
        pool: &SqlitePool,
        cid: ceramic_core::Cid,
    ) -> Result<Option<InsertableBodyWithMeta>> {
        let data = if let Some(ev) = CeramicOneEvent::value_by_cid(pool, &cid).await? {
            ev
        } else {
            return Ok(None);
        };

        let mut body = EventInsertableBody::try_from_carfile(cid, &data).await?;
        let header = Self::parse_event_body(&mut body).await?;
        Ok(Some(InsertableBodyWithMeta { body, header }))
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

#[derive(Debug, Clone)]
pub(crate) struct InsertableBodyWithMeta {
    pub(crate) body: EventInsertableBody,
    pub(crate) header: EventMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// An event header wrapper for use in the store crate.
/// TODO: replace this with something from the event crate
pub(crate) enum EventMetadata {
    Init {
        cid: ceramic_core::Cid,
    },
    Data {
        cid: ceramic_core::Cid,
        stream_cid: ceramic_core::Cid,
        prev: ceramic_core::Cid,
    },
    Time {
        cid: ceramic_core::Cid,
        stream_cid: ceramic_core::Cid,
        prev: ceramic_core::Cid,
    },
}

impl EventMetadata {
    /// Returns the stream CID of the event
    pub(crate) fn stream_cid(&self) -> ceramic_core::Cid {
        match self {
            EventMetadata::Init { cid, .. } => *cid,
            EventMetadata::Data { stream_cid, .. } | EventMetadata::Time { stream_cid, .. } => {
                *stream_cid
            }
        }
    }

    pub(crate) fn prev(&self) -> Option<ceramic_core::Cid> {
        match self {
            EventMetadata::Init { .. } => None,
            EventMetadata::Data { prev, .. } | EventMetadata::Time { prev, .. } => Some(*prev),
        }
    }
}
