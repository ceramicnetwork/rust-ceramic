use ceramic_core::EventId;
use ceramic_store::{CeramicOneEvent, EventInsertable, SqlitePool};
use tracing::{trace, warn};

use super::{
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
            let insertable = EventInsertable::try_new(event.key.to_owned(), event.value).await?;
            to_insert.push(insertable);
        }

        let ordered = OrderEvents::try_new(&self.pool, to_insert).await?;

        let missing_history = ordered
            .missing_history
            .iter()
            .map(|e| e.order_key.clone())
            .collect();

        let to_insert = if history_required {
            ordered.deliverable
        } else {
            ordered
                .deliverable
                .into_iter()
                .chain(ordered.missing_history)
                .collect()
        };

        let res = CeramicOneEvent::insert_many(&self.pool, &to_insert[..]).await?;
        // api writes shouldn't have any missed pieces that need ordering so we don't send those
        if !history_required {
            for ev in &res.inserted {
                if ev.deliverable {
                    trace!(event=?ev, "sending delivered to ordering task");
                    if let Err(e) = self.delivery_task.tx_delivered.try_send(ev.clone()) {
                        match e {
                            tokio::sync::mpsc::error::TrySendError::Full(e) => {
                                // we should only be doing this during recon, in which case we can rediscover events.
                                // the delivery task will start picking up these events once it's drained since they are stored in the db
                                warn!(event=?e, limit=%PENDING_EVENTS_CHANNEL_DEPTH, "Delivery task full. Dropping event and will not be able to mark deliverable until queue drains");
                            }
                            tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                                warn!("Delivery task closed. shutting down");
                                return Err(Error::new_fatal(anyhow::anyhow!(
                                    "Delivery task closed"
                                )));
                            }
                        }
                    }
                }
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
            api_res.push(ceramic_api::EventInsertResult::new(ev.order_key, None));
        }
        for ev in res.missing_history {
            api_res.push(ceramic_api::EventInsertResult::new(
                ev,
                Some("Failed to insert event as `prev` event was missing".to_owned()),
            ));
        }
        api_res
    }
}
