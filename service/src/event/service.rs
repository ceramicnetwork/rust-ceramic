use std::collections::HashSet;

use ceramic_core::EventId;
use ceramic_store::{CeramicOneEvent, EventInsertable, SqlitePool};
use recon::InsertResult;
use tracing::{trace, warn};

use super::ordering_task::{DeliverableTask, OrderingTask};

use crate::{Error, Result};

/// The max number of events we can have pending for delivery in the channel before we start dropping them.
/// This quickly becomes a bottleneck with very long streams and high throughput.
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
    ) -> Result<recon::InsertResult> {
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
        self.insert_events(items, false).await
    }

    async fn insert_events<'a>(
        &self,
        items: &[recon::ReconItem<'a, EventId>],
        require_history: bool,
    ) -> Result<recon::InsertResult> {
        if items.is_empty() {
            return Ok(InsertResult::default());
        }

        let mut to_insert = Vec::with_capacity(items.len());

        for event in items {
            let insertable = EventInsertable::try_new(event.key.to_owned(), event.value).await?;
            to_insert.push(insertable);
        }

        let res = CeramicOneEvent::insert_many(&self.pool, &to_insert[..], require_history).await?;
        for ev in res.delivered {
            trace!(event=?ev, "sending delivered to ordering task");
            if let Err(e) = self.delivery_task.tx_delivered.try_send(ev) {
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
        let updated_streams = res
            .undelivered
            .iter()
            .map(|ev| ev.stream_cid)
            .collect::<HashSet<_>>();
        for stream_cid in updated_streams {
            trace!(event=?stream_cid, "sending updated to ordering task");
            if let Err(e) = self.delivery_task.tx_stream_update.try_send(stream_cid) {
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
        // TODO: this order is different than the original request!!
        Ok(recon::InsertResult::new(res.keys))
    }
}
