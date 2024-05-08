use anyhow::Result;
use ceramic_store::{CeramicOneEvent, SqlitePool};

#[derive(Debug, Clone)]
/// A database store that verifies the bytes it stores are valid Ceramic events.
/// Implements the [`recon::Store`], [`iroh_bitswap::Store`], and [`ceramic_api::AccessModelStore`] traits for [`ceramic_core::EventId`].
pub struct CeramicEventService {
    pub(crate) pool: SqlitePool,
}

impl CeramicEventService {
    /// Create a new CeramicEventStore
    pub async fn new(pool: SqlitePool) -> Result<Self> {
        // who owns initiating/managing this.. feels awkward rn
        CeramicOneEvent::init_delivered_order(&pool).await?;
        Ok(Self { pool })
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
}

/*
The goal is to implement a "wrapper" of the key value store that recon relies on and adds the ceramic business logic.

Recon does recon things and tells us about keys and values. We need to:
 - Validate that the values are valid ceramic events
    - parse the carfile, build the event ID/order key, find the prev field, etc
 - Store the values in the database AFTER we have validated them.
 - any keys that don't have values or can't be validated should NOT be put on disk
    (or they can be put in a temp table and nuked)
 -

*/
