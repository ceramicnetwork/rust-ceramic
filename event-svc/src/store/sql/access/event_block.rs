use ceramic_sql::sqlite::SqliteTransaction;

use crate::store::{
    sql::{entities::EventBlockRaw, query::EventBlockQuery},
    Result,
};

/// Access to the event_block table and related logic
pub struct CeramicOneEventBlock {}

impl CeramicOneEventBlock {
    /// Insert an event block in a transaction i.e. when storing a new ceramic event
    pub(crate) async fn insert(
        conn: &mut SqliteTransaction<'_>,
        ev_block: &EventBlockRaw,
    ) -> Result<()> {
        sqlx::query(EventBlockQuery::upsert())
            .bind(&ev_block.event_cid)
            .bind(ev_block.idx)
            .bind(ev_block.root)
            .bind(ev_block.multihash.to_bytes())
            .bind(ev_block.codec)
            .execute(&mut **conn.inner())
            .await?;
        Ok(())
    }
}
