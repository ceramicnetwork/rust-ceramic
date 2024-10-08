use anyhow::Result;
use ceramic_sql::sqlite::SqlitePool;

#[derive(Debug)]
pub struct HighWaterMarkStore {
    pub(crate) pool: SqlitePool,
}
impl HighWaterMarkStore {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub(crate) async fn high_water_mark(&self) -> Result<i64> {
        Ok(sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(high_water_mark) FROM ceramic_one_anchor_high_water_mark",
        )
        .fetch_one(self.pool.reader())
        .await?
        .unwrap_or(0))
    }

    /// Inserts a new high water mark after a batch has been anchored
    pub async fn insert_high_water_mark(&self, high_water_mark: i64) -> Result<()> {
        sqlx::query(
            "INSERT INTO ceramic_one_anchor_high_water_mark (high_water_mark) VALUES ($1);",
        )
        .bind(high_water_mark)
        .execute(self.pool.writer())
        .await?;
        Ok(())
    }
}
