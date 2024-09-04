use sqlx::types::chrono;

use crate::store::{Result, SqlitePool};

#[derive(Debug, Clone, sqlx::FromRow)]
// We want to retrieve these fields for logging but we don't refer to them directly
#[allow(dead_code)]
pub struct VersionRow {
    id: i64,
    pub version: String,
    pub installed_at: chrono::NaiveDateTime,
    pub last_started_at: chrono::NaiveDateTime,
}

impl VersionRow {
    /// Return the version installed before the current version
    pub async fn _fetch_previous(pool: &SqlitePool, current_version: &str) -> Result<Option<Self>> {
        Ok(sqlx::query_as(
            "SELECT id, version, installed_at 
            FROM ceramic_one_version
            WHERE version <> $1
         ORDER BY installed_at DESC limit 1;",
        )
        .bind(current_version)
        .fetch_optional(pool.reader())
        .await?)
    }

    /// Add the current version to the database, updating the last_started_at field if the version already exists
    pub async fn insert_current(pool: &SqlitePool, current_version: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO ceramic_one_version (version) VALUES ($1) ON CONFLICT (version) DO UPDATE set last_started_at = CURRENT_TIMESTAMP;",
        )
        .bind(current_version)
        .execute(pool.writer())
        .await?;
        Ok(())
    }
}
