use std::path::Path;

use anyhow::Result;
use sqlx::sqlite::SqlitePool;

pub async fn connect(path: impl AsRef<Path>) -> Result<SqlitePool> {
    let pool = SqlitePool::connect(&format!("sqlite:{}?mode=rwc", path.as_ref().display())).await?;

    // set the WAL PRAGMA for faster writes
    const SET_WAL_PRAGMA: &str = "PRAGMA journal_mode=wal;";
    sqlx::query(SET_WAL_PRAGMA).execute(&pool).await?;

    Ok(pool)
}
