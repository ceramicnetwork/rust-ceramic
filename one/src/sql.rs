use std::path::Path;

use anyhow::Result;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

pub async fn connect(path: impl AsRef<Path>) -> Result<SqlitePool> {
    let pool = SqlitePoolOptions::new()
        // sqlite performs best with a single active connection at a time.
        .max_connections(1)
        .connect(&format!("sqlite:{}?mode=rwc", path.as_ref().display()))
        .await?;

    // set the WAL PRAGMA for faster writes
    const SET_WAL_PRAGMA: &str = "PRAGMA journal_mode=wal;";
    sqlx::query(SET_WAL_PRAGMA).execute(&pool).await?;

    Ok(pool)
}
