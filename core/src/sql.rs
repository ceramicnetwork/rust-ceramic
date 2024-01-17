use std::{path::Path, str::FromStr};

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

#[derive(Clone, Debug)]
/// The sqlite pool is split into a writer and a reader pool.
/// Wrapper around the sqlx::SqlitePool
pub struct SqlitePool {
    writer: sqlx::SqlitePool,
    reader: sqlx::SqlitePool,
}

impl SqlitePool {
    /// Connect to the sqlite database at the given path. Creates the database if it does not exist.
    /// Uses WAL journal mode.
    pub async fn connect(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db_path = format!("sqlite:{}", path.as_ref().display());
        let conn_opts = SqliteConnectOptions::from_str(&db_path)?
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true)
            // .synchronous(sqlx::sqlite::SqliteSynchronous::Normal) // normally enough in WAL mode?
            .optimize_on_close(true, None);

        let ro_opts = conn_opts.clone().read_only(true);

        let writer = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(conn_opts)
            .await?;
        let reader = SqlitePoolOptions::new()
            .max_connections(32) //TODO
            .connect_with(ro_opts)
            .await?;

        Ok(Self { writer, reader })
    }

    /// Get a reference to the writer database pool. The writer pool has only one connection.
    pub fn writer(&self) -> &sqlx::SqlitePool {
        &self.writer
    }

    /// Get a reference to the reader database pool. The reader pool has many connections.
    pub fn reader(&self) -> &sqlx::SqlitePool {
        &self.reader
    }
}
