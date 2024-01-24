use std::{path::Path, str::FromStr};

use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    Sqlite, Transaction,
};

/// A trivial wrapper around a sqlx Sqlite database transaction
pub type DbTx<'a> = Transaction<'a, Sqlite>;

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
        // As we benchmark, we will likely adjust settings and make things configurable.
        // A few ideas: number of RO connections, synchronize = NORMAL, mmap_size, temp_store = memory
        let conn_opts = SqliteConnectOptions::from_str(&db_path)?
            .journal_mode(SqliteJournalMode::Wal)
            .create_if_missing(true)
            .optimize_on_close(true, None);

        let ro_opts = conn_opts.clone().read_only(true);

        let writer = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(conn_opts)
            .await?;
        let reader = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(ro_opts)
            .await?;

        Ok(Self { writer, reader })
    }

    /// Get a reference to the writer database pool. The writer pool has only one connection.
    /// If you are going to do multiple writes in a row, instead use `tx` and `commit`.
    pub fn writer(&self) -> &sqlx::SqlitePool {
        &self.writer
    }

    /// Get a writer tranaction. The writer pool has only one connection so this is an exclusive lock.
    /// Use this method to perform simultaneous writes to the database, calling `commit` when you are done.
    pub async fn tx(&self) -> anyhow::Result<DbTx> {
        Ok(self.writer.begin().await?)
    }

    /// Get a reference to the reader database pool. The reader pool has many connections.
    pub fn reader(&self) -> &sqlx::SqlitePool {
        &self.reader
    }
}
