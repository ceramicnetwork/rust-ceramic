mod event;
mod interest;
mod root;

pub use event::SqliteEventStore;
pub use interest::SqliteInterestStore;
pub use root::RootStore;

use chrono::{SecondsFormat, Utc};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use tracing::info;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Now to handle outstanding database migrations.
/// Intend to add a `Check` variant to verify the database is up to date and return an error if it is not.
pub enum Migrations {
    /// Apply migrations after opening connection
    Apply,
    /// Do nothing
    Skip,
}

impl SqlitePool {
    /// Connect to the sqlite database at the given path. Creates the database if it does not exist.
    /// Uses WAL journal mode.
    pub async fn connect(file_path: impl AsRef<Path>, migrate: Migrations) -> anyhow::Result<Self> {
        let db = file_path.as_ref().display().to_string();
        // As we benchmark, we will likely adjust settings and make things configurable.
        // A few ideas: number of RO connections, synchronize = NORMAL, mmap_size, temp_store = memory
        let conn_opts = SqliteConnectOptions::from_str(&db)?
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .create_if_missing(true)
            .optimize_on_close(true, None)
            .foreign_keys(true);

        let ro_opts = conn_opts.clone().read_only(true);

        let writer = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(1))
            .connect_with(conn_opts)
            .await?;
        let reader = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(8)
            .connect_with(ro_opts)
            .await?;

        if migrate == Migrations::Apply {
            sqlx::migrate!("../migrations/sqlite").run(&writer).await?;
        }

        Ok(Self { writer, reader })
    }

    /// Creates an in-memory database. Useful for testing. Automatically applies migrations since all memory databases start empty
    /// and are not shared between connections.
    pub async fn connect_in_memory() -> anyhow::Result<Self> {
        SqlitePool::connect(":memory:", Migrations::Apply).await
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

    /// Connect to a SQLite file that is stored in store_dir/db.sqlite3
    /// if store_dir is None connect to $HOME/.ceramic-one/db.sqlite3
    /// if $HOME is undefined connect to /data/.ceramic-one/db.sqlite3
    pub async fn from_store_dir(
        store_dir: Option<PathBuf>,
        migrate: Migrations,
    ) -> Result<Self, anyhow::Error> {
        let home: PathBuf = dirs::home_dir().unwrap_or("/data/".into());
        let store_dir = store_dir.unwrap_or(home.join(".ceramic-one/"));
        info!(
            "{} Opening ceramic SQLite DB at: {}",
            Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            store_dir.display()
        );
        Self::connect(store_dir.join("db.sqlite3"), migrate).await
    }
}
