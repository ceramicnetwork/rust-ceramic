use std::str::FromStr;

use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    Sqlite, Transaction,
};

use crate::Result;

/// How to handle outstanding database migrations.
/// Intend to add a `Check` variant to verify the database is up to date and return an error if it is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Migrations {
    /// Apply migrations after opening connection
    Apply,
    /// Do nothing
    Skip,
}

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
    pub async fn connect(path: &str, migrate: Migrations) -> Result<Self> {
        // As we benchmark, we will likely adjust settings and make things configurable.
        // A few ideas: number of RO connections, mmap_size, temp_store = memory
        let conn_opts = SqliteConnectOptions::from_str(path)?
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .create_if_missing(true)
            .optimize_on_close(true, None)
            .foreign_keys(true);

        let ro_opts = conn_opts.clone().read_only(true);

        let writer = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .connect_with(conn_opts)
            .await?;
        let reader = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(8)
            .connect_with(ro_opts)
            .await?;

        if migrate == Migrations::Apply {
            sqlx::migrate!("../migrations/sqlite")
                .run(&writer)
                .await
                .map_err(sqlx::Error::from)?;
        }

        Ok(Self { writer, reader })
    }

    /// Creates an in-memory database. Useful for testing. Automatically applies migrations since all memory databases start empty
    /// and are not shared between connections.
    pub async fn connect_in_memory() -> Result<Self> {
        SqlitePool::connect(":memory:", Migrations::Apply).await
    }

    /// run pragma optimize. recommended once per day for long running applications
    pub async fn optimize(&self) -> Result<()> {
        sqlx::query("pragma optimize").execute(&self.writer).await?;
        Ok(())
    }

    /// Merge the blocks from one sqlite database into this one.
    pub async fn merge_blocks_from_sqlite(&self, input_ceramic_db_filename: &str) -> Result<()> {
        sqlx::query(
                    "
                            ATTACH DATABASE $1 AS other;
                            INSERT OR IGNORE INTO ceramic_one_block SELECT multihash, bytes FROM other.ceramic_one_block;
                        ",
                )
                .bind(input_ceramic_db_filename)
                .execute(&self.writer)
                .await?;
        Ok(())
    }

    /// Backup the sqlite file to the given filename.
    pub async fn backup_to_sqlite(&self, output_ceramic_db_filename: &str) -> Result<()> {
        sqlx::query(".backup $1")
            .bind(output_ceramic_db_filename)
            .execute(&self.writer)
            .await?;
        Ok(())
    }

    /// Begin a transaction. The transaction must be committed by calling `commit_tx`.
    /// Will be rolled back on drop if not committed.
    pub async fn begin_tx(&self) -> Result<SqliteTransaction> {
        let tx = self.writer.begin().await?;
        Ok(SqliteTransaction { tx })
    }

    /// Get a reference to the writer database pool. The writer pool has only one connection.
    /// If you are going to do multiple writes in a row, instead use `tx` and `commit`.
    pub fn writer(&self) -> &sqlx::SqlitePool {
        &self.writer
    }

    /// Get a reference to the reader database pool. The reader pool has many connections.
    pub fn reader(&self) -> &sqlx::SqlitePool {
        &self.reader
    }

    /// Run an arbitrary SQL statement on the writer pool.
    pub async fn run_statement(&self, statement: &str) -> Result<()> {
        let _res = sqlx::query(statement).execute(self.writer()).await?;
        Ok(())
    }
}

#[derive(Debug)]
/// A wrapper around a sqlx Sqlite transaction
pub struct SqliteTransaction<'a> {
    tx: Transaction<'a, Sqlite>,
}

impl<'a> SqliteTransaction<'a> {
    /// Commit the transaction. If this is not called, the transaction will be rolled back on drop.
    pub async fn commit(self) -> Result<()> {
        self.tx.commit().await?;
        Ok(())
    }

    /// Attempt to rollback a transaction. Automatically rolled back on drop if not committed.
    pub async fn rollback(self) -> Result<()> {
        self.tx.rollback().await?;
        Ok(())
    }

    pub fn inner(&mut self) -> &mut Transaction<'a, Sqlite> {
        &mut self.tx
    }
}
