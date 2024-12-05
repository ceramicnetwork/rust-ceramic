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
    optimize_requested: bool,
}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum SqliteTempStore {
    Default,
    File,
    Memory,
}

impl SqliteTempStore {
    fn pragma_value(self) -> String {
        match self {
            SqliteTempStore::Default => 0.to_string(),
            SqliteTempStore::File => 1.to_string(),
            SqliteTempStore::Memory => 2.to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SqliteOpts {
    /// Value to use for the sqlite cache_size pragma
    /// Use the negative version, which represents Kib e.g. 20000 = 20 Mb
    /// Or the postive version, representing pages
    /// None means the default is used.
    pub cache_size: Option<i64>,
    /// Used for pragma mmap_size
    /// 10737418240: 10 GB of memory mapped IO
    /// Set to 0 to disable. None is the default
    pub mmap_size: Option<u64>,
    /// Number of connections in the read only pool (default 8)
    pub max_ro_connections: u32,
    pub temp_store: Option<SqliteTempStore>,
    /// The analysis limit to use for optimize/analysis.
    /// 0 means no limit.
    /// Recommended values are 100-1000 (higher meaning do more work).
    /// Defaults to 1000.
    pub analysis_limit: u32,
    /// Whether or not optimize should be run. Uses `analysis_limit`
    /// if set to adjust how much work is done.
    pub optimize: bool,
}

impl Default for SqliteOpts {
    fn default() -> Self {
        Self {
            mmap_size: None,
            cache_size: None,
            max_ro_connections: 8,
            temp_store: None,
            analysis_limit: 1000,
            optimize: true,
        }
    }
}

impl SqlitePool {
    /// Connect to the sqlite database at the given path. Creates the database if it does not exist.
    /// Uses WAL journal mode.
    pub async fn connect(path: &str, opts: SqliteOpts, migrate: Migrations) -> Result<Self> {
        let conn_opts = SqliteConnectOptions::from_str(path)?
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .create_if_missing(true)
            .foreign_keys(true)
            .analysis_limit(opts.analysis_limit);

        let conn_opts = if let Some(cache) = opts.cache_size {
            conn_opts.pragma("cache_size", cache.to_string())
        } else {
            conn_opts
        };
        let conn_opts = if let Some(mmap) = opts.mmap_size {
            conn_opts.pragma("mmap_size", mmap.to_string())
        } else {
            conn_opts
        };
        let conn_opts = if let Some(temp) = opts.temp_store {
            conn_opts.pragma("temp_store", temp.pragma_value())
        } else {
            conn_opts
        };

        let ro_opts = conn_opts.clone().read_only(true);

        // optimize can only be used on connections with write access
        // make sure the initial limit is set above always and use the limit when closing as well
        let write_opts = if opts.optimize {
            conn_opts.optimize_on_close(true, opts.analysis_limit)
        } else {
            conn_opts
        };

        let writer = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(1)
            .connect_with(write_opts)
            .await?;
        let reader = SqlitePoolOptions::new()
            .min_connections(1)
            .max_connections(opts.max_ro_connections)
            .connect_with(ro_opts)
            .await?;

        if migrate == Migrations::Apply {
            sqlx::migrate!("../migrations/sqlite")
                .run(&writer)
                .await
                .map_err(sqlx::Error::from)?;
        }

        Ok(Self {
            writer,
            reader,
            optimize_requested: opts.optimize,
        })
    }

    /// Creates an in-memory database. Useful for testing. Automatically applies migrations since all memory databases start empty
    /// and are not shared between connections.
    pub async fn connect_in_memory() -> Result<Self> {
        SqlitePool::connect(":memory:", SqliteOpts::default(), Migrations::Apply).await
    }

    pub fn optimize_requested(&self) -> bool {
        self.optimize_requested
    }

    /// run pragma optimize. recommended once per day for long running applications
    /// if startup is true, will use recommended `0x10002` (Run ANALYZE on tables that might benefit)
    /// Recommended practice is that applications with long-lived database connections should run "PRAGMA optimize=0x10002"
    /// when the database connection first opens, then run "PRAGMA optimize" again at periodic intervals - perhaps once per day
    /// https://www.sqlite.org/pragma.html#pragma_optimize
    pub async fn optimize(&self, startup: bool) -> Result<()> {
        let stmt = if startup {
            "PRAGMA optimize=0x10002"
        } else {
            "PRAGMA optimize"
        };
        sqlx::query(stmt).execute(&self.writer).await?;
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
