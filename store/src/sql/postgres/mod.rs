mod event;
mod interest;

use std::str::FromStr;

pub use event::EventStorePostgres;
pub use interest::InterestStorePostgres;

use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    Postgres, Transaction,
};

use crate::{Migrations, StoreResult};

/// A trivial wrapper around a sqlx Sqlite database transaction
pub type DbTxPg<'a> = Transaction<'a, Postgres>;

#[derive(Clone, Debug)]
/// Wrapper to postgres database pool
pub struct PostgresPool {
    pool: sqlx::PgPool,
}

impl PostgresPool {
    /// Connect to the sqlite database at the given path. Creates the database if it does not exist.
    /// Uses WAL journal mode.
    pub async fn connect(path: &str, migrate: Migrations) -> StoreResult<Self> {
        let conn_opts = PgConnectOptions::from_str(path)?;

        let pool = PgPoolOptions::new()
            .min_connections(1)
            .max_connections(16)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect_with(conn_opts)
            .await?;

        if migrate == Migrations::Apply {
            sqlx::migrate!("../migrations/postgres")
                .run(&pool)
                .await
                .map_err(sqlx::Error::from)?;
        }

        Ok(Self { pool })
    }

    /// Useful for testing. Automatically applies migrations. Requires a localhost (e.g. docker) postgres database that is hardcoded
    /// to match the makefile, or uses the environment variable `TEST_DATABASE_URL` if set.
    #[allow(dead_code)]
    pub(crate) async fn connect_in_memory() -> StoreResult<Self> {
        let addr = std::env::var("TEST_DATABASE_URL")
            .unwrap_or("postgres://postgres:c3ram1c@localhost:5432/ceramic_one_tests".to_string());
        Self::connect(&addr, Migrations::Apply).await
    }

    /// Get a reference to the writer database pool. The writer pool has only one connection.
    /// If you are going to do multiple writes in a row, instead use `tx` and `commit`.
    pub fn writer(&self) -> &sqlx::PgPool {
        &self.pool
    }

    /// Get a writer tranaction. The writer pool has only one connection so this is an exclusive lock.
    /// Use this method to perform simultaneous writes to the database, calling `commit` when you are done.
    pub async fn tx(&self) -> StoreResult<DbTxPg> {
        // Ok(self.writer.begin().await?)
        Ok(self.pool.begin().await?)
    }

    /// Get a reference to the reader database pool. The reader pool has many connections.
    pub fn reader(&self) -> &sqlx::PgPool {
        &self.pool
    }

    /// Run a statement on the database. Useful for creating tables, etc.
    pub async fn run_statement(&self, statement: &str) -> anyhow::Result<()> {
        sqlx::query(statement).execute(&self.pool).await?;
        Ok(())
    }
}
