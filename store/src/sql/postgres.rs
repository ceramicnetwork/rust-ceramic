use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use crate::{DbTx, Migrations};

#[derive(Clone, Debug)]
/// Wrapper to postgres database pool
pub struct PostgresPool {
    pool: sqlx::PgPool,
}

impl PostgresPool {
    /// Connect to the sqlite database at the given path. Creates the database if it does not exist.
    /// Uses WAL journal mode.
    pub async fn connect(path: &str, migrate: Migrations) -> anyhow::Result<Self> {
        let conn_opts = PgConnectOptions::from_str(path)?;

        let pool = PgPoolOptions::new()
            .min_connections(1)
            .max_connections(16)
            .acquire_timeout(std::time::Duration::from_secs(1))
            .connect_with(conn_opts)
            .await?;

        if migrate == Migrations::Apply {
            sqlx::migrate!("../migrations/postgres").run(&pool).await?;
        }

        Ok(Self { pool })
    }

    /// Creates an in-memory database. Useful for testing. Automatically applies migrations since all memory databases start empty
    /// and are not shared between connections.
    pub async fn connect_in_memory() -> anyhow::Result<Self> {
        Self::connect(
            "postgres://postgres:c3ram1c@localhost:5432/ceramic",
            Migrations::Apply,
        )
        .await
    }

    /// Get a reference to the writer database pool. The writer pool has only one connection.
    /// If you are going to do multiple writes in a row, instead use `tx` and `commit`.
    pub fn writer(&self) -> &sqlx::PgPool {
        &self.pool
    }

    /// Get a writer tranaction. The writer pool has only one connection so this is an exclusive lock.
    /// Use this method to perform simultaneous writes to the database, calling `commit` when you are done.
    pub async fn tx(&self) -> anyhow::Result<DbTx> {
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
