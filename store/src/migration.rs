use std::sync::OnceLock;

use sqlx::{prelude::FromRow, types::chrono};
use tracing::{debug, info};

use crate::{Error, Result, SqlitePool};

static MIGRATIONS: OnceLock<Vec<Migration>> = OnceLock::new();

#[derive(Debug)]
struct Migration {
    /// The name of the migration.
    name: &'static str,
    /// The version this migration was released (anything below this should be migrated).
    _version: &'static str,
}

/// The list of migrations that need to be run in order to get the database up to date and start the server.
/// Add new migrations to the end of the list.
fn data_migrations() -> &'static Vec<Migration> {
    MIGRATIONS.get_or_init(Vec::new)
}

#[derive(Debug, Clone, sqlx::FromRow)]
// We want to retrieve these fields for logging but we don't refer to them directly
#[allow(dead_code)]
struct Version {
    id: i64,
    version: String,
    installed_at: chrono::NaiveDateTime,
}

impl Version {
    async fn fetch_previous(pool: &SqlitePool, current_version: &str) -> Result<Option<Self>> {
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

    async fn insert_current(pool: &SqlitePool, current_version: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO ceramic_one_version (version) VALUES ($1) ON CONFLICT DO NOTHING;",
        )
        .bind(current_version)
        .execute(pool.writer())
        .await?;
        Ok(())
    }
}

/// The data migrator is responsible for running data migrations the node requires as part of the verison upgrade.
#[derive(Debug)]
pub struct DataMigrator {
    required_migrations: Vec<&'static Migration>,
    pool: SqlitePool,
}

impl DataMigrator {
    /// Create a new data migrator. This updates the version table with the current version and determine which migrations need to be run.
    pub async fn try_new(pool: SqlitePool) -> Result<Self> {
        let current_version = env!("CARGO_PKG_VERSION").to_string();
        let prev_version = Version::fetch_previous(&pool, &current_version).await?;

        Version::insert_current(&pool, &current_version).await?;

        let required_migrations =
            Self::determine_required_migrations(&pool, prev_version, &current_version).await?;

        Ok(Self {
            required_migrations,
            pool,
        })
    }

    async fn determine_required_migrations(
        pool: &SqlitePool,
        prev_version: Option<Version>,
        current_version: &str,
    ) -> Result<Vec<&'static Migration>> {
        let new_install = Self::is_new_install(pool, &prev_version, current_version).await?;

        let applied_migrations = DataMigration::fetch_all(pool).await?;
        debug!(?prev_version, %current_version, ?applied_migrations, "Current data migration status");
        // In the future, we can filter out migrations that are not required based on the version as well
        let unapplied_migrations = data_migrations()
            .iter()
            .flat_map(|candidate| {
                if applied_migrations
                    .iter()
                    .find(|m| m.name == candidate.name)
                    .and_then(|m| m.completed_at)
                    .is_some()
                {
                    None
                } else {
                    Some(candidate)
                }
            })
            .collect::<Vec<_>>();
        if new_install {
            debug!("Setting up new node... data migrations are not required.");
            for migration_info in &unapplied_migrations {
                DataMigration::insert_completed(pool, migration_info.name).await?;
            }
            Ok(Vec::new())
        } else {
            Ok(unapplied_migrations)
        }
    }

    /// Determines whether migrations are needed
    pub fn needs_migration(&self) -> bool {
        !self.required_migrations.is_empty()
    }

    /// In the future, we can check the version and run migrations based on that. For now, we need to know if this is just getting
    /// the new feature that tracks versions, or if it is a fresh install. We use the presence of recon event data to indicate it's not new.
    async fn is_new_install(
        pool: &SqlitePool,
        prev_version: &Option<Version>,
        _current_version: &str,
    ) -> Result<bool> {
        if prev_version.is_none() {
            let row = sqlx::query(r#"SELECT cid as rowid from ceramic_one_event;"#)
                .fetch_optional(pool.reader())
                .await?;
            Ok(row.is_none())
        } else {
            Ok(false)
        }
    }

    /// Run all migrations that have not been run yet.
    pub async fn run_all(&mut self) -> Result<()> {
        for migration_info in &self.required_migrations {
            info!("Starting migration: {}", migration_info.name);
            DataMigration::upsert_as_started(&self.pool, migration_info.name).await?;
            self.run_migration_by_name(migration_info.name).await?;
            DataMigration::mark_completed(&self.pool, migration_info.name).await?;
        }
        Ok(())
    }

    async fn run_migration_by_name(&self, name: &str) -> Result<()> {
        #[allow(clippy::match_single_binding)]
        let _res: Result<()> = match name {
            _ => {
                return Err(Error::new_fatal(anyhow::anyhow!(
                    "Unknown migration: {}",
                    name
                )))
            }
        };
        #[allow(unreachable_code)]
        match _res {
            Ok(_) => {
                info!("Migration {} completed successfully", name);
                Ok(())
            }
            Err(e) => {
                tracing::error!("Migration encountered error: {:?}", e);
                match e {
                    Error::Fatal { error } => Err(Error::new_fatal(anyhow::anyhow!(
                        "Migration {} failed in irrecoverable way: {}",
                        name,
                        error
                    ))),

                    e => {
                        let err = e.context("Migration failed but can be retried");
                        Err(err)
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, FromRow)]
// we want to retrieve these fields for logging but we don't refer to them directly
#[allow(dead_code)]
struct DataMigration {
    name: String,
    version: String,
    started_at: chrono::NaiveDateTime,
    last_attempted_at: chrono::NaiveDateTime,
    completed_at: Option<chrono::NaiveDateTime>,
}

impl DataMigration {
    async fn fetch_all(pool: &SqlitePool) -> Result<Vec<Self>> {
        Ok(sqlx::query_as(
            "SELECT name, version, started_at, completed_at, last_attempted_at FROM ceramic_one_data_migration;",
        )
        .fetch_all(pool.reader())
        .await?)
    }

    async fn upsert_as_started(pool: &SqlitePool, name: &str) -> Result<()> {
        sqlx::query("INSERT INTO ceramic_one_data_migration (name, version) VALUES ($1, $2) on conflict (name) do update set last_attempted_at = CURRENT_TIMESTAMP;")
            .bind(name)
            .bind(env!("CARGO_PKG_VERSION"))
            .execute(pool.writer())
            .await?;
        Ok(())
    }

    async fn insert_completed(pool: &SqlitePool, name: &str) -> Result<()> {
        sqlx::query("INSERT INTO ceramic_one_data_migration (name, version, completed_at) VALUES ($1, $2, CURRENT_TIMESTAMP) on conflict (name) do nothing;")
            .bind(name)
            .bind(env!("CARGO_PKG_VERSION"))
            .execute(pool.writer())
            .await?;
        Ok(())
    }

    async fn mark_completed(pool: &SqlitePool, name: &str) -> Result<()> {
        sqlx::query("UPDATE ceramic_one_data_migration SET completed_at = CURRENT_TIMESTAMP WHERE name = $1 and completed_at IS NULL;").bind(name).execute(pool.writer()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    #[tokio::test]
    async fn correctly_skips_new_install() {
        let pool = crate::sql::SqlitePool::connect_in_memory().await.unwrap();
        let migrator = crate::migration::DataMigrator::try_new(pool).await.unwrap();
        assert!(!migrator.needs_migration());
    }
}
