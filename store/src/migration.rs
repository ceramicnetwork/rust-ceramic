use std::sync::OnceLock;

use sqlx::{prelude::FromRow, types::chrono};
use tracing::{debug, info};

use crate::{
    sql::entities::{EventHeader, ReconEventBlockRaw},
    CeramicOneStream, Error, EventInsertableBody, Result, SqlitePool,
};

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
    MIGRATIONS.get_or_init(|| {
        vec![Migration {
            name: "events_to_streams",
            _version: "0.22.0",
        }]
    })
}

#[derive(Debug, Clone, sqlx::FromRow)]
// We want to retrieve these fields for logging but we don't refer to them directly
#[allow(dead_code)]
struct Version {
    id: i64,
    version: String,
    installed_at: chrono::NaiveDateTime,
}

/// The data migrator is responsible for running data migrations the node requires as part of the verison upgrade.
pub struct DataMigrator {
    prev_version: Option<Version>,
    required_migrations: Vec<&'static Migration>,
    pool: SqlitePool,
}

impl DataMigrator {
    /// Create a new data migrator. This updates the version table with the current version and determine which migrations need to be run.
    pub async fn try_new(pool: SqlitePool) -> Result<Self> {
        let current_version = env!("CARGO_PKG_VERSION").to_string();

        let prev_version: Option<Version> = sqlx::query_as(
            "SELECT id, version, installed_at FROM ceramic_one_version ORDER BY installed_at DESC limit 1;",
        )
        .fetch_optional(pool.reader())
        .await?;

        sqlx::query(
            "INSERT INTO ceramic_one_version (version) VALUES ($1) ON CONFLICT DO NOTHING;",
        )
        .bind(&current_version)
        .execute(pool.writer())
        .await?;

        let applied_migrations = DataMigration::fetch_all(&pool).await?;
        debug!(?prev_version, %current_version, ?applied_migrations, "Current data migration status");
        // In the future, we can filter out migrations that are not required based on the version as well
        let required_migrations = data_migrations()
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
            .collect();

        tracing::debug!("required migrations: {:?}", required_migrations);

        Ok(Self {
            prev_version,
            required_migrations,
            pool,
        })
    }

    /// Determines whether migrations are needed. Will mark all migrations as "complete" on a fresh install.
    pub async fn needs_migration(&self) -> Result<bool> {
        let new_install = self.is_new_install().await?;

        if new_install {
            debug!("Setting up new node... data migrations are not required.");
            for migration_info in &self.required_migrations {
                DataMigration::insert_completed(&self.pool, migration_info.name).await?;
            }
            return Ok(false);
        }

        Ok(!self.required_migrations.is_empty())
    }

    /// In the future, we can check the version and run migrations based on that. For now, we need to know if this is just getting
    /// the new feature that tracks versions, or if it is a fresh install. We use the presence of recon event data to indicate it's not new.
    async fn is_new_install(&self) -> Result<bool> {
        if self.prev_version.is_none() {
            let row = sqlx::query(r#"SELECT cid as rowid from ceramic_one_event;"#)
                .fetch_optional(self.pool.reader())
                .await?;
            Ok(row.is_none())
        } else {
            Ok(false)
        }
    }

    /// Run all migrations that have not been run yet.
    pub async fn run_all(&self) -> Result<()> {
        for migration_info in &self.required_migrations {
            info!("Starting migration: {}", migration_info.name);
            DataMigration::upsert(&self.pool, migration_info.name).await?;
            self.run_migration_by_name(migration_info.name).await?;
            DataMigration::mark_completed(&self.pool, migration_info.name).await?;
        }
        Ok(())
    }

    async fn run_migration_by_name(&self, name: &str) -> Result<()> {
        let res = match name {
            "events_to_streams" => self.migrate_events_to_streams().await,
            _ => {
                return Err(Error::new_fatal(anyhow::anyhow!(
                    "Unknown migration: {}",
                    name
                )))
            }
        };
        match res {
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

    // This isn't the most efficient approach but it's simple and we should only run it once.
    // It isn't expected to ever be run on something that isn't a sqlite database upgrading from version 0.22.0 or below.
    async fn migrate_events_to_streams(&self) -> Result<()> {
        let mut cid_cursor = Some(0);

        struct EventToStreamRow {
            row_blocks: ReconEventBlockRaw,
            row_id: i64,
        }
        use sqlx::Row;
        impl sqlx::FromRow<'_, sqlx::sqlite::SqliteRow> for EventToStreamRow {
            fn from_row(row: &sqlx::sqlite::SqliteRow) -> std::result::Result<Self, sqlx::Error> {
                let row_id: i64 = row.try_get("rowid")?;
                let row_blocks = ReconEventBlockRaw::from_row(row)?;
                Ok(Self { row_id, row_blocks })
            }
        }

        let limit = 1000;
        let mut total_migrated = 0;
        while let Some(last_cid) = cid_cursor {
            let mut tx = self.pool.begin_tx().await?;

            // RowID starts from 1 so we can just use greater than 0 to start
            let all_blocks: Vec<EventToStreamRow> = sqlx::query_as(
            r#"WITH key AS (
                    SELECT
                        e.cid AS event_cid, e.order_key, e.rowid
                    FROM ceramic_one_event e
                    WHERE
                        EXISTS (
                            SELECT 1 
                            FROM ceramic_one_event_block 
                            WHERE event_cid = e.cid
                        )
                        AND NOT EXISTS (
                            SELECT 1 
                            FROM ceramic_one_event_metadata 
                            WHERE cid = e.cid
                        )
                        AND e.rowid > $1
                    ORDER BY e.rowid
                    LIMIT $2
                )
                SELECT
                    key.order_key, key.event_cid, eb.codec, eb.root, eb.idx, b.multihash, b.bytes, key.rowid
                FROM key
                JOIN ceramic_one_event_block eb ON key.event_cid = eb.event_cid
                JOIN ceramic_one_block b ON b.multihash = eb.block_multihash
                ORDER BY key.order_key, eb.idx;"#,
            )
            .bind(last_cid)
            .bind(limit)
            .fetch_all(&mut **tx.inner())
            .await?;

            let new_max = all_blocks.iter().map(|v| v.row_id).max();
            if all_blocks.is_empty() || new_max.is_none() {
                tx.commit().await?;
                return Ok(());
            } else {
                cid_cursor = new_max;
            }

            let values = ReconEventBlockRaw::into_carfiles(
                all_blocks.into_iter().map(|b| b.row_blocks).collect(),
            )
            .await?;
            total_migrated += values.len();
            if total_migrated % 10000 == 0 {
                debug!("Migrated {} events to the stream format", total_migrated);
            }
            tracing::trace!("found {} values to migrate", values.len());

            for (event_id, payload) in values {
                tracing::trace!("Migrating event: {}", event_id);
                // should we log and continue if anything fails? It shouldn't be possible unless something bad happened
                // and we allowed unexpected data into the system, but if we error, it will require manual intervention to recover
                // as the bad data will need to be deleted by hand. temporary failures to write can be retried.
                let event_cid = event_id.cid().ok_or_else(|| {
                    Error::new_fatal(anyhow::anyhow!("Event ID is missing a CID: {}", event_id))
                })?;

                let insertable = EventInsertableBody::try_from_carfile(event_cid, &payload).await?;

                if let EventHeader::Init { header, .. } = &insertable.header {
                    CeramicOneStream::insert_tx(&mut tx, insertable.cid, header).await?;
                }

                CeramicOneStream::insert_event_header_tx(&mut tx, &insertable.header).await?;
            }
            tx.commit().await?;
        }

        Ok(())
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

    async fn upsert(pool: &SqlitePool, name: &str) -> Result<()> {
        sqlx::query("INSERT INTO ceramic_one_data_migration (name, version) VALUES ($1, $2) on conflict (name) do update set last_attempted_at = CURRENT_TIMESTAMP;")
            .bind(name)
            .bind(env!("CARGO_PKG_VERSION"))
            .execute(pool.writer())
            .await?;
        Ok(())
    }

    async fn insert_completed(pool: &SqlitePool, name: &str) -> Result<()> {
        sqlx::query("INSERT INTO ceramic_one_data_migration (name, version, completed_at) VALUES ($1, $2, CURRENT_TIMESTAMP);")
            .bind(name)
            .bind(env!("CARGO_PKG_VERSION"))
            .execute(pool.writer())
            .await?;
        Ok(())
    }

    async fn mark_completed(pool: &SqlitePool, name: &str) -> Result<()> {
        sqlx::query("UPDATE ceramic_one_data_migration SET completed_at = CURRENT_TIMESTAMP WHERE name = $1;").bind(name).execute(pool.writer()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    #[tokio::test]
    async fn correctly_skips_new_install() {
        let pool = crate::sql::SqlitePool::connect_in_memory().await.unwrap();
        let migrator = crate::migration::DataMigrator::try_new(pool).await.unwrap();
        assert!(!migrator.needs_migration().await.unwrap());
    }
}
