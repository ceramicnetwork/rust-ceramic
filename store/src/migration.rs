use std::sync::OnceLock;

use sqlx::{prelude::FromRow, types::chrono};
use tracing::{info};

use crate::{
    sql::{
        entities::{EventCid, EventHeader, ReconEventBlockRaw},
    },
    CeramicOneStream, Error, EventInsertableBody, Result, SqlitePool,
};

static MIGRATIONS: OnceLock<Vec<Migration>> = OnceLock::new();

struct Migration {
    /// The name of the migration.
    name: &'static str,
    /// The version this migration was released (anything below this should be migrated).
    _version: &'static str,
}

/// The list of migrations that need to be run in order to get the database up to date and start the server.
/// Add new migrations to the end of the list.
fn required_migrations() -> &'static Vec<Migration> {
    MIGRATIONS.get_or_init(|| {
        vec![Migration {
            name: "events_to_streams",
            _version: "0.23.0",
        }]
    })
}

#[derive(Debug, Clone, sqlx::FromRow)]
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
        // In the future, we can filter out migrations that are not required based on the version as well
        let required_migrations = required_migrations()
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
            info!("Setting up new node... data migrations are not required.");
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
            let x = sqlx::query(r#"SELECT cid from ceramic_one_event limit 1;"#)
                .fetch_optional(self.pool.reader())
                .await?;
            Ok(x.is_some())
        } else {
            Ok(false)
        }
    }

    /// Run all migrations that have not been run yet.
    pub async fn run_all(&self) -> Result<()> {
        for migration_info in &self.required_migrations {
            DataMigration::upsert(&self.pool, migration_info.name).await?;
            self.run_migration_by_name(migration_info.name).await?;
            DataMigration::mark_completed(&self.pool, migration_info.name).await?;
        }
        Ok(())
    }

    async fn run_migration_by_name(&self, name: &str) -> Result<()> {
        info!("Starting migration: {}", name);
        match name {
            "events_to_streams" => self.migrate_events_to_streams().await,
            _ => Err(Error::new_fatal(anyhow::anyhow!(
                "Unknown migration: {}",
                name
            ))),
        }
    }

    // This isn't the most efficient approach but it's simple and we should only run it once.
    // It isn't expected to ever be run on something that isn't a sqlite database upgrading from version 0.22.0 or below.
    async fn migrate_events_to_streams(&self) -> Result<()> {
        let mut cid_cursor = Some(EventCid::default());

        while let Some(last_cid) = cid_cursor {
            cid_cursor = self.migrate_events_to_streams_batch(last_cid).await?;
        }

        Ok(())
    }

    async fn migrate_events_to_streams_batch(
        &self,
        last_cid: EventCid,
    ) -> Result<Option<EventCid>> {
        let all_blocks: Vec<ReconEventBlockRaw> = sqlx::query_as(
            r#"SELECT
            key.order_key, key.event_cid, eb.codec, eb.root, eb.idx, b.multihash, b.bytes
        FROM (
            SELECT
                e.cid as event_cid, e.order_key
            FROM ceramic_one_event e
            WHERE
                EXISTS (SELECT 1 FROM ceramic_one_event_block where event_cid = e.cid)
                AND NOT EXISTS (SELECT 1 from ceramic_one_event_metadata where cid = e.cid)
                AND e.cid > $1
            ORDER BY e.cid
            LIMIT 1000
        ) key
        JOIN
            ceramic_one_event_block eb ON key.event_cid = eb.event_cid
        JOIN ceramic_one_block b on b.multihash = eb.block_multihash
            ORDER BY key.order_key, eb.idx;"#,
        )
        .bind(last_cid.to_bytes())
        .fetch_all(self.pool.reader())
        .await?;

        let values = ReconEventBlockRaw::into_carfiles(all_blocks).await?;

        let last_cid = values.last().and_then(|(id, _)| id.cid());
        if last_cid.is_none() {
            return Ok(None);
        }
        let mut tx = self.pool.begin_tx().await?;
        for (event_id, payload) in values {
            // should we log and continue? this shouldn't be possible unless something bad happened
            // if we error, will require manual intervention to recover
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

        Ok(last_cid)
    }
}

#[derive(Debug, Clone, FromRow)]
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
