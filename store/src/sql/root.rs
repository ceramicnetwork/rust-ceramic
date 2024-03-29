use anyhow::Result;

use crate::SqlitePool;

/// We use the RootStore as a local cache of the EthereumRootStore
///
/// The EthereumRootStore is the authoritative root store but it is also immutable  
/// once the blocks are final.
/// We only pull each tx_hash once and store the root, block_hash and timestamp.
/// After that Time Events validation can be done locally.
#[derive(Debug, Clone)]
pub struct RootStore {
    pool: SqlitePool,
}

impl RootStore {
    /// Create a new RootStore
    pub async fn new(pool: SqlitePool) -> Result<Self> {
        let store = Self { pool };
        Ok(store)
    }

    /// Store tx_hash, root, and timestamp in roots.
    pub async fn put(
        &self,
        tx_hash: &[u8],
        root: &[u8],
        block_hash: String, // 0xhex_hash
        timestamp: i64,
    ) -> Result<()> {
        match sqlx::query!(
            r#"INSERT OR IGNORE INTO "root" (tx_hash, root, block_hash, timestamp) VALUES (?, ?, ?, ?)"#,
            tx_hash,
            root,
            block_hash,
            timestamp,
        )
        .execute(self.pool.writer())
        .await
        {
            Ok(_) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    /// Get the transaction timestamp from the roots table.
    pub async fn get(&self, tx_hash: &[u8]) -> Result<Option<i64>> {
        Ok(sqlx::query!(
            r#"SELECT timestamp FROM "root" WHERE tx_hash = ?;"#,
            tx_hash
        )
        .fetch_optional(self.pool.reader())
        .await?
        .map(|row| row.timestamp))
    }
}
