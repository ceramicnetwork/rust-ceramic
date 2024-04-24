use anyhow::Result;
use sqlx::Row;

use crate::SqlitePool;

/// We use the RootStore as a local cache of the EthereumRootStore
///
/// The EthereumRootStore is the authoritative root store but it is also immutable  
/// once the blocks are final.
/// We only pull each tx_hash once and store the root, block_hash and timestamp.
/// After that Time Events validation can be done locally.
#[derive(Debug, Clone)]
pub struct RootStoreSqlite {
    pool: SqlitePool,
}

const PUT_QUERY: &str = r#"INSERT INTO ceramic_one_root (tx_hash, root, block_hash, timestamp)
VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING"#;

const GET_QUERY: &str = r#"SELECT timestamp FROM ceramic_one_root WHERE tx_hash = $1;"#;

impl RootStoreSqlite {
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
        let _ = sqlx::query(PUT_QUERY)
            .bind(tx_hash)
            .bind(root)
            .bind(block_hash)
            .bind(timestamp)
            .execute(self.pool.writer())
            .await?;
        Ok(())
    }

    /// Get the transaction timestamp from the roots table.
    pub async fn get(&self, tx_hash: &[u8]) -> Result<Option<i64>> {
        Ok(sqlx::query(GET_QUERY)
            .bind(tx_hash)
            .fetch_optional(self.pool.reader())
            .await?
            .map(|row| {
                let time: i64 = row.get(0);
                time
            }))
    }
}
