use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use cid::{multihash::Code::Sha2_256, multihash::MultihashDigest, Cid};
use iroh_bitswap::{Block, Store};
use sqlx::{Row, SqlitePool};

#[derive(Debug, Clone)]
pub struct SQLiteBlockStore {
    pool: SqlitePool,
}

impl SQLiteBlockStore {
    /// ```sql
    /// CREATE TABLE IF NOT EXISTS blocks (
    ///     multihash BLOB, -- the CID of the Block
    ///     bytes BLOB, -- the Block
    ///     PRIMARY KEY(multihash)
    /// )
    /// ```
    pub async fn new(pool: SqlitePool) -> Result<Self> {
        let store = SQLiteBlockStore { pool };
        store.create_table_if_not_exists().await?;
        Ok(store)
    }

    async fn create_table_if_not_exists(&self) -> Result<()> {
        sqlx::query(
            "
        CREATE TABLE IF NOT EXISTS blocks (
            multihash BLOB, -- the CID of the Block
            bytes BLOB, -- the Block
            PRIMARY KEY(multihash)
        );
        ",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_size(&self, cid: Cid) -> Result<Option<u64>> {
        Ok(Some(
            sqlx::query("SELECT length(bytes) FROM blocks WHERE multihash = ?;")
                .bind(cid.hash().to_bytes())
                .fetch_one(&self.pool)
                .await?
                .get::<'_, i64, _>(0) as u64,
        ))
    }

    pub async fn get(&self, cid: Cid) -> Result<Option<Bytes>> {
        Ok(Some(
            sqlx::query("SELECT bytes FROM blocks WHERE multihash = ?;")
                .bind(cid.hash().to_bytes())
                .fetch_one(&self.pool)
                .await?
                .get::<'_, Vec<u8>, _>(0)
                .into(),
        ))
    }

    /// Store a DAG node into IPFS.
    pub async fn put(&self, cid: Cid, blob: Bytes, _links: Vec<Cid>) -> Result<()> {
        let hash = Sha2_256.digest(&blob);
        if cid.hash().to_bytes() != hash.to_bytes() {
            return Err(anyhow!("cid did not match blob {} != {:?}", cid, hash));
        }

        match sqlx::query("INSERT INTO blocks (multihash, bytes) VALUES (?, ?)")
            .bind(cid.hash().to_bytes())
            .bind(blob.to_vec())
            .execute(&self.pool)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if e.as_database_error().unwrap().is_unique_violation() => Ok(()),
            Err(e) => Err(e)?,
        }
    }
}

#[async_trait]
impl Store for SQLiteBlockStore {
    /// ```sql
    /// SELECT length(bytes) FROM blocks WHERE multihash = ?;
    /// ```
    async fn get_size(&self, cid: &Cid) -> Result<usize> {
        Ok(
            sqlx::query("SELECT length(bytes) FROM blocks WHERE multihash = ?;")
                .bind(cid.hash().to_bytes())
                .fetch_one(&self.pool)
                .await?
                .get::<'_, i64, _>(0) as usize,
        )
    }

    /// ```sql
    /// SELECT bytes FROM blocks WHERE multihash = ?;
    /// ```
    async fn get(&self, cid: &Cid) -> Result<Block> {
        Ok(Block::from_v0_data(
            sqlx::query("SELECT bytes FROM blocks WHERE multihash = ?;")
                .bind(cid.hash().to_bytes())
                .fetch_one(&self.pool)
                .await?
                .get::<'_, Vec<u8>, _>(0)
                .into(),
        )?)
    }

    /// ```sql
    /// SELECT count(1) FROM blocks WHERE multihash = ?;
    /// ```
    async fn has(&self, cid: &Cid) -> Result<bool> {
        Ok(
            sqlx::query("SELECT count(1) FROM blocks WHERE multihash = ?;")
                .bind(cid.hash().to_bytes())
                .fetch_one(&self.pool)
                .await?
                .get::<'_, i64, _>(0)
                > 0,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::SQLiteBlockStore;
    use anyhow::Error;
    use bytes::Bytes;
    use cid::{Cid, CidGeneric};
    use expect_test::expect;
    use iroh_bitswap::{Block, Store};
    use sqlx::SqlitePool;

    #[tokio::test]
    async fn test_store_block() {
        let blob: Bytes = hex::decode("0a050001020304").unwrap().into();
        let cid: CidGeneric<64> =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line

        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let store: SQLiteBlockStore = SQLiteBlockStore::new(pool).await.unwrap();

        let result = store.put(cid, blob, vec![]).await;
        result.unwrap();

        let has: Result<bool, Error> = Store::has(&store, &cid).await;
        expect![["true"]].assert_eq(&has.unwrap().to_string());

        let size: Result<usize, Error> = Store::get_size(&store, &cid).await;
        expect![["7"]].assert_eq(&size.unwrap().to_string());

        let block: Result<Block, Error> = Store::get(&store, &cid).await;
        expect![["0A050001020304"]].assert_eq(&hex::encode_upper(block.unwrap().data()));
    }
}
