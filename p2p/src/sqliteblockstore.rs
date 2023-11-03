use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use cid::{
    multihash::Code::{Keccak256, Sha2_256},
    multihash::MultihashDigest,
    Cid,
};
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
        // this will need to be moved to migration logic if we ever change the schema
        sqlx::query(
            // the comments are in the CREATE TABLE statement so that it will be in the SQLiteDB
            // a human looking at the schema will see the CREATE TABLE statement
            "
        CREATE TABLE IF NOT EXISTS blocks (
            multihash BLOB, -- the multihash of the Block as bytes no 0x00 prefix
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
        Ok(sqlx::query("SELECT bytes FROM blocks WHERE multihash = ?;")
            .bind(cid.hash().to_bytes())
            .fetch_optional(&self.pool)
            .await?
            .map(|row| row.get::<'_, Vec<u8>, _>(0).into()))
    }

    /// Store a DAG node into IPFS.
    /// Reports true when the block does not previously exist in the store.
    pub async fn put(&self, cid: Cid, blob: Bytes, _links: Vec<Cid>) -> Result<bool> {
        let hash = match cid.hash().code() {
            0x12 => Sha2_256.digest(&blob),
            0x1b => Keccak256.digest(&blob),
            0x11 => return Err(anyhow!("Sha1 not supported")),
            _ => {
                return Err(anyhow!(
                    "multihash type {:#x} not Sha2_256, Keccak256",
                    cid.hash().code(),
                ))
            }
        };
        if cid.hash().to_bytes() != hash.to_bytes() {
            return Err(anyhow!(
                "cid did not match blob {} != {}",
                hex::encode(cid.hash().to_bytes()),
                hex::encode(hash.to_bytes())
            ));
        }

        match sqlx::query("INSERT INTO blocks (multihash, bytes) VALUES (?, ?)")
            .bind(cid.hash().to_bytes())
            .bind(blob.to_vec())
            .execute(&self.pool)
            .await
        {
            Ok(_) => Ok(true),
            Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    /// merge_from_sqlite takes the filepath to a sqlite file.
    /// If the file dose not exist the ATTACH DATABASE command will create it.
    /// This function assumes that the database contains a table named blocks with multihash, bytes columns.
    pub async fn merge_from_sqlite(&self, input_ceramic_db_filename: &str) -> Result<()> {
        sqlx::query(
            "
                ATTACH DATABASE ? AS other;
                INSERT OR IGNORE INTO blocks SELECT multihash, bytes FROM other.blocks;
            ",
        )
        .bind(input_ceramic_db_filename)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Backup the database to a filepath output_ceramic_db_filename.
    pub async fn backup_to_sqlite(&self, output_ceramic_db_filename: &str) -> Result<()> {
        sqlx::query(".backup ?")
            .bind(output_ceramic_db_filename)
            .execute(&self.pool)
            .await?;
        Ok(())
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
        Ok(Block::new(
            sqlx::query("SELECT bytes FROM blocks WHERE multihash = ?;")
                .bind(cid.hash().to_bytes())
                .fetch_one(&self.pool)
                .await?
                .get::<'_, Vec<u8>, _>(0)
                .into(),
            cid.to_owned(),
        ))
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
    use iroh_bitswap::Store;
    use sqlx::SqlitePool;

    #[tokio::test]
    async fn test_store_block() {
        let blob: Bytes = hex::decode("0a050001020304").unwrap().into();
        let cid: CidGeneric<64> =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line

        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let store: SQLiteBlockStore = SQLiteBlockStore::new(pool).await.unwrap();

        let result = store.put(cid, blob, vec![]).await;
        // Assert that the block is new
        assert!(result.unwrap());

        let has: Result<bool, Error> = Store::has(&store, &cid).await;
        expect![["true"]].assert_eq(&has.unwrap().to_string());

        let size: Result<usize, Error> = Store::get_size(&store, &cid).await;
        expect![["7"]].assert_eq(&size.unwrap().to_string());

        let block = Store::get(&store, &cid).await.unwrap();
        expect!["bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom"]
            .assert_eq(&block.cid().to_string());
        expect![["0A050001020304"]].assert_eq(&hex::encode_upper(block.data()));
    }

    #[tokio::test]
    async fn test_double_store_block() {
        let blob: Bytes = hex::decode("0a050001020304").unwrap().into();
        let cid: CidGeneric<64> =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line

        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let store: SQLiteBlockStore = SQLiteBlockStore::new(pool).await.unwrap();

        let result = store.put(cid, blob.clone(), vec![]).await;
        // Assert that the block is new
        assert!(result.unwrap());

        // Try to put the block again
        let result = store.put(cid, blob, vec![]).await;
        // Assert that the block already existed
        assert!(!result.unwrap());

        let has: Result<bool, Error> = Store::has(&store, &cid).await;
        expect![["true"]].assert_eq(&has.unwrap().to_string());

        let size: Result<usize, Error> = Store::get_size(&store, &cid).await;
        expect![["7"]].assert_eq(&size.unwrap().to_string());

        let block = Store::get(&store, &cid).await.unwrap();
        expect!["bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom"]
            .assert_eq(&block.cid().to_string());
        expect![["0A050001020304"]].assert_eq(&hex::encode_upper(block.data()));
    }

    #[tokio::test]
    async fn test_get_nonexistant_block() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let store: SQLiteBlockStore = SQLiteBlockStore::new(pool).await.unwrap();

        let cid =
            Cid::from_str("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(); // cspell:disable-line

        let block = store.get(cid).await.unwrap();
        assert_eq!(None, block);
    }
}
