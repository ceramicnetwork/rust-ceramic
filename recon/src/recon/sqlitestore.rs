#![warn(missing_docs, missing_debug_implementations, clippy::all)]

use crate::{AssociativeHash, Key, Store};
use anyhow::anyhow;
use rusqlite::{Connection, ErrorCode, Result};
use std::{marker::PhantomData, path::Path};
use tracing::{debug, instrument};

/// ReconSQLite is a implementation of Recon store
#[derive(Debug)]
pub struct SQLiteStore<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    key: PhantomData<K>,
    hash: PhantomData<H>,
    conn: Connection,
    sort_key: String,
}

impl<K, H> SQLiteStore<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    /// make a new SQLiteStore from a connection and sort_key
    pub fn new(conn: Connection, sort_key: String) -> Self {
        SQLiteStore {
            conn,
            sort_key,
            key: PhantomData,
            hash: PhantomData,
        }
    }

    /// create a SQLite Connection to the provided path
    pub fn conn_for_filename<P>(path: P) -> Result<Connection>
    where
        P: AsRef<Path>,
    {
        let conn = Connection::open(path)?;

        // set the WAL PRAGMA for faster writes
        const SET_WAL_PRAGMA: &str = "PRAGMA journal_mode=WAL;";
        conn.execute(SET_WAL_PRAGMA, ())
            .expect("todo: what do we want to do when the database file fails?");

        Ok(conn)
    }
}

impl<K, H> Default for SQLiteStore<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    fn default() -> Self {
        SQLiteStore {
            conn: Connection::open_in_memory().unwrap(),
            sort_key: "model".to_owned(),
            key: PhantomData,
            hash: PhantomData,
        }
    }
}

impl<K, H> SQLiteStore<K, H>
where
    K: Key,
    H: AssociativeHash + std::convert::From<[u32; 8]>,
{
    /// init the recon table using the connection
    pub fn create_table(&mut self) {
        const CREATE_RECON_TABLE: &str = "CREATE TABLE IF NOT EXISTS recon (
            sort_key TEXT, -- the field in the event header to sort by e.g. model
            key BLOB, -- network_id sort_value controller StreamID height event_cid
            ahash_0 INTEGER, -- the ahash is decomposed as [u32; 8]
            ahash_1 INTEGER,
            ahash_2 INTEGER,
            ahash_3 INTEGER,
            ahash_4 INTEGER,
            ahash_5 INTEGER,
            ahash_6 INTEGER,
            ahash_7 INTEGER,
            CID TEXT,
            block_retrieved BOOL, -- indicates if we still want the block
            PRIMARY KEY(sort_key, key)
        )";

        self.conn
            .execute(
                CREATE_RECON_TABLE,
                (), // empty list of parameters.
            )
            .unwrap();
    }
}

impl<K, H> Store for SQLiteStore<K, H>
where
    K: Key + From<Vec<u8>>,
    H: AssociativeHash,
{
    type Key = K;
    type Hash = H;

    // Ok(true): inserted the key
    // Ok(false): did not insert the key ConstraintViolation
    // Err(e): sql error
    #[instrument(skip(self), ret)]
    fn insert(&mut self, key: &Self::Key) -> anyhow::Result<bool> {
        let hash = H::digest(key);

        let resp = self.conn.execute(
            "INSERT INTO recon (
                sort_key, key,
                ahash_0, ahash_1, ahash_2, ahash_3,
                ahash_4, ahash_5, ahash_6, ahash_7,
                block_retrieved
            ) VALUES (
                ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?
            )",
            (
                &self.sort_key,
                key.as_bytes(),
                hash.as_u32s()[0],
                hash.as_u32s()[1],
                hash.as_u32s()[2],
                hash.as_u32s()[3],
                hash.as_u32s()[4],
                hash.as_u32s()[5],
                hash.as_u32s()[6],
                hash.as_u32s()[7],
                false,
            ),
        );
        match resp {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.sqlite_error_code() == Some(ErrorCode::ConstraintViolation) {
                    Ok(false)
                } else {
                    Err(anyhow!("SQL Error: {}", e))
                }
            }
        }
    }

    #[instrument(skip(self), ret)]
    fn hash_range(&self, left_fencepost: &Self::Key, right_fencepost: &Self::Key) -> Self::Hash {
        let query = "
        SELECT
            TOTAL(ahash_0) & 0xFFFFFFFF, TOTAL(ahash_1) & 0xFFFFFFFF,
            TOTAL(ahash_2) & 0xFFFFFFFF, TOTAL(ahash_3) & 0xFFFFFFFF,
            TOTAL(ahash_4) & 0xFFFFFFFF, TOTAL(ahash_5) & 0xFFFFFFFF,
            TOTAL(ahash_6) & 0xFFFFFFFF, TOTAL(ahash_7) & 0xFFFFFFFF
        FROM
            recon
        WHERE
            sort_key = ? AND
            key > ? AND key < ?;
        ";
        let mut stmt = self.conn.prepare(query).unwrap();
        let mut rows = stmt
            .query((
                &self.sort_key,
                left_fencepost.as_bytes(),
                right_fencepost.as_bytes(),
            ))
            .unwrap();
        let row = rows.next().unwrap().unwrap();
        let bytes: [u32; 8] = [
            row.get(0).unwrap(),
            row.get(1).unwrap(),
            row.get(2).unwrap(),
            row.get(3).unwrap(),
            row.get(4).unwrap(),
            row.get(5).unwrap(),
            row.get(6).unwrap(),
            row.get(7).unwrap(),
        ];
        H::from(bytes)
    }

    #[instrument(skip(self))]
    fn range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Box<dyn Iterator<Item = Self::Key> + '_> {
        debug!(
            self.sort_key,
            left_fencepost = left_fencepost.to_hex(),
            right_fencepost = right_fencepost.to_hex(),
            offset,
            limit,
            "range"
        );
        let query = "
        SELECT
            key
        FROM
            recon
        WHERE
            sort_key = ? AND
            key > ? AND key < ?
        ORDER BY
            key ASC
        LIMIT
            ?
        OFFSET
            ?;
        ";
        let mut stmt = self.conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(
                (
                    &self.sort_key,
                    left_fencepost.as_bytes(),
                    right_fencepost.as_bytes(),
                    limit as i64,
                    offset as i64,
                ),
                |row| -> Result<Self::Key, rusqlite::Error> {
                    Ok(K::from(row.get::<usize, Vec<u8>>(0).unwrap()))
                },
            )
            .unwrap()
            .map(|row| row.unwrap().clone());
        let rows = rows.collect::<Vec<Self::Key>>();
        debug!(count = rows.len(), "rows");
        Box::new(rows.into_iter())
    }
    /// Return the number of keys within the range.
    #[instrument(skip(self), ret)]
    fn count(&self, left_fencepost: &Self::Key, right_fencepost: &Self::Key) -> usize {
        let query = "
        SELECT
            count(key)
        FROM
            recon
        WHERE
            sort_key = ? AND
            key > ? AND key < ?
        ; ";
        let mut stmt = self.conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(
                (
                    &self.sort_key,
                    left_fencepost.as_bytes(),
                    right_fencepost.as_bytes(),
                ),
                |row| -> Result<usize, rusqlite::Error> { Ok(row.get::<usize, usize>(0).unwrap()) },
            )
            .unwrap()
            .map(|row| row.unwrap());
        let rows = rows.collect::<Vec<usize>>();
        rows.first().unwrap().to_owned()
    }

    /// Return the first key within the range.
    #[instrument(skip(self), ret)]
    fn first(&self, left_fencepost: &Self::Key, right_fencepost: &Self::Key) -> Option<Self::Key> {
        let query = "
    SELECT
        key
    FROM
        recon
    WHERE
        sort_key = ? AND
        key > ? AND key < ?
    ORDER BY
        key ASC
    LIMIT
        1
    ; ";
        let mut stmt = self.conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(
                (
                    &self.sort_key,
                    left_fencepost.as_bytes(),
                    right_fencepost.as_bytes(),
                ),
                |row| -> Result<Self::Key, rusqlite::Error> {
                    Ok(K::from(row.get::<usize, Vec<u8>>(0).unwrap()))
                },
            )
            .unwrap()
            .map(|row| row.unwrap().clone());
        let rows = rows.collect::<Vec<Self::Key>>();
        rows.get(0).cloned()
    }

    #[instrument(skip(self), ret)]
    fn last(&self, left_fencepost: &Self::Key, right_fencepost: &Self::Key) -> Option<Self::Key> {
        let query = "
        SELECT
            key
        FROM
            recon
        WHERE
            sort_key = ? AND
            key > ? AND key < ?
        ORDER BY
            key DESC
        LIMIT
            1
        ; ";
        let mut stmt = self.conn.prepare(query).unwrap();
        let rows = stmt
            .query_map(
                (
                    &self.sort_key,
                    left_fencepost.as_bytes(),
                    right_fencepost.as_bytes(),
                ),
                |row| -> Result<Self::Key, rusqlite::Error> {
                    Ok(K::from(row.get::<usize, Vec<u8>>(0).unwrap()))
                },
            )
            .unwrap()
            .map(|row| row.unwrap().clone());
        let rows = rows.collect::<Vec<Self::Key>>();
        rows.get(0).cloned()
    }

    #[instrument(skip(self), ret)]
    fn first_and_last(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Option<(Self::Key, Self::Key)> {
        let first = self.first(left_fencepost, right_fencepost);
        if let Some(first) = first {
            let last = self.last(left_fencepost, right_fencepost);
            if let Some(last) = last {
                Some((first, last))
            } else {
                Some((first.clone(), first))
            }
        } else {
            None
        }
    }

    fn middle(&self, left_fencepost: &Self::Key, right_fencepost: &Self::Key) -> Option<Self::Key> {
        {
            let count = self.count(left_fencepost, right_fencepost);
            self.range(left_fencepost, right_fencepost, (count - 1) / 2, 1)
                .next()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SQLiteStore;
    use crate::{Sha256a, Store};
    use ceramic_core::Bytes;
    use expect_test::expect;

    #[test]
    fn test_hash_range_query() {
        let mut recon: SQLiteStore<Bytes, Sha256a> = SQLiteStore::default();
        recon.create_table();
        let _ = recon.insert(&b"hello".as_slice().into());
        let _ = recon.insert(&b"world".as_slice().into());
        let hash: Sha256a = recon.hash_range(&b"a".as_slice().into(), &b"z".as_slice().into());
        expect![[r#"7460F21C83815F5EDC682F7A4154BC09AA3A0AE5DD1A2DEDCD709888A12751CC"#]]
            .assert_eq(&hash.to_string())
    }

    #[test]
    fn test_range_query() {
        let mut recon: SQLiteStore<Bytes, Sha256a> = SQLiteStore::default();
        recon.create_table();
        let _ = recon.insert(&b"hello".as_slice().into());
        let _ = recon.insert(&b"world".as_slice().into());
        let ids = recon.range(
            &b"a".as_slice().into(),
            &b"z".as_slice().into(),
            0,
            usize::MAX,
        );
        expect![[r#"
        [
            Bytes(
                "hello",
            ),
            Bytes(
                "world",
            ),
        ]
        "#]]
        .assert_debug_eq(&ids.collect::<Vec<Bytes>>());
    }

    #[test]
    fn test_double_insert() {
        let mut recon: SQLiteStore<Bytes, Sha256a> = SQLiteStore::default();
        recon.create_table();

        // do take the first one
        expect![
            r#"
        Ok(
            true,
        )
        "#
        ]
        .assert_debug_eq(&recon.insert(&b"hello".as_slice().into()));

        // reject the second insert of same key
        expect![
            r#"
        Ok(
            false,
        )
        "#
        ]
        .assert_debug_eq(&recon.insert(&b"hello".as_slice().into()));
    }
}
