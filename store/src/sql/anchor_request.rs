use std::borrow::Cow;

use cid::Cid;
use sqlx::{
    encode::IsNull,
    error::BoxDynError,
    sqlite::{SqliteArgumentValue, SqliteTypeInfo, SqliteValueRef},
    Decode, Encode, FromRow, Sqlite, Type,
};

use crate::{DbTxSqlite, Error, Result, SqliteEventStore};

const PUT_QUERY: &str =
    r#"INSERT INTO ceramic_one_anchor_request (cid) VALUES ($1) ON CONFLICT DO NOTHING"#;

const GET_QUERY: &str =
    r#"SELECT (cid, detached_time_event_cid) FROM ceramic_one_anchor_request WHERE cid = $1;"#;

const SCAN_QUERY: &str =
    r#"SELECT cid FROM ceramic_one_anchor_request WHERE detached_time_event_cid IS NULL LIMIT $1;"#;

const UPDATE_QUERY: &str =
    r#"UPDATE ceramic_one_anchor_request detached_time_event_cid = $1 WHERE cid = $2"#;

#[derive(Debug)]
pub struct CidBlob(Cid);

impl Type<Sqlite> for CidBlob {
    fn type_info() -> SqliteTypeInfo {
        <&[u8] as Type<Sqlite>>::type_info()
    }

    fn compatible(ty: &SqliteTypeInfo) -> bool {
        <&[u8] as Type<Sqlite>>::compatible(ty)
    }
}

impl<'q> Encode<'q, Sqlite> for CidBlob {
    fn encode(self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        args.push(SqliteArgumentValue::Blob(Cow::Owned(self.0.to_bytes())));

        IsNull::No
    }

    fn encode_by_ref(&self, args: &mut Vec<SqliteArgumentValue<'q>>) -> IsNull {
        args.push(SqliteArgumentValue::Blob(Cow::Owned(self.0.to_bytes())));

        IsNull::No
    }
}

impl<'r> Decode<'r, Sqlite> for CidBlob {
    fn decode(value: SqliteValueRef<'r>) -> std::result::Result<CidBlob, BoxDynError> {
        let v: &[u8] = <&[u8] as Decode<Sqlite>>::decode(value)?;
        let cid = Cid::try_from(v)?;
        Ok(CidBlob(cid))
    }
}

#[derive(Debug, FromRow)]
struct TimeEventRow {
    cid: CidBlob,
    detached_time_event_cid: Option<CidBlob>,
}

#[derive(Debug, FromRow)]
struct CidRow {
    cid: CidBlob,
}

impl SqliteEventStore {
    /// Store CID
    pub async fn put_anchor_request(&self, cid: &Cid, conn: &mut DbTxSqlite<'_>) -> Result<()> {
        let _ = sqlx::query(PUT_QUERY)
            .bind(cid.to_bytes())
            .execute(&mut **conn)
            .await?;
        Ok(())
    }

    /// Get the CID and Detached Time Event CID from the anchor request table
    pub(crate) async fn get_anchor_request(&self, cid: &Cid) -> Result<Option<Cid>> {
        let time_event_row: TimeEventRow = sqlx::query_as(GET_QUERY)
            .bind(cid.to_bytes())
            .fetch_one(self.pool.reader())
            .await
            .map_err(Error::from)?;
        Ok(time_event_row.detached_time_event_cid.map(|cid| cid.0))
    }

    /// Scan the anchor request table for all unanchored CIDs
    pub async fn scan_anchor_requests(&self, limit: usize) -> Result<Vec<Cid>> {
        let rows: Vec<CidRow> = sqlx::query_as(SCAN_QUERY)
            .bind(limit as i64)
            .fetch_all(self.pool.reader())
            .await
            .map_err(Error::from)?;
        Ok(rows.into_iter().map(|row| row.cid.0).collect::<Vec<Cid>>())
    }

    /// Update the Detached Time Event CID for an anchored CID
    pub async fn update_anchor_request(
        &self,
        cid: &Cid,
        detached_time_event_cid: &Cid,
    ) -> Result<()> {
        let _ = sqlx::query(UPDATE_QUERY)
            .bind(detached_time_event_cid.to_bytes())
            .bind(cid.to_bytes())
            .execute(self.pool.writer())
            .await?;
        Ok(())
    }
}
