use cid::Cid;
use sqlx::{sqlite::SqliteRow, Row};

use super::StreamCid;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct StreamRow {
    pub cid: Vec<u8>,
    pub sep: String,
    pub sep_val: Vec<u8>,
}

impl StreamRow {
    pub fn insert() -> &'static str {
        "INSERT INTO ceramic_one_stream (cid, sep, sep_value) VALUES ($1, $2, $3) returning cid"
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct StreamEventRow {
    pub cid: Vec<u8>,
    pub prev: Option<Vec<u8>>,
    pub deliverable: bool,
}

impl StreamEventRow {
    /// Requires binding one argument:
    ///     $1 = stream_cid (bytes)
    pub fn fetch_by_stream_cid() -> &'static str {
        r#"
        SELECT e.cid as "cid", eh.prev as "prev", 
            e.delivered IS NOT NULL as "deliverable" 
        FROM ceramic_one_stream s
            JOIN ceramic_one_event_metadata eh on eh.stream_cid = s.cid
            JOIN ceramic_one_event e on e.cid = eh.cid
        WHERE s.cid = $1"#
    }
}

#[derive(Debug, Clone)]
pub struct IncompleteStream {
    pub stream_cid: StreamCid,
    pub row_id: i64,
}

impl IncompleteStream {
    /// Requires binding two arguments:
    ///     $1 = highwater mark (i64)
    ///     $2 = limit (usize)
    pub fn fetch_all_with_undelivered() -> &'static str {
        r#"
        SELECT DISTINCT s.cid as "stream_cid", s.rowid 
        FROM ceramic_one_stream s
            JOIN ceramic_one_event_metadata eh on eh.stream_cid = s.cid
            JOIN ceramic_one_event e on e.cid = eh.cid
        WHERE e.delivered is NULL and s.rowid > $1
        LIMIT $2"#
    }
}

impl sqlx::FromRow<'_, SqliteRow> for IncompleteStream {
    fn from_row(row: &SqliteRow) -> sqlx::Result<Self> {
        let cid: Vec<u8> = row.try_get("stream_cid")?;
        let row_id: i64 = row.try_get("rowid")?;
        let stream_cid = Cid::try_from(cid).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        Ok(Self { stream_cid, row_id })
    }
}
