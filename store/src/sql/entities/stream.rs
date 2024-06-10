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
pub struct StreamCommitRow {
    pub cid: Vec<u8>,
    pub prev: Option<Vec<u8>>,
    pub delivered: bool,
}

impl StreamCommitRow {
    pub fn fetch_by_stream_cid() -> &'static str {
        r#"
        SELECT eh.cid as "cid", eh.prev as "prev", 
            e.delivered IS NOT NULL as "delivered" 
        FROM ceramic_one_stream s
            JOIN ceramic_one_event_header eh on eh.stream_cid = s.cid
            JOIN ceramic_one_event e on e.cid = eh.cid
        WHERE s.cid = $1"#
    }
}
