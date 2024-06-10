use cid::Cid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
pub enum EventType {
    Init,
    Data,
    Time,
}

#[derive(Debug, Clone)]
pub struct EventHeaderRow {
    pub cid: Vec<u8>,
    pub stream_cid: Vec<u8>,
    pub event_type: EventType,
    pub prev: Option<Vec<u8>>,
}

impl EventHeaderRow {
    pub fn insert() -> &'static str {
        "INSERT INTO ceramic_one_event_header (cid, stream_cid, event_type, prev) VALUES ($1, $2, $3, $4)"
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventHeader {
    Init {
        cid: Cid,
        header: ceramic_event::unvalidated::init::Header,
    },
    Data {
        cid: Cid,
        stream_cid: Cid,
        prev: Cid,
    },
    Time {
        cid: Cid,
        stream_cid: Cid,
        prev: Cid,
    },
}

impl EventHeader {
    pub fn event_type(&self) -> EventType {
        match self {
            EventHeader::Init { .. } => EventType::Init,
            EventHeader::Data { .. } => EventType::Data,
            EventHeader::Time { .. } => EventType::Time,
        }
    }

    pub fn stream_cid(&self) -> Cid {
        match self {
            EventHeader::Init { cid, .. } => *cid,
            EventHeader::Data { stream_cid, .. } | EventHeader::Time { stream_cid, .. } => {
                *stream_cid
            }
        }
    }
}
