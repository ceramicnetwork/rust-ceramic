use cid::Cid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
pub enum EventType {
    Init,
    Data,
    Time,
}

#[derive(Debug, Clone)]
pub struct EventMetadataRow {}

impl EventMetadataRow {
    pub fn insert() -> &'static str {
        "INSERT INTO ceramic_one_event_metadata (cid, stream_cid, event_type, prev) VALUES ($1, $2, $3, $4)"
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// An event header wrapper for use in the store crate.
/// TODO: replace this with something from the event crate
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
    /// Returns the event type of the event header
    pub(crate) fn event_type(&self) -> EventType {
        match self {
            EventHeader::Init { .. } => EventType::Init,
            EventHeader::Data { .. } => EventType::Data,
            EventHeader::Time { .. } => EventType::Time,
        }
    }

    /// Returns the stream CID of the event
    pub(crate) fn stream_cid(&self) -> Cid {
        match self {
            EventHeader::Init { cid, .. } => *cid,
            EventHeader::Data { stream_cid, .. } | EventHeader::Time { stream_cid, .. } => {
                *stream_cid
            }
        }
    }
}
