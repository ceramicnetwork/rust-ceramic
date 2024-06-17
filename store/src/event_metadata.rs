use cid::Cid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum EventType {
    Init,
    Data,
    Time,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// An event header wrapper for use in the store crate.
/// TODO: replace this with something from the event crate
#[allow(missing_docs)]
pub enum EventHeader {
    Init {
        cid: Cid,
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
    pub fn event_type(&self) -> EventType {
        match self {
            EventHeader::Init { .. } => EventType::Init,
            EventHeader::Data { .. } => EventType::Data,
            EventHeader::Time { .. } => EventType::Time,
        }
    }

    /// Returns the stream CID of the event
    pub fn stream_cid(&self) -> Cid {
        match self {
            EventHeader::Init { cid, .. } => *cid,
            EventHeader::Data { stream_cid, .. } | EventHeader::Time { stream_cid, .. } => {
                *stream_cid
            }
        }
    }
}
