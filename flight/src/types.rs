use cid::Cid;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConclusionEvent {
    Data(ConclusionData),
    Time(ConclusionTime),
}

impl AsRef<ConclusionEvent> for ConclusionEvent {
    fn as_ref(&self) -> &ConclusionEvent {
        self
    }
}

impl ConclusionEvent {
    pub fn event_type_as_int(&self) -> u8 {
        match self {
            ConclusionEvent::Data(_) => 0,
            ConclusionEvent::Time(_) => 1,
        }
    }
}

// Dimension is a tuple of (name, value) : way to identify a stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionInit {
    pub stream_cid: Cid,
    pub stream_type: u8,
    pub controller: String,
    pub dimensions: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionData {
    pub event_cid: Cid,
    pub init: ConclusionInit,
    // TODO : rethink this, add a checkpoint to make it work in datafusion query
    pub previous: Vec<Cid>,
    pub data: Vec<u8>,
    pub index: u64
}

pub type CeramicTime = chrono::DateTime<chrono::Utc>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionTime {
    // WIP : Need clearer structure for time events
    pub event_cid: Cid,
    pub init: ConclusionInit,
    pub previous: Vec<Cid>,
    pub index: u64
}
