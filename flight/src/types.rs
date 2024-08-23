use cid::Cid;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConclusionEvent {
    Data(ConclusionData),
    Time(TimeEvent),
}

impl AsRef<ConclusionEvent> for ConclusionEvent {
    fn as_ref(&self) -> &ConclusionEvent {
        self
    }
}

// Dimension is a tuple of (name, value) : way to identify a stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionInit {
    pub stream_type: u8,
    pub controller: String,
    pub dimensions: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionData {
    pub id: Cid,
    pub init: ConclusionInit,
    // TODO : rethink this, add a checkpoint to make it work in datafusion query
    pub previous: Vec<Cid>,
    pub data: Vec<u8>,
}

pub type CeramicTime = chrono::DateTime<chrono::Utc>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeEvent {
    // WIP : Need clearer structure for time events
    pub id: Cid,
    pub previous: Vec<Cid>,
}
