use cid::Cid;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConclusionEvent {
    Data(ConclusionData),
    Time(TimeEvent),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionInit {
    pub stream_type: String,
    pub controllers: String,
    pub dimensions: Vec<(String, ByteBuf)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionData {
    pub id: Cid,
    pub init: ConclusionInit,
    // TODO : rethink this, add a checkpoint to make it work in datafusion query
    pub previous: Vec<Cid>,
    pub before: Option<CeramicTime>,
    pub after: Option<CeramicTime>,
    pub data: ByteBuf,
}

pub type CeramicTime = chrono::DateTime<chrono::Utc>;
pub type Ipld = ByteBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeEvent {
    // WIP : Need clearer structure for time events
    pub before: Option<CeramicTime>,
    pub after: Option<CeramicTime>,
    pub data: ByteBuf,
    pub id: Cid,
}
