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
    pub stream_type: StreamType,
    pub controllers: String,
    pub dimensions: Vec<(String, ByteBuf)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionData {
    pub id: Cid,
    pub init: ConclusionInit,
    pub previous: Vec<Cid>,
    pub before: Option<CeramicTime>,
    pub after: Option<CeramicTime>,
    pub data: Ipld,
}

// You'll need to define these types:
pub type StreamType = String; // Or use a more specific type if needed
pub type CeramicTime = u64; // Or use a more appropriate time representation
pub type Ipld = serde_json::Value; // Or use a proper IPLD type if available

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeEvent {
    // Define the structure for TimeEvent if needed
}
