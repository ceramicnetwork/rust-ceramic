use std::fmt;

use cid::Cid;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConclusionEvent {
    Data(ConclusionData),
    Time(TimeEvent),
}

// TODO_Discussion: Do we still support all these types?
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StreamType {
    /// A stream type representing a json document
    /// https://cips.ceramic.network/CIPs/cip-8
    Tile = 0,
    /// Link blockchain accounts to DIDs
    /// https://cips.ceramic.network/CIPs/cip-7
    Caip10Link = 1,
    /// Defines a schema shared by group of documents in ComposeDB
    /// https://github.com/ceramicnetwork/js-ceramic/tree/main/packages/stream-model
    Model = 2,
    /// Represents a json document in ComposeDB
    /// https://github.com/ceramicnetwork/js-ceramic/tree/main/packages/stream-model-instance
    ModelInstanceDocument = 3,
    /// A stream that is not meant to be loaded
    /// https://github.com/ceramicnetwork/js-ceramic/blob/main/packages/stream-model/src/model.ts#L163-L165
    Unloadable = 4,
    /// An event id encoded as a cip-124 EventID
    /// https://cips.ceramic.network/CIPs/cip-124
    EventId = 5,
}

impl fmt::Display for StreamType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamType::Tile => write!(f, "tile"),
            StreamType::Model => write!(f, "model"),
            StreamType::Caip10Link => write!(f, "caip10-link"),
            StreamType::Unloadable => write!(f, "unloadable"),
            StreamType::EventId => write!(f, "event-id"),
            StreamType::ModelInstanceDocument => write!(f, "model-instance-document"),
        }
    }
}

impl std::str::FromStr for StreamType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "tile" => Ok(StreamType::Tile),
            "model" => Ok(StreamType::Model),
            "caip10-link" => Ok(StreamType::Caip10Link),
            "event-id" => Ok(StreamType::EventId),
            "unloadable" => Ok(StreamType::Unloadable),
            "model-instance-document" => Ok(StreamType::ModelInstanceDocument),
            _ => Err(format!("Unknown stream type: {}", s)),
        }
    }
}

impl StreamType {
    pub fn as_u8(&self) -> u8 {
        match self {
            StreamType::Tile => 0,
            StreamType::Caip10Link => 1,
            StreamType::Model => 2,
            StreamType::ModelInstanceDocument => 3,
            StreamType::Unloadable => 4,
            StreamType::EventId => 5,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionInit {
    pub stream_type: StreamType,
    pub controller: String,
    pub dimensions: Vec<(String, ByteBuf)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionData {
    pub id: Cid,
    pub init: ConclusionInit,
    // TODO : rethink this, add a checkpoint to make it work in datafusion query
    pub previous: Vec<Cid>,
    pub data: ByteBuf,
}

pub type CeramicTime = chrono::DateTime<chrono::Utc>;
pub type Ipld = ByteBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeEvent {
    // WIP : Need clearer structure for time events
    pub id: Cid,
    pub previous: Vec<Cid>,
}
