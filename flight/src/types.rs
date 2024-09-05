use anyhow::{anyhow, Result};
use ceramic_event::unvalidated;
use cid::Cid;
use ipld_core::ipld::Ipld;
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
    pub index: u64,
}

pub type CeramicTime = chrono::DateTime<chrono::Utc>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionTime {
    // WIP : Need clearer structure for time events
    pub event_cid: Cid,
    pub init: ConclusionInit,
    pub previous: Vec<Cid>,
    pub index: u64,
}

impl TryFrom<unvalidated::Event<Ipld>> for ConclusionInit {
    type Error = anyhow::Error;

    fn try_from(event: unvalidated::Event<Ipld>) -> Result<Self> {
        let init_payload = event
            .init_payload()
            .ok_or_else(|| anyhow!("no init payload found"))?;
        Ok(ConclusionInit {
            stream_cid: *event.id(),
            controller: init_payload
                .header()
                .controllers()
                .first()
                .ok_or_else(|| anyhow!("no controller found"))?
                .to_string(),
            dimensions: vec![],
        })
    }
}
