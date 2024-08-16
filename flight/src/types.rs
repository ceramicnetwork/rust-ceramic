use anyhow::{anyhow, Result};
use ceramic_event::unvalidated;
use cid::Cid;
use ipld_core::ipld::Ipld;
use serde::{Deserialize, Serialize};

/// A Ceramic event annotated with conclusions about the event.
///
/// Conclusions included for all events:
///     1. An event's signature has been verified
///     2. An event's previous events will have an index less than the event's index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConclusionEvent {
    /// An event that contains data for a stream.
    Data(ConclusionData),
    /// An event that contains temporal information for a stream.
    Time(ConclusionTime),
}

impl AsRef<ConclusionEvent> for ConclusionEvent {
    fn as_ref(&self) -> &ConclusionEvent {
        self
    }
}

impl ConclusionEvent {
    pub(crate) fn event_type_as_int(&self) -> u8 {
        match self {
            ConclusionEvent::Data(_) => 0,
            ConclusionEvent::Time(_) => 1,
        }
    }
}

/// ConclusionInit is static metadata about a stream.
/// All events within a stream have the same ConclusionInit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionInit {
    /// The CID of the stream. Can be used as a unique identifier for the stream.
    /// This is not the StreamId as it does not contain the stream type.
    pub stream_cid: Cid,
    /// DID controller of the stream.
    pub controller: String,
    /// Order set of key value pairs that annotate the stream.
    pub dimensions: Vec<(String, Vec<u8>)>,
}

/// ConclusionData represents a Ceramic event that contained data.
///
/// Additionally we have concluded to which stream the event belongs and its associated metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionData {
    /// Index of the event. See [`ConclusionEvent`] for invariants about the index.
    pub index: u64,
    /// The CID of the event itself. Can be used as a unique identifier for the event.
    pub event_cid: Cid,
    /// The stream metadata of the event.
    pub init: ConclusionInit,
    /// Ordered list of previous events this event references.
    pub previous: Vec<Cid>,
    /// Raw bytes of the event data.
    pub data: Vec<u8>,
}

/// ConclusionTime represents a Ceramic event that contains time relevant information.
///
/// Additionally we have concluded to which stream the event belongs and its associated metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConclusionTime {
    /// Index of the event. See [`ConclusionEvent`] for invariants about the index.
    pub index: u64,
    /// The CID of the event itself. Can be used as a unique identifier for the event.
    pub event_cid: Cid,
    /// The stream metadata of the event.
    pub init: ConclusionInit,
    /// Ordered list of previous events this event references.
    pub previous: Vec<Cid>,
    //TODO Add temporal conclusions, i.e the block timestamp of this event
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
