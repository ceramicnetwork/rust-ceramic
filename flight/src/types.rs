use anyhow::{anyhow, Result};
use ceramic_core::METAMODEL_STREAM_ID;
use ceramic_event::{unvalidated, StreamId, StreamIdType};
use cid::Cid;
use int_enum::IntEnum;
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
    /// The CID of the init event of the stream. Can be used as a unique identifier for the stream.
    /// This is not the StreamId as it does not contain the StreamType.
    pub stream_cid: Cid,
    /// The type of the stream.
    pub stream_type: u8,
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
    /// Raw bytes of the event data encoded as dag-json.
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

impl<'a> TryFrom<&'a unvalidated::Event<Ipld>> for ConclusionInit {
    type Error = anyhow::Error;

    fn try_from(event: &'a unvalidated::Event<Ipld>) -> Result<Self> {
        // Extract the init payload from the event
        let init_payload = event
            .init_payload()
            .ok_or_else(|| anyhow!("malformed event: no init payload found"))?;

        // Get the model from the init header
        // The model indicates the creator of the stream
        let model = init_payload.header().model();

        // Convert the model to a StreamId
        let stream_id = StreamId::try_from(model)?;

        // Determine the stream type:
        // If the stream_id matches the metamodel, it's a Model stream
        // Otherwise, it's a ModelInstanceDocument stream
        let stream_type = if stream_id == METAMODEL_STREAM_ID {
            StreamIdType::Model
        } else {
            StreamIdType::ModelInstanceDocument
        };

        // Construct and return the ConclusionInit
        Ok(ConclusionInit {
            stream_cid: *event.stream_cid(),
            stream_type: stream_type.int_value() as u8,
            controller: init_payload
                .header()
                .controllers()
                .first()
                .ok_or_else(|| anyhow!("no controller found"))?
                .to_string(),
            dimensions: vec![
                ("model".to_string(), init_payload.header().model().to_vec()),
                (
                    "controller".to_string(),
                    init_payload
                        .header()
                        .controllers()
                        .first()
                        .cloned()
                        .unwrap_or_default()
                        .into_bytes(),
                ),
                (
                    "context".to_string(),
                    init_payload
                        .header()
                        .context()
                        .map(|unique| unique.to_vec())
                        .unwrap_or_default(),
                ),
            ],
        })
    }
}
