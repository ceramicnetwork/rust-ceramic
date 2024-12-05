use anyhow::anyhow;

use super::{PROTOCOL_NAME_INTEREST, PROTOCOL_NAME_MODEL, PROTOCOL_NAME_PEER};

/// Represents a stream set key
#[derive(Copy, Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub enum StreamSet {
    /// Stream set of peer ranges
    Peer,
    /// Stream set of interest ranges
    Interest,
    /// Stream set of models
    Model,
}

impl StreamSet {
    /// Report the sort key for this stream set.
    pub fn sort_key(&self) -> &str {
        match self {
            StreamSet::Peer => "peer",
            StreamSet::Interest => "interest",
            StreamSet::Model => "model",
        }
    }
}

impl TryFrom<&str> for StreamSet {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "peer" => Ok(StreamSet::Peer),
            "model" => Ok(StreamSet::Model),
            "interest" => Ok(StreamSet::Interest),
            _ => Err(anyhow!("unknown sort_key {}", value)),
        }
    }
}

impl AsRef<str> for StreamSet {
    fn as_ref(&self) -> &str {
        match self {
            StreamSet::Interest => PROTOCOL_NAME_INTEREST,
            StreamSet::Peer => PROTOCOL_NAME_PEER,
            StreamSet::Model => PROTOCOL_NAME_MODEL,
        }
    }
}
