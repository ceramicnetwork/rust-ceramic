use anyhow::anyhow;

use crate::libp2p::{PROTOCOL_NAME_INTEREST, PROTOCOL_NAME_MODEL};

/// Represents a stream set key
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum StreamSet {
    /// Stream set of interest ranges
    Interest,
    /// Stream set of models
    Model,
}

impl StreamSet {
    /// Report the sort key for this stream set.
    pub fn sort_key(&self) -> &str {
        match self {
            StreamSet::Interest => "interest",
            StreamSet::Model => "model",
        }
    }
}

impl TryFrom<&str> for StreamSet {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
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
            StreamSet::Model => PROTOCOL_NAME_MODEL,
        }
    }
}
