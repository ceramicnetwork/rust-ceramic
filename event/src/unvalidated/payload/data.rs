use std::fmt::Debug;

use cid::Cid;
use serde::{Deserialize, Serialize};

/// Payload of a data event
#[derive(Serialize, Deserialize)]
pub struct Payload<D> {
    id: Cid,
    prev: Cid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    header: Option<Header>,
    data: D,
}

impl<D: Debug> Debug for Payload<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Payload")
            .field("id", &self.id.to_string())
            .field("prev", &self.prev.to_string())
            .field("header", &self.header)
            .field("data", &self.data)
            .finish()
    }
}

impl<D> Payload<D> {
    /// Construct a new payload for a data event
    pub fn new(id: Cid, prev: Cid, header: Option<Header>, data: D) -> Self {
        Self {
            id,
            prev,
            header,
            data,
        }
    }

    /// Get the id
    pub fn id(&self) -> &Cid {
        &self.id
    }

    /// Get the prev
    pub fn prev(&self) -> &Cid {
        &self.prev
    }

    /// Get the header
    pub fn header(&self) -> Option<&Header> {
        self.header.as_ref()
    }

    /// Get the data
    pub fn data(&self) -> &D {
        &self.data
    }
}

/// Headers for a data event
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Header {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    should_index: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_version: Option<Cid>,
}

impl Header {
    /// Construct a header for a data event payload
    pub fn new(should_index: Option<bool>, model_version: Option<Cid>) -> Self {
        Self {
            should_index,
            model_version,
        }
    }

    /// Signal to indexers whether this stream should be indexed
    pub fn should_index(&self) -> Option<bool> {
        self.should_index
    }
    /// Explicit model version to validate against.
    pub fn model_version(&self) -> Option<Cid> {
        self.model_version
    }
}
impl std::fmt::Debug for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("Header")
                .field("should_index", &self.should_index)
                .field(
                    "model_version",
                    &self.model_version.as_ref().map(|m| m.to_string()),
                )
                .finish()
        } else {
            f.debug_struct("Header")
                .field("should_index", &self.should_index)
                .field("model_version", &self.model_version)
                .finish()
        }
    }
}
