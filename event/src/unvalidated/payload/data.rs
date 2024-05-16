use cid::Cid;
use serde::{Deserialize, Serialize};

/// Payload of a data event
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Payload<D> {
    id: Cid,
    prev: Cid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    header: Option<Header>,
    data: D,
}

impl<D> Payload<D> {
    /// Construct a new payload for a data event
    /// TODO: Remove this method and use a builder pattern for building events instead.
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
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Header {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) should_index: Option<bool>,
}

impl Header {
    /// Construct a header for a data event payload
    /// TODO: Remove this method and use a builder pattern for building events instead.
    pub fn new(should_index: Option<bool>) -> Self {
        Self { should_index }
    }

    /// Signal to indexers whether this stream should be indexed
    pub fn should_index(&self) -> bool {
        self.should_index.unwrap_or(true)
    }
}
