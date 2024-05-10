use crate::Value;
use cid::Cid;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Payload of a data event
#[derive(Serialize, Deserialize)]
pub struct Payload<D> {
    pub(crate) id: Cid,
    pub(crate) prev: Cid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) header: Option<Header>,
    pub(crate) data: D,
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
    pub fn id(&self) -> Cid {
        self.id
    }

    /// Get the prev
    pub fn prev(&self) -> &Cid {
        &self.prev
    }

    /// Get the header
    pub fn header(&self) -> Option<&Header> {
        self.header.as_ref()
    }

    /// Get a header value
    pub fn header_value(&self, key: &str) -> Option<&Value> {
        self.header.as_ref().and_then(|h| h.additional.get(key))
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) controllers: Vec<String>,
    #[serde(flatten)]
    pub(crate) additional: HashMap<String, Value>,
}

impl Header {
    /// Construct a header for a data event payload
    pub fn new(controllers: Vec<String>, additional: HashMap<String, Value>) -> Self {
        Self {
            controllers,
            additional,
        }
    }

    /// Get the controllers
    pub fn controllers(&self) -> &[String] {
        self.controllers.as_ref()
    }

    /// Report the should_index property of the header.
    pub fn additional(&self) -> &HashMap<String, Value> {
        &self.additional
    }

    /// Determine if this should be indexed
    pub fn should_index(&self) -> bool {
        self.additional
            .get("shouldIndex")
            .map_or(true, |v| v.as_bool().unwrap_or(true))
    }
}
