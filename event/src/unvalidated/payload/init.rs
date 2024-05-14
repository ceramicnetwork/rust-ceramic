use crate::unvalidated::{Value, ValueMap};
use serde::{Deserialize, Serialize};

/// Payload of an init event
#[derive(Serialize, Deserialize)]
pub struct Payload<D> {
    pub(crate) header: Header,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) data: Option<D>,
}

impl<D> Payload<D> {
    /// Construct a new payload for an init event
    pub fn new(header: Header, data: Option<D>) -> Self {
        Self { header, data }
    }

    /// Get the header
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Get a header value
    pub fn header_value(&self, key: &str) -> Option<&Value> {
        self.header.additional().get(key)
    }

    /// Get the data
    pub fn data(&self) -> Option<&D> {
        self.data.as_ref()
    }
}

/// Headers for an init event
#[derive(Default, Serialize, Deserialize)]
pub struct Header {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) controllers: Vec<String>,
    pub(crate) sep: String,
    #[serde(flatten, skip_serializing_if = "ValueMap::is_empty")]
    pub(crate) additional: ValueMap,
}

impl Header {
    /// Construct a header for an init event payload
    pub fn new(controllers: Vec<String>, sep: String, additional: ValueMap) -> Self {
        Self {
            controllers,
            sep,
            additional,
        }
    }

    /// Get the controllers
    pub fn controllers(&self) -> &[String] {
        self.controllers.as_ref()
    }

    /// Get the sep
    pub fn sep(&self) -> &str {
        self.sep.as_ref()
    }

    /// Get the additional fields
    pub fn additional(&self) -> &ValueMap {
        &self.additional
    }
}
