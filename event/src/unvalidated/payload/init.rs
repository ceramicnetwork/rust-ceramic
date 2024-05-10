use crate::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Payload of an init event
#[derive(Serialize, Deserialize)]
pub struct Payload<D> {
    pub(crate) header: Header,
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

    /// Get the data
    pub fn data(&self) -> Option<&D> {
        self.data.as_ref()
    }
}

/// Headers for an init event
#[derive(Serialize, Deserialize)]
pub struct Header {
    pub(crate) controllers: Vec<String>,
    pub(crate) sep: String,
    // model: Bytes,
    // unique: Option<Bytes>,
    #[serde(flatten)]
    pub(crate) additional: HashMap<String, Value>,
}

impl Header {
    /// Construct a header for an init event payload
    pub fn new(controllers: Vec<String>, sep: String, additional: HashMap<String, Value>) -> Self {
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
    pub fn additional(&self) -> &HashMap<String, Value> {
        &self.additional
    }
}
