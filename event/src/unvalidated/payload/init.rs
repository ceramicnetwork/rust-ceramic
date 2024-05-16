use crate::bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Payload of an init event
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Payload<D> {
    pub(crate) header: Header,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) data: Option<D>,
}

impl<D> Payload<D> {
    /// Construct a new payload for an init event
    /// TODO: Remove this method and use a builder pattern for building events instead.
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
#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Header {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) controllers: Vec<String>, // todo make all fields private
    pub(crate) sep: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) model: Option<Bytes>, // rename to sep_value, make not optional
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) should_index: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) unique: Option<Bytes>,
}

impl Header {
    /// Construct a header for an init event payload
    /// TODO: Remove this method and use a builder pattern for building events instead.
    pub fn new(
        controllers: Vec<String>,
        sep: String,
        model: Option<Vec<u8>>,
        should_index: Option<bool>,
        unique: Option<Vec<u8>>,
    ) -> Self {
        Self {
            controllers,
            sep,
            model: model.map(Bytes::from),
            should_index,
            unique: unique.map(Bytes::from),
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

    /// Get the model
    pub fn model(&self) -> Option<&[u8]> {
        self.model.as_ref().map(|m| m.as_slice())
    }

    /// Signal to indexers whether this stream should be indexed
    pub fn should_index(&self) -> bool {
        self.should_index.unwrap_or(true)
    }
}
