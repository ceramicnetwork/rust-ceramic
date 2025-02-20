use cid::Cid;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use ceramic_car::sync::{CarHeader, CarWriter};
use ceramic_core::SerializeExt;

use crate::bytes::Bytes;

/// Represents an event with a payload and its corresponding CID
#[derive(Debug)]
pub struct Event<D> {
    payload: Payload<D>,
    cid: Cid,
}

impl<D> Event<D> {
    /// Get a reference to the payload
    pub fn payload(&self) -> &Payload<D> {
        &self.payload
    }

    /// Get the CID of the event
    pub fn cid(&self) -> &Cid {
        &self.cid
    }

    /// Create init Event with cid and payload
    pub(crate) fn new_with_cid(cid: Cid, payload: Payload<D>) -> Self {
        Self { cid, payload }
    }
}

impl<D: serde::Serialize> Event<D> {
    /// Create a new init Event
    pub fn new(payload: Payload<D>) -> Self {
        Self {
            cid: payload
                .to_cid()
                .expect("payload should always serialize to cid"),
            payload,
        }
    }

    /// Encode the unsigned init event into CAR bytes.
    pub fn encode_car(&self) -> Result<Vec<u8>, anyhow::Error> {
        let event = self.payload.to_cbor()?;
        let mut car = Vec::new();
        let roots: Vec<Cid> = vec![self.cid];
        let mut writer = CarWriter::new(CarHeader::V1(roots.into()), &mut car);
        writer.write(self.cid, event)?;
        writer.finish()?;
        Ok(car)
    }
}

/// Payload of an init event
#[derive(Debug, Serialize, Deserialize)]
pub struct Payload<D> {
    header: Header,
    data: Option<D>,
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

impl<D: serde::Serialize> Payload<D> {
    /// Encode the unsigned init event into CAR bytes.
    pub fn encode_car(&self) -> anyhow::Result<Vec<u8>> {
        let (cid, event) = self.to_dag_cbor_block()?;
        let mut car = Vec::new();
        let roots: Vec<Cid> = vec![cid];
        let mut writer = CarWriter::new(CarHeader::V1(roots.into()), &mut car);
        writer.write(cid, event)?;
        writer.finish()?;
        Ok(car)
    }
}

const DEFAULT_SEP: &str = "model";

/// Headers for an init event
#[derive(Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Header {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    controllers: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sep: Option<String>,
    // TODO: Handle separator keys other than "model"
    model: Bytes,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    should_index: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    unique: Option<Bytes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context: Option<Bytes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_version: Option<Cid>,
}

impl Header {
    /// Construct a header for an init event payload
    pub fn new(
        controllers: Vec<String>,
        sep: String,
        model: Vec<u8>,
        should_index: Option<bool>,
        unique: Option<Vec<u8>>,
        context: Option<Vec<u8>>,
        model_version: Option<Cid>,
    ) -> Self {
        Self {
            controllers,
            sep: Some(sep),
            model: Bytes::from(model),
            should_index,
            unique: unique.map(Bytes::from),
            context: context.map(Bytes::from),
            model_version,
        }
    }

    /// Get the controllers
    pub fn controllers(&self) -> &[String] {
        self.controllers.as_ref()
    }

    /// Get the sep
    pub fn sep(&self) -> &str {
        // NOTE: We implement the defaulting behavior here, as opposed to defaulting the value of
        // the sep field itself, so we can re-encode the header with the missing sep value so its
        // CID doesn't change.
        self.sep.as_deref().unwrap_or(DEFAULT_SEP)
    }

    /// Get the model
    pub fn model(&self) -> &[u8] {
        self.model.as_slice()
    }

    /// Signal to indexers whether this stream should be indexed
    pub fn should_index(&self) -> bool {
        self.should_index.unwrap_or(true)
    }
    /// Explicit model version to validate against.
    pub fn model_version(&self) -> Option<Cid> {
        self.model_version
    }

    /// The unique value for the stream
    pub fn unique(&self) -> Option<&[u8]> {
        self.unique.as_ref().map(Bytes::as_slice)
    }

    /// The context value for the stream
    pub fn context(&self) -> Option<&[u8]> {
        self.context.as_ref().map(Bytes::as_slice)
    }
}
impl std::fmt::Debug for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("Header")
                .field("controllers", &self.controllers)
                .field("sep", &self.sep)
                .field("model", &self.model)
                .field("should_index", &self.should_index)
                .field("unique", &self.unique)
                .field("context", &self.context)
                .field(
                    "model_version",
                    &self.model_version.as_ref().map(|m| m.to_string()),
                )
                .finish()
        } else {
            f.debug_struct("Header")
                .field("controllers", &self.controllers)
                .field("sep", &self.sep)
                .field("model", &self.model)
                .field("should_index", &self.should_index)
                .field("unique", &self.unique)
                .field("context", &self.context)
                .field("model_version", &self.model_version)
                .finish()
        }
    }
}
