use crate::{bytes::Bytes, unvalidated::cid_from_dag_cbor};
use cid::Cid;
use iroh_car::{CarHeader, CarWriter};
use serde::{Deserialize, Serialize};

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
    /// Compute CID of encoded event.
    pub async fn encoded_cid(&self) -> Result<Cid, anyhow::Error> {
        let event = serde_ipld_dagcbor::to_vec(self)?;
        Ok(cid_from_dag_cbor(&event))
    }
    /// Encode the unsigned init event into CAR bytes.
    pub async fn encode_car(&self) -> Result<Vec<u8>, anyhow::Error> {
        let event = serde_ipld_dagcbor::to_vec(self)?;
        let cid = cid_from_dag_cbor(&event);
        let mut car = Vec::new();
        let roots: Vec<Cid> = vec![cid];
        let mut writer = CarWriter::new(CarHeader::V1(roots.into()), &mut car);
        writer.write(cid, event).await?;
        writer.finish().await?;
        Ok(car)
    }
}

const DEFAULT_SEP: &str = "model";

/// Headers for an init event
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    ) -> Self {
        Self {
            controllers,
            sep: Some(sep),
            model: Bytes::from(model),
            should_index,
            unique: unique.map(Bytes::from),
            context: context.map(Bytes::from),
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

    /// The unique value for the stream
    pub fn unique(&self) -> Option<&[u8]> {
        self.unique.as_ref().map(Bytes::as_slice)
    }

    /// The context value for the stream
    pub fn context(&self) -> Option<&[u8]> {
        self.context.as_ref().map(Bytes::as_slice)
    }
}
