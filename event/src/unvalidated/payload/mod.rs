pub(crate) mod data;
pub(crate) mod init;
pub(crate) mod signed;

pub use data::{Header as DataHeader, Payload as DataPayload};
pub use init::{Header as InitHeader, Payload as InitPayload};
use serde::{Deserialize, Serialize};
pub use signed::Payload as SignedPayload;

/// Payload of a signed event
#[derive(Serialize, Deserialize)]
// Note untagged variants a deserialized in order and the first one that succeeds is returned.
// Therefore the order of the variants is important to be most specific to least specific
#[serde(untagged)]
pub enum Payload<D> {
    /// Data event
    Data(DataPayload<D>),
    /// Init event
    Init(InitPayload<D>),
}

impl<D> From<DataPayload<D>> for Payload<D> {
    fn from(value: DataPayload<D>) -> Self {
        Self::Data(value)
    }
}

impl<D> From<InitPayload<D>> for Payload<D> {
    fn from(value: InitPayload<D>) -> Self {
        Self::Init(value)
    }
}
