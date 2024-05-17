/// unvalidated data payloads
pub mod data;
/// unvalidated init payloads
pub mod init;

use serde::{Deserialize, Serialize};

/// Payload of a signed event
#[derive(Serialize, Deserialize)]
// Note untagged variants a deserialized in order and the first one that succeeds is returned.
// Therefore the order of the variants is important to be most specific to least specific
#[serde(untagged, deny_unknown_fields)]
pub enum Payload<D> {
    /// Data event
    Data(data::Payload<D>),
    /// Init event
    Init(init::Payload<D>),
}

impl<D> From<data::Payload<D>> for Payload<D> {
    fn from(value: data::Payload<D>) -> Self {
        Self::Data(value)
    }
}

impl<D> From<init::Payload<D>> for Payload<D> {
    fn from(value: init::Payload<D>) -> Self {
        Self::Init(value)
    }
}
