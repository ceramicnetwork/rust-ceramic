use std::convert::Infallible;

use serde::Deserialize;

/// DeserializeExt extends [`Deserialize`] with methods specific to dag-json and dag-cbor
/// deserialization.
pub trait DeserializeExt<'de>: Deserialize<'de> {
    /// Deserialize dag-json encoded data into self.
    fn from_json(data: &'de [u8]) -> Result<Self, serde_ipld_dagjson::DecodeError> {
        serde_ipld_dagjson::from_slice(data)
    }
    /// Deserialize dag-cbor encoded data into self.
    fn from_cbor(data: &'de [u8]) -> Result<Self, serde_ipld_dagcbor::DecodeError<Infallible>> {
        serde_ipld_dagcbor::from_slice(data)
    }
}

impl<'de, T: Deserialize<'de>> DeserializeExt<'de> for T {}
