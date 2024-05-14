use crate::unvalidated::init::Payload;
use ceramic_core::DagCborEncoded;
use serde::{Deserialize, Serialize};

/// Unsigned event. Can only occur with init events without payload
#[derive(Deserialize, Serialize)]
pub struct UnsignedEvent {
    /// Encoded payload
    pub encoded: DagCborEncoded,
}

impl UnsignedEvent {
    /// Create a new unsigned event from an init payload with no data
    pub fn new(payload: Payload<()>) -> anyhow::Result<Self> {
        let encoded = DagCborEncoded::new(&payload)?;
        Ok(Self { encoded })
    }
}
