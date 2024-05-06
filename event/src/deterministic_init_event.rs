use anyhow::Result;
use ceramic_core::DagCborEncoded;
use serde::Serialize;
use std::fmt::Formatter;

use crate::unvalidated;

/// A deterministic init event, where it will always hash the same way
#[derive(Serialize)]
pub struct DeterministicInitEvent {
    /// The encoded event
    #[serde(flatten)]
    pub encoded: DagCborEncoded,
}

impl DeterministicInitEvent {
    /// Create a deterministic init event from an unsigned event
    pub fn new(evt: &unvalidated::Payload<()>) -> Result<Self> {
        let data = DagCborEncoded::new(&evt)?;
        Ok(Self { encoded: data })
    }
}

impl std::fmt::Display for DeterministicInitEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.encoded)
    }
}
