use crate::args::UnsignedEvent;
use anyhow::Result;
use ceramic_core::DagCborEncoded;
use serde::Serialize;
use std::fmt::Formatter;

/// A deterministic init event, where it will always hash the same way
pub struct DeterministicInitEvent {
    /// The encoded event
    pub encoded: DagCborEncoded,
}

impl DeterministicInitEvent {
    /// Create a deterministic init event from an unsigned event
    pub fn new<T: Serialize>(evt: &UnsignedEvent<'_, T>) -> Result<Self> {
        let data = DagCborEncoded::new(&evt)?;
        Ok(Self { encoded: data })
    }
}

impl std::fmt::Display for DeterministicInitEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.encoded)
    }
}
