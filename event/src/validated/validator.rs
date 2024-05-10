use crate::{UnvalidatedEvent, ValidatedEvent};

/// A validator for events
#[async_trait::async_trait]
pub trait Validator {
    /// Validate an event
    async fn validate<T>(&self, event: &UnvalidatedEvent<T>) -> anyhow::Result<ValidatedEvent<T>>;
}
