use crate::UnvalidatedEvent;
use serde::{Deserialize, Serialize};

/// A validated event
#[derive(Serialize, Deserialize)]
pub struct Event<D> {
    #[serde(flatten)]
    inner: UnvalidatedEvent<D>,
}
