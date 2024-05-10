//! # Ceramic Event
//! Implementation of ceramic event protocol, with appropriate compatibilility with js-ceramic
#![deny(missing_docs)]
mod bytes;
mod signed_event;
mod unvalidated;
mod validated;
mod value;

pub use bytes::Bytes as EventBytes;
pub use signed_event::SignedEvent;
pub use unvalidated::{
    event::Event as UnvalidatedEvent,
    ext::*,
    payload::{InitPayload as UnvalidatedInitPayload, Payload as UnvalidatedPayload},
};
pub use validated::Event as ValidatedEvent;
pub use value::Value;

pub use ceramic_core::*;

/// Prelude for building events
pub mod event_builder {
    pub use super::unvalidated::builder::*;
}
