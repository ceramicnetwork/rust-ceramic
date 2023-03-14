//! # Ceramic Event
//! Implementation of ceramic event protocol, with appropriate compatibilility with js-ceramic
#![deny(warnings)]
#![deny(missing_docs)]
mod args;
mod deterministic_event;
mod event;

pub use args::EventArgs;
pub use deterministic_event::DeterministicEvent;
pub use event::Event;

pub use ceramic_core::*;
