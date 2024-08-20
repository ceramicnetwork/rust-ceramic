pub mod signature;
pub mod siwx_message;
#[cfg(test)]
mod test;
mod verifier;

pub use ceramic_event::unvalidated::signed::cacao;
pub use verifier::{cacao_verifier, event_verifier, key_verifier, opts::VerifyOpts};
