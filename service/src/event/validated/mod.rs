
use anyhow::Result;
use base64::Engine;
use ceramic_core::{ssi, DidDocument, EventId, Jwk};
use cid::Cid;
use ipld_core::ipld::Ipld;
use serde::{Deserialize, Serialize};
use ceramic_event::unvalidated::{self, signed::cacao::Capability};

#[derive(Debug)]
pub enum ValidatedEvent<D> {
    Init(ValidatedInitEvent<D>),
    Data(ValidatedDataEvent<D>),
    Time(ValidatedTimeEvent<D>),
}

#[derive(Debug, Clone)]
pub struct ValidatedInitEvent<D> {
    pub event: unvalidated::signed::Event<D>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidatedDataEvent<D> {
    pub event_id: EventId,
    pub payload: Option<unvalidated::data::Payload<D>>,
    pub signature: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidatedTimeEvent<D> {
    pub event_id: EventId,
    // TODO : Fix this
    pub payload: unvalidated::data::Payload<D>,
    pub signature: Vec<u8>,
}

pub trait ValidateEvent {
    async fn validate_event(&self, event: &unvalidated::Event<Ipld>) -> Result<ValidatedEvent<Ipld>>;
}

pub struct EventValidator {
    // TODO : Add signer information in the struct before using it?
    pub signer: DidDocument,
}


impl ValidateEvent for EventValidator {
    async fn validate_event(&self, event: &unvalidated::Event<Ipld>) -> Result<ValidatedEvent<Ipld>> {
        match event {
            unvalidated::Event::Signed(signed_event) => {
                verify_event(signed_event).await.map(ValidatedEvent::Init)
            }
            _ => Err(anyhow::anyhow!("Unsupported event type")),
        }
    }
}
// return OK() 
async fn verify_event(s: &unvalidated::signed::Event<Ipld>) -> anyhow::Result<ValidatedInitEvent<Ipld>> {
    let (protected, signature) = match s.envelope.signatures.first() {
        Some(sig) => (
            sig.protected
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Missing protected field"))?
                .as_slice(),
            sig.signature.as_ref(),
        ),
        None => {
            anyhow::bail!("signature is missing")
        }
    };
    let header: ssi::jws::Header = serde_json::from_slice(protected)?;
    let kid = header
        .key_id
        .ok_or_else(|| anyhow::anyhow!("Missing jws kid"))?;
    let kid = kid.split_once("#").map(|(did, _)| did).unwrap_or(&kid);

    let jwk = get_public_key_from_did(kid).await.unwrap();

    let header_str = base64::engine::general_purpose::STANDARD_NO_PAD.encode(protected);
    let payload_cid =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s.envelope.payload.as_slice());
    let digest = format!("{}.{}", header_str, payload_cid);

    ssi::jws::verify_bytes(header.algorithm, digest.as_bytes(), &jwk, signature)?;
    // TODO : lifetimes? Easy : take a move , take a borrow ? 
    Ok(ValidatedInitEvent {
        event: s.clone(),
    })
}


async fn get_public_key_from_did(kid: &str) -> Result<Jwk> {
    let did_document = Jwk::resolve_did(kid).await.unwrap();
    let keys = Jwk::new(&did_document).await.unwrap();
    return Ok(keys);
}