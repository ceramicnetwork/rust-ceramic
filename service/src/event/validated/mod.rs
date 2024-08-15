
use anyhow::Result;
use ceramic_core::{ssi, DidDocument, EventId};
use cid::Cid;
use ipld_core::ipld::Ipld;
use serde::{Deserialize, Serialize};
use ssi::jwk::Algorithm;
use ceramic_event::unvalidated::{self, signed::cacao::Capability};

#[derive(Debug, Serialize, Deserialize)]
pub enum ValidatedEvent<D> {
    Init(ValidatedInitEvent<D>),
    Data(ValidatedDataEvent<D>),
    Time(ValidatedTimeEvent<D>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidatedInitEvent<D> {
    pub envelope: Option<unvalidated::signed::Envelope>,
    pub envelope_cid: Cid,
    pub payload: Option<unvalidated::init::Payload<D>>,
    pub payload_cid: Cid,
    pub capability: Option<(Cid, Capability)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidatedDataEvent<D> {
    pub event_id: Option<EventId>,
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
    fn validate_event(&self, event: &unvalidated::Event<Ipld>) -> Result<ValidatedEvent<Ipld>>;
}

pub struct EventValidator {
    // pub did_document: DidDocument,
    pub signer: DidDocument,
}


impl ValidateEvent for EventValidator {
    fn validate_event(&self, event: &unvalidated::Event<Ipld>) -> Result<ValidatedEvent<Ipld>> {
        match event {
            unvalidated::Event::Signed(signed_event) => {
               validate_signed_event(signed_event).map(ValidatedEvent::Init)
            }
            // unvalidated::Event::Time(time_event) => {
            //     // validate_time_event(time_event).map(ValidatedEvent::Time)
            // }
            _ => Err(anyhow::anyhow!("Unsupported event type")),
        }
    }
}

pub fn validate_signed_event(signed_event: &unvalidated::signed::Event<Ipld>) -> Result<ValidatedInitEvent<Ipld>> {
    let envelope = signed_event.envelope();
    let payload = signed_event.payload();
    let envelope_cid = signed_event.envelope_cid();
    let payload_cid = signed_event.payload_cid();
    // let capability = signed_event.capability();

    let signature_protected_header = signed_event.envelope().get_decoded_signature_protected_header();
    println!("signature_protected_header: {:?}", signature_protected_header);

    let (alg, kid) = if let Some(header) = signature_protected_header {
        let alg = header.get("alg").and_then(|v| v.as_str()).map(String::from);
        let kid = header.get("kid").and_then(|v| v.as_str()).map(String::from);
        (alg, kid)
    } else {
        return Err(anyhow::anyhow!("No signature protected header found in the envelope"));
    };

    // TODO : return an error if any step of this verification fails
    let public_key = get_public_key_from_did(&kid)?;
    verify_signature(&signature, &signed_data, &public_key)?;

    Ok(ValidatedInitEvent {
        envelope: None,
        envelope_cid,
        payload: None,
        payload_cid,
        capability: None,
    })
}


    pub fn get_public_key_from_did(kid: &str) -> Result<String> {
        // TODO: Implement this
        unimplemented!()
    }

    pub fn verify_signature(signature: &[u8], signed_data: &[u8], public_key: &String) -> Result<()> {
        // TODO: Implement this
        unimplemented!()
    }

