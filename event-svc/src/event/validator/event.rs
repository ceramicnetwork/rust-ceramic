use std::sync::Arc;

use ceramic_core::{Cid, EventId, NodeId};
use ceramic_event::unvalidated;
use ipld_core::ipld::Ipld;
use recon::ReconItem;

use crate::{
    event::service::{ValidationError, ValidationRequirement},
    store::{EventInsertable, SqlitePool},
    Result,
};

#[derive(Debug)]
pub struct ValidatedEvents {
    /// These events are valid
    pub valid: Vec<ValidatedEvent>,
    /// We don't have enough information to validate these events yet.
    /// e.g. The init event is required and the node has not yet discovered it.
    pub unvalidated: Vec<UnvalidatedEvent>,
    /// Events that failed validation
    pub invalid: Vec<ValidationError>,
}

#[derive(Debug)]
pub struct ValidatedEvent {
    key: EventId,
    cid: Cid,
    event: Arc<unvalidated::Event<Ipld>>,
}

impl ValidatedEvent {
    pub fn order_key(&self) -> &EventId {
        &self.key
    }

    pub fn into_insertable(value: Self, informant: Option<NodeId>) -> EventInsertable {
        EventInsertable::new(value.key, value.cid, value.event, informant, false)
            .expect("validated events must be insertable")
    }

    /// Skip the validation process. `unchecked` has a "memory unsafety" implication typically, but
    /// this is safe code, however, doing this is not protocol safe. Mainly used in tests and by anything
    /// that skips the validation process (e.g. an ipfs -> ceramic-one migration).
    pub(crate) fn from_unvalidated_unchecked(event: UnvalidatedEvent) -> Self {
        Self {
            key: event.key,
            cid: event.cid,
            event: event.event,
        }
    }
}

#[derive(Debug)]
pub struct UnvalidatedEvent {
    pub key: EventId,
    pub cid: Cid,
    pub event: Arc<unvalidated::Event<Ipld>>,
}

impl UnvalidatedEvent {
    pub fn order_key(&self) -> &EventId {
        &self.key
    }
}

impl TryFrom<&ReconItem<EventId>> for UnvalidatedEvent {
    type Error = crate::Error;

    fn try_from(item: &ReconItem<EventId>) -> std::result::Result<Self, Self::Error> {
        let (cid, event) = unvalidated::Event::<Ipld>::decode_car(item.value.as_slice(), false)
            .map_err(Self::Error::new_app)?;

        let key_cid = item.key.cid().ok_or_else(|| {
            Self::Error::new_app(anyhow::anyhow!("EventId missing CID. EventID={}", item.key))
        })?;

        if key_cid != cid {
            return Err(Self::Error::new_app(anyhow::anyhow!(
                "EventId CID ({}) does not match the root CID of the CAR file ({})",
                key_cid,
                cid
            )));
        }

        Ok(Self {
            key: item.key.to_owned(),
            cid: key_cid,
            event: Arc::new(event),
        })
    }
}

impl ValidatedEvents {
    pub fn new_with_expected_valid(valid: usize) -> Self {
        // sort of arbitrary sizes, not betting on invalid events
        Self {
            valid: Vec::with_capacity(valid),
            unvalidated: Vec::with_capacity(valid / 4),
            invalid: Vec::new(),
        }
    }

    pub fn extend_with(&mut self, other: Self) {
        self.valid.extend(other.valid);
        self.invalid.extend(other.invalid);
        self.unvalidated.extend(other.unvalidated);
    }
}

#[derive(Debug)]
pub struct EventValidator<'a> {
    pool: &'a SqlitePool,
    /// Whether we should check the signature is currently valid or simply whether it was once valid
    requirement: ValidationRequirement,
}

impl<'a> EventValidator<'a> {
    fn new(pool: &'a SqlitePool, requirement: ValidationRequirement) -> Self {
        Self { pool, requirement }
    }
    pub(crate) async fn validate_events(
        _pool: &'a SqlitePool,
        _validation_req: Option<ValidationRequirement>,
        parsed_events: Vec<UnvalidatedEvent>,
    ) -> Result<ValidatedEvents> {
        // let _validator = Self::new(pool, validation_req);
        // TODO: IMPLEMENT THIS
        Ok(ValidatedEvents {
            valid: parsed_events
                .into_iter()
                .map(ValidatedEvent::from_unvalidated_unchecked)
                .collect(),
            unvalidated: Vec::new(),
            invalid: Vec::new(),
        })
    }
}
