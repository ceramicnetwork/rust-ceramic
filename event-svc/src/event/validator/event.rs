use std::sync::Arc;

use ceramic_core::{Cid, EventId, NodeId};
use ceramic_event::unvalidated;
use ipld_core::ipld::Ipld;
use recon::ReconItem;
use tokio::try_join;

use crate::{
    event::{
        service::{ValidationError, ValidationRequirement},
        validator::signed::SignedEventValidator,
    },
    store::{EventInsertable, SqlitePool},
    Result,
};

use super::grouped::{GroupedEvents, SignedValidationBatch, TimeValidationBatch};

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
    /// Convert this ValidatedEvent into an EventInsertable
    pub fn into_insertable(value: Self, informant: Option<NodeId>) -> EventInsertable {
        EventInsertable::new(value.key, value.cid, value.event, informant, false)
            .expect("validated events must be insertable")
    }

    /// The way to convert into a validated event. Normally `unchecked` has a "memory unsafety" implication typically,
    /// but this is safe code, however, doing this outside of the validation flow is not protocol safe.
    /// It is used to covert an event after validation succeeds currently, but is also used in tests and
    /// by anything that skips the validation process (e.g. an ipfs -> ceramic-one migration).
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
    check_exp: bool,
}

impl<'a> EventValidator<'a> {
    fn new(pool: &'a SqlitePool, requirement: &ValidationRequirement) -> Self {
        Self {
            pool,
            check_exp: requirement.check_exp,
        }
    }

    /// Validates the events with the given validation requirement
    /// If the [`ValidationRequirement`] is None, it just returns every event as valid
    pub(crate) async fn validate_events(
        pool: &'a SqlitePool,
        validation_req: Option<&ValidationRequirement>,
        parsed_events: Vec<UnvalidatedEvent>,
    ) -> Result<ValidatedEvents> {
        let validation_req = if let Some(req) = validation_req {
            req
        } else {
            // we don't validate so we just return done
            return Ok(ValidatedEvents {
                valid: parsed_events
                    .into_iter()
                    .map(ValidatedEvent::from_unvalidated_unchecked)
                    .collect(),
                unvalidated: Vec::new(),
                invalid: Vec::new(),
            });
        };
        let validator = Self::new(pool, validation_req);

        let mut validated = ValidatedEvents::new_with_expected_valid(parsed_events.len());
        // partition the events by type of validation needed and delegate to validators
        let grouped = GroupedEvents::from(parsed_events);

        let (validated_signed, validated_time) = try_join!(
            validator.validate_signed_events(grouped.signed_batch),
            validator.validate_time_events(grouped.time_batch)
        )?;
        validated.extend_with(validated_signed);
        validated.extend_with(validated_time);

        if !validated.invalid.is_empty() {
            tracing::warn!(count=%validated.invalid.len(), "invalid events discovered");
        }
        Ok(validated)
    }

    async fn validate_signed_events(
        &self,
        events: SignedValidationBatch,
    ) -> Result<ValidatedEvents> {
        let opts = if self.check_exp {
            ceramic_validation::VerifyJwsOpts::default()
        } else {
            ceramic_validation::VerifyJwsOpts {
                at_time: ceramic_validation::AtTime::SkipTimeChecks,
                ..Default::default()
            }
        };
        SignedEventValidator::validate_events(self.pool, &opts, events).await
    }

    async fn validate_time_events(&self, events: TimeValidationBatch) -> Result<ValidatedEvents> {
        // TODO: IMPLEMENT THIS
        Ok(ValidatedEvents {
            valid: events.0.into_iter().map(ValidatedEvent::from).collect(),
            unvalidated: Vec::new(),
            invalid: Vec::new(),
        })
    }
}

#[cfg(test)]
mod test {
    use test_log::test;

    use crate::tests::{build_recon_item_with_controller, get_n_events};

    use super::*;

    /// returns 10 valid, 9 pending, 1 invalid event
    async fn get_validation_events() -> Vec<UnvalidatedEvent> {
        let valid = get_n_events(10).await;
        // without the init event the rest can't be validated
        let pending: Vec<_> = get_n_events(10).await.into_iter().skip(1).collect();
        // the controller isn't a real did so it won't validate
        let invalid = build_recon_item_with_controller("did:notvalid:abcdefg".into()).await;

        let events = valid
            .iter()
            .chain(&[invalid])
            .chain(&pending)
            .map(|e| UnvalidatedEvent::try_from(e).unwrap())
            .collect();
        events
    }

    #[test(tokio::test)]
    async fn valid_invalid_pending_recon() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let events = get_validation_events().await;

        let validated = EventValidator::validate_events(
            &pool,
            Some(&ValidationRequirement::new_recon()),
            events,
        )
        .await
        .unwrap();

        assert_eq!(10, validated.valid.len());
        assert_eq!(9, validated.unvalidated.len());
        assert_eq!(1, validated.invalid.len());

        let err = validated.invalid.first().unwrap();
        assert!(
            matches!(err, ValidationError::InvalidSignature { .. }),
            "{:?}",
            err
        );
    }

    #[test(tokio::test)]
    async fn valid_invalid_pending_local() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let events = get_validation_events().await;

        let validated = EventValidator::validate_events(
            &pool,
            Some(&ValidationRequirement::new_local()),
            events,
        )
        .await
        .unwrap();

        assert_eq!(10, validated.valid.len());
        assert_eq!(9, validated.unvalidated.len());
        assert_eq!(1, validated.invalid.len());

        let err = validated.invalid.first().unwrap();
        assert!(
            matches!(err, ValidationError::InvalidSignature { .. }),
            "{:?}",
            err
        );
    }

    #[test(tokio::test)]
    async fn always_valid_when_none() {
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let events = get_validation_events().await;

        let validated = EventValidator::validate_events(&pool, None, events)
            .await
            .unwrap();

        assert_eq!(20, validated.valid.len(), "{:?}", validated);
    }
}
