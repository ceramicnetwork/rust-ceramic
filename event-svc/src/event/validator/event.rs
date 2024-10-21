use std::sync::Arc;

use ceramic_core::{Cid, EventId, NodeId};
use ceramic_event::unvalidated;
use ceramic_validation::eth_rpc;
use ipld_core::ipld::Ipld;
use recon::ReconItem;
use tokio::try_join;

use crate::event::validator::ChainInclusionProvider;
use crate::store::EventAccess;
use crate::{
    event::{
        service::{ValidationError, ValidationRequirement},
        validator::{
            grouped::{GroupedEvents, SignedValidationBatch, TimeValidationBatch},
            signed::SignedEventValidator,
            time::TimeEventValidator,
        },
    },
    store::EventInsertable,
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
    /// Convert this ValidatedEvent into an EventInsertable
    pub fn into_insertable(value: Self, informant: Option<NodeId>) -> EventInsertable {
        EventInsertable::try_new(
            value.key,
            value.cid,
            value.event.is_init(),
            value.event,
            informant,
        )
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
pub struct EventValidator {
    event_access: Arc<EventAccess>,
    /// The validator to use for time events if enabled.
    /// It contains the ethereum RPC providers and lives for the live of the [`EventValidator`].
    /// The [`SignedEventValidator`] is currently constructed on a per validation request basis
    /// as it caches and drops events per batch.
    time_event_verifier: TimeEventValidator,
}

impl EventValidator {
    /// Create a new event validator
    pub async fn try_new(
        event_access: Arc<EventAccess>,
        ethereum_rpc_providers: Vec<ChainInclusionProvider>,
    ) -> Result<Self> {
        let time_event_verifier = TimeEventValidator::new_with_providers(ethereum_rpc_providers);

        Ok(Self {
            event_access,
            time_event_verifier,
        })
    }

    /// Validates the events with the given validation requirement
    /// If the [`ValidationRequirement`] is None, it just returns every event as valid
    pub(crate) async fn validate_events(
        &self,
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

        let mut validated = ValidatedEvents::new_with_expected_valid(parsed_events.len());
        // partition the events by type of validation needed and delegate to validators
        let grouped = GroupedEvents::from(parsed_events);

        let (validated_signed, validated_time) = try_join!(
            self.validate_signed_events(grouped.signed_batch, validation_req),
            self.validate_time_events(grouped.time_batch)
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
        validation_req: &ValidationRequirement,
    ) -> Result<ValidatedEvents> {
        let opts = if validation_req.check_exp {
            ceramic_validation::VerifyJwsOpts::default()
        } else {
            ceramic_validation::VerifyJwsOpts {
                at_time: ceramic_validation::AtTime::SkipTimeChecks,
                ..Default::default()
            }
        };
        SignedEventValidator::validate_events(
            Arc::clone(&self.event_access),
            &opts,
            events,
            validation_req.require_local_init,
        )
        .await
    }

    async fn validate_time_events(&self, events: TimeValidationBatch) -> Result<ValidatedEvents> {
        let mut validated_events = ValidatedEvents::new_with_expected_valid(events.0.len());
        for time_event in events.0 {
            // TODO: better transient error handling from RPC client
            match self
                .time_event_verifier
                .validate_chain_inclusion(time_event.as_time())
                .await
            {
                Ok(_t) => {
                    // TODO(AES-345): Someday, we will use `t.as_unix_ts()` and care about the actual timestamp, but for now we just consider it valid
                    validated_events.valid.push(time_event.into());
                }
                Err(err) => {
                    validated_events.invalid.push(Self::convert_inclusion_error(
                        err,
                        &time_event.as_inner().key,
                    ));
                }
            }
        }
        Ok(validated_events)
    }

    /// Transforms the [`eth_rpc::Error`] into a [`ValidationError`] with an appropriate message
    fn convert_inclusion_error(err: eth_rpc::Error, order_key: &EventId) -> ValidationError {
        match err {
            eth_rpc::Error::TxNotFound { chain_id, tx_hash } => {
                // we have an RPC provider so the transaction missing means it's invalid/unproveable
                ValidationError::InvalidTimeProof {
                    key: order_key.to_owned(),
                    reason: format!(
                        "Transaction on chain '{chain_id}' with hash '{tx_hash}' not found."
                    ),
                }
            },
           eth_rpc::Error::TxNotMined { chain_id, tx_hash } => {
                    ValidationError::InvalidTimeProof {
                        key: order_key.to_owned(),
                        reason: format!("Transaction on chain '{chain_id}' with hash '{tx_hash}' has not been mined in a block yet."),
                    }
            },
            eth_rpc::Error::InvalidProof(reason) => ValidationError::InvalidTimeProof {
                key: order_key.to_owned(),
                reason,
            },
            eth_rpc::Error::NoChainProvider(chain_id) => {
                    ValidationError::InvalidTimeProof {
                        key: order_key.to_owned(),
                    reason: format!("No RPC provider for chain '{chain_id}'. Transaction for event cannot be verified."),
                }
            },
            eth_rpc::Error::InvalidArgument(e) =>  ValidationError::InvalidTimeProof {
                key: order_key.to_owned(),
            reason: format!("Invalid argument: {}", e),
            },
            eth_rpc::Error::BlockNotFound { chain_id, block_hash } => ValidationError::InvalidTimeProof {
                key: order_key.to_owned(),
            reason: format!("Block not found  on chain {} with hash: {}", chain_id, block_hash)
            },
            eth_rpc::Error::Transient(error) => ValidationError::InvalidTimeProof {
                key: order_key.to_owned(),
            reason: format!("transient error encountered talking to eth rpc: {error}")
            },
            eth_rpc::Error::Application(error) => ValidationError::InvalidTimeProof {
                key: order_key.to_owned(),
            reason: format!("application error encountered: {error}")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use ceramic_sql::sqlite::SqlitePool;
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
        let event_access = Arc::new(EventAccess::try_new(pool).await.unwrap());
        let events = get_validation_events().await;

        let validated = EventValidator::try_new(event_access, vec![])
            .await
            .unwrap()
            .validate_events(Some(&ValidationRequirement::new_recon()), events)
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
        let event_access = Arc::new(EventAccess::try_new(pool).await.unwrap());
        let events = get_validation_events().await;

        let validated = EventValidator::try_new(event_access, vec![])
            .await
            .unwrap()
            .validate_events(Some(&ValidationRequirement::new_local()), events)
            .await
            .unwrap();

        assert_eq!(10, validated.valid.len());
        assert_eq!(0, validated.unvalidated.len());
        assert_eq!(10, validated.invalid.len()); // 1 + 9 pending

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
        let event_access = Arc::new(EventAccess::try_new(pool).await.unwrap());
        let events = get_validation_events().await;

        let validated = EventValidator::try_new(event_access, vec![])
            .await
            .unwrap()
            .validate_events(None, events)
            .await
            .unwrap();

        assert_eq!(20, validated.valid.len(), "{:?}", validated);
    }
}
