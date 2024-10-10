use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use ceramic_core::{SerializeExt, StreamId};
use ceramic_event::unvalidated::{self, init, signed};
use ceramic_validation::{cacao_verifier::Verifier, event_verifier::Verifier as _, VerifyJwsOpts};
use cid::Cid;
use ipld_core::ipld::Ipld;
use tracing::warn;

use crate::{event::service::ValidationError, store::EventAccess, Result};

use super::{
    event::{ValidatedEvent, ValidatedEvents},
    grouped::SignedValidationBatch,
    UnvalidatedEvent,
};

#[derive(Debug)]
/// A signed validator is expected to be constructed to validate a batch of events and then be dropped.
/// It will cache init events for the batch to use at different stages of validation and then drop everything
/// when it finishes.
pub struct SignedEventValidator {
    /// The init events needed to validate this batch of events.
    init_map: HashMap<Cid, Arc<unvalidated::Event<Ipld>>>,
    /// The set of invalid init CIDs that can be used to reject any events that depend on them without even looking at them.
    invalid_init: HashSet<Cid>,
    /// The result of this validation batch that is updated as we go.
    validated: ValidatedEvents,
    /// Whether events without their init event should be considered invalid or simply ignored for now
    require_init_event: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ValidationStatus {
    ValidInit,
    ValidData,
    InvalidInit(String),
    InvalidData(String),
    Pending,
}

impl SignedEventValidator {
    fn new(init_cnt: usize, data_cnt: usize, require_init_event: bool) -> Self {
        Self {
            init_map: HashMap::with_capacity(init_cnt),
            invalid_init: HashSet::new(),
            validated: ValidatedEvents::new_with_expected_valid(init_cnt + data_cnt),
            require_init_event,
        }
    }

    /// blindly trusts that this is indeed an init event
    /// will error later if not
    fn track_init(&mut self, cid: Cid, event: Arc<unvalidated::Event<Ipld>>) {
        self.init_map.insert(cid, event);
    }

    fn add_invalid(&mut self, item: ValidationError) {
        self.validated.invalid.push(item);
    }

    fn add_pending(&mut self, item: UnvalidatedEvent) {
        self.validated.unvalidated.push(item);
    }

    fn add_valid(&mut self, item: ValidatedEvent) {
        self.validated.valid.push(item);
    }

    fn add_valid_iter<I>(&mut self, item: I)
    where
        I: Iterator<Item = ValidatedEvent>,
    {
        self.validated.valid.extend(item);
    }

    pub async fn validate_events(
        event_access: Arc<EventAccess>,
        opts: &VerifyJwsOpts,
        events: SignedValidationBatch,
        require_init_event: bool,
    ) -> Result<ValidatedEvents> {
        let mut validator = Self::new(
            events.init.len() + events.unsigned.len(),
            events.data.len(),
            require_init_event,
        );
        validator.init_map.extend(
            events
                .unsigned
                .iter()
                .map(|i| (i.as_inner().cid, i.as_inner().event.to_owned())),
        );
        validator.add_valid_iter(events.unsigned.into_iter().map(|i| i.into()));

        for event in events.init {
            let status = validator
                .validate_signed(Arc::clone(&event_access), opts, event.as_signed())
                .await?;
            validator.process_result(status, event.into_inner());
        }

        for event in events.data {
            let status = validator
                .validate_signed(Arc::clone(&event_access), opts, event.as_signed())
                .await?;
            validator.process_result(status, event.into_inner());
        }

        Ok(validator.validated)
    }

    fn process_result(&mut self, status: ValidationStatus, event: UnvalidatedEvent) {
        match status {
            ValidationStatus::ValidInit => {
                self.init_map.insert(event.cid, event.event.clone());
                self.add_valid(ValidatedEvent::from_unvalidated_unchecked(event));
            }
            ValidationStatus::ValidData => {
                self.add_valid(ValidatedEvent::from_unvalidated_unchecked(event))
            }
            ValidationStatus::InvalidInit(reason) => {
                self.invalid_init.insert(event.cid);
                self.add_invalid(ValidationError::InvalidSignature {
                    key: event.key,
                    reason,
                })
            }
            ValidationStatus::InvalidData(reason) => {
                self.add_invalid(ValidationError::InvalidSignature {
                    key: event.key,
                    reason,
                })
            }
            ValidationStatus::Pending => {
                if self.require_init_event {
                    self.add_invalid(ValidationError::RequiresHistory { key: event.key });
                } else {
                    self.add_pending(event)
                }
            }
        }
    }

    /// Returns an enum representing the validation status and what type of event it was
    async fn validate_signed(
        &mut self,
        event_access: Arc<EventAccess>,
        opts: &VerifyJwsOpts,
        signed: &signed::Event<Ipld>,
    ) -> Result<ValidationStatus> {
        match signed.payload() {
            unvalidated::Payload::Data(d) => {
                let init = if let Some(init) = self.init_map.get(d.id()) {
                    init.to_owned()
                } else {
                    match event_access.value_by_cid(d.id()).await? {
                        Some(init) => {
                            let (init_cid, init) =
                                            unvalidated::Event::<Ipld>::decode_car(
                                                std::io::Cursor::new(init),
                                                false,
                                            )
                                            .unwrap_or_else(|err| panic!("should not have stored invalid data in db for init cid={}. err={:#}", d.id(), err));
                            let init = std::sync::Arc::new(init);
                            self.track_init(init_cid, init.clone()); // cache in case other events need it
                            init
                        }
                        None => {
                            // if it depends on invalid item, it's invalid, else we have to wait for more info
                            if self.invalid_init.contains(d.id()) {
                                return Ok(ValidationStatus::InvalidData(format!(
                                    "data event refers to stream with invalid init event (cid={})",
                                    d.id()
                                )));
                            } else {
                                return Ok(ValidationStatus::Pending);
                            }
                        }
                    }
                };

                match self.verify_signature(init, signed, opts).await {
                    Ok(_) => Ok(ValidationStatus::ValidData),
                    Err(err) => Ok(ValidationStatus::InvalidData(err)),
                }
            }
            unvalidated::Payload::Init(init) => {
                // we pass the same event for the `init`` and `signed`` parameters which is a bit silly, but it works fine
                match self
                    .verify_signature_against_init_controller(init, signed, opts)
                    .await
                {
                    Ok(_) => Ok(ValidationStatus::ValidInit),
                    Err(err) => Ok(ValidationStatus::InvalidInit(err)),
                }
            }
        }
    }

    /// The main entry point for validating a signed event's signatures for this struct.
    /// It will delegate to [`SignedValidator::verify_signature_against_init_controller`] to validate the cacao and event signatures.
    async fn verify_signature(
        &mut self,
        init: Arc<unvalidated::Event<Ipld>>,
        signed: &signed::Event<Ipld>,
        opts: &VerifyJwsOpts,
    ) -> std::result::Result<(), String> {
        match init.as_ref() {
            unvalidated::Event::Time(_) => {
                Err("init event pointer cannot reference a time event".to_string())
            }
            unvalidated::Event::Signed(s) => match s.payload() {
                unvalidated::Payload::Data(_d) => {
                    Err("init event pointer cannot reference a data event".to_string())
                }
                unvalidated::Payload::Init(init) => {
                    self.verify_signature_against_init_controller(init, signed, opts)
                        .await
                }
            },
            unvalidated::Event::Unsigned(init) => {
                self.verify_signature_against_init_controller(init.payload(), signed, opts)
                    .await
            }
        }
    }

    /// An extension to the [`SignedValidator::verify_signature`] function that requires the init event to make sure the controller of the
    /// new event is indeed the in the stream delegation chain. This should *almost* always be called, the only reason we
    /// support validating without the init event is that we migrate data from an old ipfs install out of order and don't
    /// want to pend everything in memory while try to put things back in order to verify signatures, so instead we make sure
    /// each event is well structured and correctly signed, assuming that the original protocol rules were followed.
    /// If we send invalid events to other nodes, they will ignore them or hang up on us, so it won't propagate around the network.
    async fn verify_signature_against_init_controller(
        &self,
        init: &init::Payload<Ipld>,
        signed: &signed::Event<Ipld>,
        opts: &VerifyJwsOpts,
    ) -> std::result::Result<(), String> {
        // TODO: should we error if controller is missing?
        let controller = init.header().controllers().first().cloned();
        match signed.capability() {
            Some((cid, cacao)) => {
                let init_cid = init.to_cid().map_err(|e| {
                    format!(
                        "failed to determine init event CID for data event={}. err={:#}",
                        signed.envelope_cid(),
                        e
                    )
                })?;
                let model_id = match StreamId::try_from(init.header().model()) {
                    Ok(m) => Some(m),
                    Err(e) => {
                        warn!(error=?e, %init_cid, "failed to parse init event model field as stream");
                        None
                    }
                };
                // TODO(AES-351): this assumes all streams are MIDs
                let stream_id = StreamId::document(init_cid);
                cacao
                    .verify_access(&stream_id, Some(*cid), model_id.as_ref())
                    .map_err(|e| e.to_string())?;
            }
            None => {
                // no cacao to verify so signature is sufficient
            }
        }
        signed
            .verify_signature(controller.as_deref(), opts)
            .await
            .map_err(|e| e.to_string())
    }
}
