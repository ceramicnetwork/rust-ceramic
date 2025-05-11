use ceramic_event::unvalidated::{self, signed};
use ipld_core::ipld::Ipld;

use super::{UnvalidatedEvent, ValidatedEvent};

#[derive(Debug)]
pub enum ValidationNeeded {
    SignedInit(SignedInit),
    SignedData(SignedData),
    Time(Time),
    None(Unsigned),
}

impl From<UnvalidatedEvent> for ValidationNeeded {
    fn from(value: UnvalidatedEvent) -> Self {
        match value.event.as_ref() {
            unvalidated::Event::Time(_) => Self::Time(Time(value)),
            unvalidated::Event::Signed(signed) => match signed.payload() {
                unvalidated::Payload::Data(_) => Self::SignedData(SignedData(value)),
                unvalidated::Payload::Init(_) => Self::SignedInit(SignedInit(value)),
            },
            unvalidated::Event::Unsigned(_) => Self::None(Unsigned(value)),
        }
    }
}

/// This is a simple macro to create a tuple struct around UnvalidatedEvent and allow access
/// to the inner value. It's purpose is to support implementing `From<UnvalidatedEvent> for ValidationNeeded`
/// and get the correct structs that can be required in fn signatures, but still dereferenced as the
/// original event type without having to match on all the branches when we require it to be a specific variant.
macro_rules! unvalidated_wrapper {
    ($name: ident) => {
        #[derive(Debug)]
        pub(crate) struct $name(UnvalidatedEvent);

        impl $name {
            #[allow(dead_code)]
            /// Get the inner tuple struct value
            pub fn into_inner(self) -> UnvalidatedEvent {
                self.0
            }

            #[allow(dead_code)]
            /// Get the inner tuple struct value by reference
            pub fn as_inner(&self) -> &UnvalidatedEvent {
                &self.0
            }
        }

        impl From<$name> for ValidatedEvent {
            // We wrap the unchecked here since we expect these to get validated and it makes it easy
            fn from(value: $name) -> Self {
                Self::from_unvalidated_unchecked(value.into_inner())
            }
        }
    };
}

/// Create an as `as_signed` function that panics for unsigned events.
/// Will be derived on our signed structs to
macro_rules! as_signed {
    ($name: ident) => {
        impl $name {
            pub fn as_signed(&self) -> &signed::Event<Ipld> {
                match self.0.event.as_ref() {
                    unvalidated::Event::Time(_) => unreachable!("time event is not signed"),
                    unvalidated::Event::Signed(s) => s,
                    unvalidated::Event::Unsigned(_) => unreachable!("unsigned event is not signed"),
                }
            }
        }
    };
}

impl Time {
    #[allow(dead_code)]
    pub fn as_time(&self) -> &unvalidated::TimeEvent {
        match self.0.event.as_ref() {
            unvalidated::Event::Time(t) => t,
            unvalidated::Event::Signed(_) => unreachable!("signed event event is not time"),
            unvalidated::Event::Unsigned(_) => unreachable!("unsigned event is not time"),
        }
    }
}

// Generate the tuple structs to use in the `ValidationNeeded` enum variants
unvalidated_wrapper!(SignedData);
unvalidated_wrapper!(SignedInit);
unvalidated_wrapper!(Time);
unvalidated_wrapper!(Unsigned);

// Add `as_signed(&self) -> &signed::Event<Ipld>` functions to the signed types
as_signed!(SignedData);
as_signed!(SignedInit);

#[derive(Debug)]
pub struct GroupedEvents {
    pub time_batch: TimeValidationBatch,
    pub signed_batch: SignedValidationBatch,
}

#[derive(Debug)]
pub struct TimeValidationBatch(pub(crate) Vec<Time>);

#[derive(Debug)]
pub struct SignedValidationBatch {
    pub data: Vec<SignedData>,
    pub init: Vec<SignedInit>,
    /// Possibly needed to verify data event controllers as these
    /// don't yet exist on disk, but the signatures aren't checked.
    pub unsigned: Vec<Unsigned>,
}

impl From<SignedValidationBatch> for Vec<ValidatedEvent> {
    fn from(value: SignedValidationBatch) -> Self {
        let mut events =
            Vec::with_capacity(value.data.len() + value.init.len() + value.unsigned.len());
        events.extend(value.data.into_iter().map(Into::into));
        events.extend(value.init.into_iter().map(Into::into));
        events.extend(value.unsigned.into_iter().map(Into::into));
        events
    }
}

impl From<Vec<UnvalidatedEvent>> for GroupedEvents {
    fn from(value: Vec<UnvalidatedEvent>) -> Self {
        let mut grouped = GroupedEvents::new(value.len() / 2);

        value
            .into_iter()
            .for_each(|v| grouped.add(ValidationNeeded::from(v)));
        grouped
    }
}

impl GroupedEvents {
    fn new(capacity: usize) -> Self {
        Self {
            time_batch: TimeValidationBatch(Vec::with_capacity(capacity)),
            signed_batch: SignedValidationBatch {
                data: Vec::with_capacity(capacity),
                init: Vec::with_capacity(capacity),
                unsigned: Vec::with_capacity(capacity),
            },
        }
    }

    fn add(&mut self, new: ValidationNeeded) {
        match new {
            ValidationNeeded::SignedInit(v) => self.signed_batch.init.push(v),
            ValidationNeeded::SignedData(v) => self.signed_batch.data.push(v),
            ValidationNeeded::Time(v) => self.time_batch.0.push(v),
            ValidationNeeded::None(v) => self.signed_batch.unsigned.push(v),
        }
    }
}
