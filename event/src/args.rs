use std::str::FromStr;

use anyhow::Result;
use ceramic_core::{Cid, Signer};
use once_cell::sync::Lazy;
use rand::Fill;
use serde::Serialize;

use crate::{
    bytes::Bytes,
    deterministic_init_event::DeterministicInitEvent,
    event::Event,
    unvalidated::{self, DataPayload, InitHeader, InitPayload},
    DidDocument, StreamId,
};

const SEP: &str = "model";

/// All models have this model StreamId, while all documents have the model stream id of the model
/// they use
pub static PARENT_STREAM_ID: Lazy<StreamId> =
    Lazy::new(|| StreamId::from_str("kh4q0ozorrgaq2mezktnrmdwleo1d").unwrap());

/// Arguments for creating an event
pub struct EventArgs<'a, S: Signer> {
    signer: &'a S,
    controllers: Vec<&'a DidDocument>,
    model: &'a StreamId,
    sep: &'static str,
}

impl<'a, S: Signer> EventArgs<'a, S> {
    /// Create a new event args object from a DID document
    pub fn new(signer: &'a S) -> Self {
        Self {
            signer,
            controllers: vec![signer.id()],
            model: &PARENT_STREAM_ID,
            sep: SEP,
        }
    }

    /// Create a new event args object from a DID document and a model stream id
    pub fn new_with_model(signer: &'a S, model: &'a StreamId) -> Self {
        Self {
            signer,
            controllers: vec![signer.id()],
            model,
            sep: SEP,
        }
    }

    /// Model stream id
    pub fn model(&self) -> &StreamId {
        self.model
    }

    /// Create an init event from these arguments
    pub fn init(&self) -> Result<DeterministicInitEvent> {
        let evt = create_unsigned_event(self, CreateArgs::<()>::Init)?;
        let evt = DeterministicInitEvent::new(&evt)?;
        Ok(evt)
    }

    /// Create an init event from these arguments with a unique header
    pub fn init_with_unique(&self) -> Result<DeterministicInitEvent> {
        let evt = create_unsigned_event(
            self,
            CreateArgs::<()>::InitWithUnique {
                unique: create_unique()?,
            },
        )?;
        let evt = DeterministicInitEvent::new(&evt)?;
        Ok(evt)
    }

    /// Create an init event from these arguments with data
    pub async fn init_with_data<T: Serialize>(&self, data: &T) -> Result<Event> {
        let evt = create_unsigned_event(
            self,
            CreateArgs::InitWithData {
                data,
                unique: create_unique()?,
            },
        )?;
        let evt = Event::new(evt, self.signer).await?;
        Ok(evt)
    }

    /// Create an update event from these arguments
    pub async fn update<T: Serialize>(&self, current: &Cid, prev: &Cid, data: &T) -> Result<Event> {
        let evt = create_unsigned_event(
            self,
            CreateArgs::Update {
                current: *current,
                prev: *prev,
                data,
            },
        )?;
        let evt = Event::new(evt, self.signer).await?;
        Ok(evt)
    }

    /// Controllers for this event
    pub fn controllers(&self) -> impl Iterator<Item = &&DidDocument> {
        self.controllers.iter()
    }
}

fn create_unique() -> Result<Bytes> {
    let mut rng = rand::thread_rng();
    let mut unique = [0u8; 12];
    unique.try_fill(&mut rng)?;
    Ok(unique.to_vec().into())
}

enum CreateArgs<T> {
    Init,
    InitWithUnique { unique: Bytes },
    InitWithData { data: T, unique: Bytes },
    Update { current: Cid, prev: Cid, data: T },
}

fn create_unsigned_event<S: Signer, T: Serialize>(
    event_args: &EventArgs<S>,
    create_args: CreateArgs<T>,
) -> Result<unvalidated::Payload<T>> {
    let controllers: Vec<_> = event_args
        .controllers
        .iter()
        .map(|d| d.id.to_string())
        .collect();
    match create_args {
        CreateArgs::Init => Ok(unvalidated::Payload::Init(unvalidated::InitPayload::new(
            InitHeader::new(
                controllers,
                event_args.sep.to_string(),
                event_args.model.to_vec()?.into(),
                None,
            ),
            None,
        ))),
        CreateArgs::InitWithUnique { unique } => Ok(unvalidated::Payload::Init(InitPayload::new(
            InitHeader::new(
                controllers,
                event_args.sep.to_string(),
                event_args.model.to_vec()?.into(),
                Some(unique),
            ),
            None,
        ))),
        CreateArgs::InitWithData { data, unique } => {
            Ok(unvalidated::Payload::Init(InitPayload::new(
                InitHeader::new(
                    controllers,
                    event_args.sep.to_string(),
                    event_args.model.to_vec()?.into(),
                    Some(unique),
                ),
                Some(data),
            )))
        }
        CreateArgs::Update {
            current,
            prev,
            data,
        } => Ok(unvalidated::Payload::Data(DataPayload::new(
            current, prev, None, data,
        ))),
    }
}
