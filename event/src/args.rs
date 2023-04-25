use crate::deterministic_init_event::DeterministicInitEvent;
use crate::event::Event;
use crate::{DidDocument, StreamId};

use anyhow::Result;
use ceramic_core::{Bytes, Cid, Jwk};
use once_cell::sync::Lazy;
use rand::Fill;
use serde::Serialize;
use std::str::FromStr;

const SEP: &str = "model";

/// All models have this parent StreamId, while all documents have the parent stream id of the model
/// they use
pub static PARENT_STREAM_ID: Lazy<StreamId> =
    Lazy::new(|| StreamId::from_str("kh4q0ozorrgaq2mezktnrmdwleo1d").unwrap());

/// Arguments for creating an event
pub struct EventArgs<'a> {
    signer: &'a DidDocument,
    controllers: Vec<&'a DidDocument>,
    parent: &'a StreamId,
    sep: &'a str,
}

impl<'a> EventArgs<'a> {
    /// Create a new event args object from a DID document
    pub fn new(signer: &'a DidDocument) -> Self {
        Self {
            signer,
            controllers: vec![signer],
            parent: &PARENT_STREAM_ID,
            sep: SEP,
        }
    }

    /// Create a new event args object from a DID document and a parent stream id
    pub fn new_with_parent(signer: &'a DidDocument, parent: &'a StreamId) -> Self {
        Self {
            signer,
            controllers: vec![signer],
            parent,
            sep: SEP,
        }
    }

    /// Parent stream id
    pub fn parent(&self) -> &StreamId {
        self.parent
    }

    /// Create an init event from these arguments
    pub fn init(&self) -> Result<DeterministicInitEvent> {
        let evt = UnsignedEvent::<()>::init(self)?;
        let evt = DeterministicInitEvent::new(&evt)?;
        Ok(evt)
    }

    /// Create an init event from these arguments with data
    pub async fn init_with_data<T: Serialize>(&self, data: &T, private_key: &str) -> Result<Event> {
        let jwk = Jwk::new(self.signer).await?;
        let jwk = jwk.with_private_key(private_key)?;
        let evt = UnsignedEvent::init_with_data(self, data)?;
        let evt = Event::new(&evt, self.signer, &jwk).await?;
        Ok(evt)
    }

    /// Create an update event from these arguments
    pub async fn update<T: Serialize>(
        &self,
        data: &T,
        private_key: &str,
        prev: &Cid,
    ) -> Result<Event> {
        let jwk = Jwk::new(self.signer).await?;
        let jwk = jwk.with_private_key(private_key)?;
        let evt = UnsignedEvent::update(self, data, prev)?;
        let evt = Event::new(&evt, self.signer, &jwk).await?;
        Ok(evt)
    }

    /// Controllers for this event
    pub fn controllers(&self) -> impl Iterator<Item = &&DidDocument> {
        self.controllers.iter()
    }
}

#[derive(Serialize)]
struct UnsignedEventHeader<'a> {
    controllers: Vec<&'a str>,
    #[serde(rename = "model")]
    parent: Bytes,
    sep: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    unique: Option<Vec<u32>>,
}

/// An unsigned event, which can be used to create a events
#[derive(Serialize)]
pub struct UnsignedEvent<'a, T: Serialize> {
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<&'a T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    header: Option<UnsignedEventHeader<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prev: Option<&'a Cid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<&'a Cid>,
}

impl<'a, T: Serialize> UnsignedEvent<'a, T> {
    /// Initialize a new unsigned event from event arguments
    pub fn init(args: &'a EventArgs) -> Result<Self> {
        create_unsigned_event(args, CreateArgs::Init)
    }

    /// Initialize a new unsigned event from event arguments with data
    pub fn init_with_data(args: &'a EventArgs, data: &'a T) -> Result<Self> {
        let mut rng = rand::thread_rng();
        let mut unique = [0u32; 12];
        unique.try_fill(&mut rng)?;
        create_unsigned_event(
            args,
            CreateArgs::InitWithData {
                data,
                unique: unique.to_vec(),
            },
        )
    }

    /// Create an update unsigned event from event arguments
    pub fn update(args: &'a EventArgs, data: &'a T, prev: &'a Cid) -> Result<Self> {
        create_unsigned_event(args, CreateArgs::Update { data, prev })
    }
}

enum CreateArgs<'a, T: Serialize> {
    Init,
    InitWithData { data: &'a T, unique: Vec<u32> },
    Update { data: &'a T, prev: &'a Cid },
}

fn create_unsigned_event<'a, T: Serialize>(
    event_args: &'a EventArgs<'a>,
    create_args: CreateArgs<'a, T>,
) -> Result<UnsignedEvent<'a, T>> {
    let controllers: Vec<&str> = event_args
        .controllers
        .iter()
        .map(|d| d.id.as_ref())
        .collect();
    let (header, data, prev) = match create_args {
        CreateArgs::Init => (
            Some(UnsignedEventHeader {
                controllers,
                parent: event_args.parent.try_into()?,
                sep: event_args.sep,
                unique: None,
            }),
            None,
            None,
        ),
        CreateArgs::InitWithData { data, unique } => (
            Some(UnsignedEventHeader {
                controllers,
                parent: event_args.parent.try_into()?,
                sep: event_args.sep,
                unique: Some(unique),
            }),
            Some(data),
            None,
        ),
        CreateArgs::Update { data, prev } => (None, Some(data), Some(prev)),
    };
    Ok(UnsignedEvent {
        data,
        header,
        prev,
        id: prev,
    })
}
