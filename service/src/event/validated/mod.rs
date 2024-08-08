// mod.rs

use anyhow::Result;
use ceramic_core::{DidDocument, EventId};
use cid::Cid;
use serde::{Deserialize, Serialize};
use ssi::jwk::Algorithm;
use crate::unvalidated::{self, Payload, signed::Signer};

#[derive(Debug, Serialize, Deserialize)]
pub enum ValidatedEvent<D> {
    Init(ValidatedInitEvent<D>),
    Data(ValidatedDataEvent<D>),
    Time(ValidatedTimeEvent<D>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidatedInitEvent<D> {
    pub event_id: EventId,
    pub payload: Payload<D>,
    pub signature: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidatedDataEvent<D> {
    pub event_id: EventId,
    pub payload: Payload<D>,
    pub signature: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ValidatedTimeEvent<D> {
    pub event_id: EventId,
    pub payload: Payload<D>,
    pub signature: Vec<u8>,
}

pub trait ValidateEvent {
    fn validate_event(&self, event: &unvalidated::Event<Ipld>) -> Result<ValidatedEvent<Ipld>>;
}

pub struct EventValidator {
    // pub did_document: DidDocument,
    pub signer: DidDocument,
}

// pub struct EventQueue {
//     tx: mpsc::Sender<unvalidated::Event<Ipld>>,
//     rx: mpsc::Receiver<unvalidated::Event<Ipld>>,
// }

// impl EventQueue {
//     pub fn new(buffer_size: usize) -> Self {
//         let (tx, rx) = mpsc::channel(buffer_size);
//         Self { tx, rx }
//     }

//     pub async fn add_event(&self, event: unvalidated::Event<Ipld>) -> Result<()> {
//         self.tx.send(event).await?;
//         Ok(())
//     }

//     pub async fn process_events(&mut self, batch_size: usize, interval_ms: u64) -> Result<()> {
//         let mut interval = time::interval(Duration::from_millis(interval_ms));
//         let mut buffer = Vec::with_capacity(batch_size);

//         loop {
//             tokio::select! {
//                 _ = interval.tick() => {
//                     // Process the batch if the interval elapses
//                     if !buffer.is_empty() {
//                         self.process_batch(&buffer).await?;
//                         buffer.clear();
//                     }
//                 }
//                 Some(event) = self.rx.recv() => {
//                     // Add the event to the buffer
//                     buffer.push(event);
//                     // Process the batch if the buffer reaches the batch size
//                     if buffer.len() >= batch_size {
//                         self.process_batch(&buffer).await?;
//                         buffer.clear();
//                     }
//                 }
//                 else => break,
//             }
//         }

//         // Process any remaining events
//         if !buffer.is_empty() {
//             self.process_batch(&buffer).await?;
//         }

//         Ok(())
//     }

//     async fn process_batch(&self, events: &[unvalidated::Event<Ipld>]) -> Result<()> {
//         for event in events {
//             match event {
//                 unvalidated::Event::Signed(signed_event) => {
//                     // Validate the signed event
//                 }
//                 unvalidated::Event::Time(time_event) => {
//                     // Validate the time event
//                 }
//                 _ => {
//                     // Handle other event types
//                 }
//             }
//         }
//         Ok(())
//     }
// }

impl ValidateEvent for EventValidator {
    fn validate_event(&self, event: &unvalidated::Event<Ipld>) -> Result<ValidatedEvent<Ipld>> {
        match event {
            unvalidated::Event::Signed(signed_event) => {
               validate_signed_event(signed_event)
            }
            unvalidated::Event::Time(time_event) => {
                validate_time_event(time_event)
            }
            _ => Err(anyhow::anyhow!("Unsupported event type")),
        }
    }
}

// TODO: validate signed event
// TODO: validate time event
