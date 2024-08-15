use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use anyhow::Result;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub enum EventType {
    Signed(String), // Dummy event type
    Time(String),   // Dummy event type
}

#[derive(Debug)]
pub struct Event {
    pub event_type: EventType,
}

pub struct EventQueue {
    tx: mpsc::Sender<Event>,
    rx: Arc<Mutex<mpsc::Receiver<Event>>>,
}

impl EventQueue {
    pub fn new(buffer_size: usize) -> Self {
        let (tx, rx) = mpsc::channel(buffer_size);
        Self {
            tx,
            rx: Arc::new(Mutex::new(rx)),
        }
    }

    pub async fn add_event(&self, event: Event) -> Result<()> {
        self.tx.send(event).await?;
        Ok(())
    }

    pub async fn process_events(rx: Arc<Mutex<mpsc::Receiver<Event>>>, batch_size: usize, interval_ms: u64) -> Result<()> {
        let mut interval = time::interval(Duration::from_millis(interval_ms));
        let mut buffer = Vec::with_capacity(batch_size);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if !buffer.is_empty() {
                        Self::process_batch(&buffer).await?;
                        buffer.clear();
                    }
                }
                Some(event) = rx.lock().unwrap().recv() => {
                    buffer.push(event);
                    if buffer.len() >= batch_size {
                        Self::process_batch(&buffer).await?;
                        buffer.clear();
                    }
                }
                else => break,
            }
        }

        if !buffer.is_empty() {
            Self::process_batch(&buffer).await?;
        }

        Ok(())
    }

    async fn process_batch(events: &[Event]) -> Result<()> {
        for event in events {
            match &event.event_type {
                EventType::Signed(data) => {
                    println!("Processing signed event: {}", data);
                }
                EventType::Time(data) => {
                    println!("Processing time event: {}", data);
                }
            }
        }
        Ok(())
    }
}

lazy_static::lazy_static! {
    static ref EVENT_QUEUE: EventQueue = EventQueue::new(100);
}

pub fn get_event_sender() -> mpsc::Sender<Event> {
    EVENT_QUEUE.tx.clone()
}

pub fn get_event_receiver() -> Arc<Mutex<mpsc::Receiver<Event>>> {
    EVENT_QUEUE.rx.clone()
}