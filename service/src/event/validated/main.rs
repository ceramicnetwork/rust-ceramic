use your_project::validated::lib::{get_event_receiver, EventQueue};
use anyhow::Result;
use tokio::task;

#[tokio::main]
async fn main() -> Result<()> {
    let event_receiver = get_event_receiver();

    // Process events in the queue
    task::spawn(async move {
        EventQueue::process_events(event_receiver, 5, 1000).await.unwrap();
    }).await.unwrap();

    Ok(())
}