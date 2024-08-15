use std::future::Future;

use anyhow::Result;
use datafusion::execution::context::SessionContext;

pub async fn run(shutdown_signal: impl Future<Output = ()>) -> Result<()> {
    // Create datafusion context
    let _ctx = SessionContext::new();

    shutdown_signal.await;
    Ok(())
}
