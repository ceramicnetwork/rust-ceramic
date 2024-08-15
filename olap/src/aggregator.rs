use std::future::Future;

use anyhow::Result;

pub async fn run(shutdown_signal: impl Future<Output = ()>) -> Result<()> {
    shutdown_signal.await;
    Ok(())
}
