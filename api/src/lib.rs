use anyhow::Result;
use tracing::info;

use ceramic_core::{EventId, Network};

use crate::server::Recon;

mod server;

pub async fn start(network: Network, addr: String, recon: impl Recon<Key = EventId>) -> Result<()> {
    info!("starting ceramic api server");
    server::create(network, &addr, recon).await;
    Ok(())
}
