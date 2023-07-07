use anyhow::Result;
use tracing::info;

use ceramic_core::Network;

use crate::server::Recon;

mod server;

pub async fn start(network: Network, addr: String, recon: impl Recon) -> Result<()> {
    info!("starting ceramic api server");
    server::create(network, &addr, recon).await;
    Ok(())
}
