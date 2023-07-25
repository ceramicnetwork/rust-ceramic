use anyhow::Result;
use tracing::info;

use ceramic_core::{EventId, Interest, Network, PeerId};

use crate::server::Recon;

mod server;

pub async fn start(
    peer_id: PeerId,
    network: Network,
    addr: String,
    interest: impl Recon<Key = Interest>,
    model: impl Recon<Key = EventId>,
) -> Result<()> {
    info!("starting ceramic api server");
    server::create(peer_id, network, &addr, interest, model).await;
    Ok(())
}
