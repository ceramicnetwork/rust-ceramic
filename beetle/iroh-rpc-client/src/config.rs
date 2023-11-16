use iroh_rpc_types::{gateway::GatewayAddr, p2p::P2pAddr, store::StoreAddr};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
/// Config for the rpc Client.
pub struct Config {
    /// Gateway rpc address.
    pub gateway_addr: Option<GatewayAddr>,
    /// P2p rpc address.
    pub p2p_addr: Option<P2pAddr>,
    /// Store rpc address.
    pub store_addr: Option<StoreAddr>,
    /// Number of concurent channels.
    ///
    /// If `None` defaults to `1`, not used for in-memory addresses.
    // TODO: Consider changing this to NonZeroUsize instead of Option<usize>.
    pub channels: Option<usize>,
}

impl Config {
    pub fn default_network() -> Self {
        Self {
            gateway_addr: Some("irpc://127.0.0.1:4400".parse().unwrap()),
            p2p_addr: Some("irpc://127.0.0.1:4401".parse().unwrap()),
            store_addr: Some("irpc://127.0.0.1:4402".parse().unwrap()),
            // disable load balancing by default by just having 1 channel
            channels: Some(1),
        }
    }
}
