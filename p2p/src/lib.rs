pub mod behaviour;
pub mod config;
mod keys;
mod metrics;
mod node;
mod peers;
mod providers;
pub mod rpc;
mod swarm;

pub use self::behaviour::Event;
pub use self::config::*;
pub use self::keys::{DiskStorage, Keychain, MemoryStorage};
pub use self::metrics::Metrics;
pub use self::node::*;
pub use libp2p::PeerId;
pub use peers::{PeerKeyInterests, PeerService};

pub(crate) const VERSION: &str = env!("CARGO_PKG_VERSION");
