use iroh_bitswap::BitswapEvent;
use libp2p::{autonat, dcutr, identify, kad, mdns, ping, relay};

use super::ceramic_peer_manager::PeerManagerEvent;

/// Event type which is emitted from the [`NodeBehaviour`].
///
/// [`NodeBehaviour`]: crate::behaviour::NodeBehaviour
#[derive(Debug)]
pub enum Event {
    Ping(ping::Event),
    Identify(Box<identify::Event>),
    Kademlia(kad::Event),
    Mdns(mdns::Event),
    Bitswap(BitswapEvent),
    Autonat(autonat::Event),
    Relay(relay::Event),
    RelayClient(relay::client::Event),
    Dcutr(dcutr::Event),
    PeerManager(PeerManagerEvent),
    Recon(recon::libp2p::Event),
    Void,
}

impl From<ping::Event> for Event {
    fn from(event: ping::Event) -> Self {
        Event::Ping(event)
    }
}

impl From<identify::Event> for Event {
    fn from(event: identify::Event) -> Self {
        Event::Identify(Box::new(event))
    }
}

impl From<kad::Event> for Event {
    fn from(event: kad::Event) -> Self {
        Event::Kademlia(event)
    }
}

impl From<mdns::Event> for Event {
    fn from(event: mdns::Event) -> Self {
        Event::Mdns(event)
    }
}

impl From<BitswapEvent> for Event {
    fn from(event: BitswapEvent) -> Self {
        Event::Bitswap(event)
    }
}

impl From<autonat::Event> for Event {
    fn from(event: autonat::Event) -> Self {
        Event::Autonat(event)
    }
}

impl From<relay::Event> for Event {
    fn from(event: relay::Event) -> Self {
        Event::Relay(event)
    }
}

impl From<relay::client::Event> for Event {
    fn from(event: relay::client::Event) -> Self {
        Event::RelayClient(event)
    }
}

impl From<dcutr::Event> for Event {
    fn from(event: dcutr::Event) -> Self {
        Event::Dcutr(event)
    }
}

impl From<PeerManagerEvent> for Event {
    fn from(event: PeerManagerEvent) -> Self {
        Event::PeerManager(event)
    }
}
impl From<void::Void> for Event {
    fn from(_event: void::Void) -> Self {
        Event::Void
    }
}
impl From<recon::libp2p::Event> for Event {
    fn from(value: recon::libp2p::Event) -> Self {
        Event::Recon(value)
    }
}
