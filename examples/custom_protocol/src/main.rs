use libp2p::{
    futures::StreamExt,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
};
use libp2p::{identity, Multiaddr, PeerId};
use std::error::Error;
use tracing::info;

use ceramic::set_rec;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    init_tracing();

    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    info!("Local peer id: {local_peer_id:?}");

    let transport = libp2p::tokio_development_transport(local_key)?;

    let mut swarm = Swarm::with_tokio_executor(transport, Behaviour::default(), local_peer_id);

    // Tell the swarm to listen on all interfaces and a random, OS-assigned
    // port.
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    // Dial the peer identified by the multi-address given as the second
    // command-line argument, if any.
    if let Some(addr) = std::env::args().nth(1) {
        let remote: Multiaddr = addr.parse()?;
        swarm.dial(remote)?;
        info!("Dialed {addr}")
    }

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => info!("Listening on {address:?}"),
            SwarmEvent::Behaviour(_event) => {}
            _ => {}
        }
    }
}

/// Our network behaviour.
#[derive(NetworkBehaviour, Default)]
struct Behaviour {
    set_rec: set_rec::Behaviour,
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    info!("tracing initialized");

