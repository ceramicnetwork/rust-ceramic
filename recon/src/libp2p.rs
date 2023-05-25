//! Implementation of Recon over libp2p
//!
//! There are various types within this module and its children.
//!
//! Behaviour - Responsible for coordiating the many concurrent Recon syncs
//! handler::Handler - Manages performing a Recon sync with a single peer
//! Recon - Manages the key space
//!
//! The Behaviour and Handler communicate via message passing. The Behaviour can instruct
//! the Handler to start a sync and the Handler reports to the behaviour once it has
//! completed a sync.

mod handler;
mod protocol;

use libp2p::swarm::{ConnectionId, NetworkBehaviour, NotifyHandler, ToSwarm};
use libp2p_identity::PeerId;
use rand::{self, distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    task::Poll,
    time::{Duration, Instant},
};
use tracing::debug;

use crate::{
    libp2p::handler::{FromBehaviour, FromHandler, Handler},
    recon::Response,
    AHash, Hash, Message,
};

/// Name of the Recon protocol
pub const PROTOCOL_NAME: &[u8] = b"/ceramic/recon/0.1.0";

/// Defines the Recon API.
pub trait Recon {
    /// The specific Hash function to use.
    type Hash: Hash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de> + Send + 'static;
    /// Construct a message to send as the first message.
    fn initial_message(&self) -> Message<Self::Hash>;
    /// Process an incoming message and respond with a message reply.
    fn process_message(&mut self, msg: &Message<Self::Hash>) -> Response<Self::Hash>;
    /// Insert a new key into the key space.
    fn insert_key(&mut self, key: &str);
}

// Implement the  Recon trait using crate::recon::Recon
//
// NOTE: We use a std::sync::Mutex because we are not doing any async
// logic within Recon itself, all async logic exists outside its scope.
// We should use a tokio::sync::Mutex if we introduce any async logic into Recon.
impl Recon for Arc<Mutex<crate::recon::Recon>> {
    type Hash = AHash;

    fn insert_key(&mut self, key: &str) {
        self.lock()
            .expect("should be able to acquire lock")
            .insert(key)
    }
    fn initial_message(&self) -> Message<Self::Hash> {
        self.lock()
            .expect("should be able to acquire lock")
            .first_message()
    }

    fn process_message(&mut self, msg: &Message<Self::Hash>) -> Response<Self::Hash> {
        self.lock()
            .expect("should be able to acquire lock")
            .process_message(msg)
    }
}

/// Behaviour of Recon on the peer to peer network.
///
/// The Behaviour tracks all peers on the network that speak the Recon protocol.
/// It is reponsible for starting and stoping syncs with various peers depending on the needs of
/// the application.
pub struct Behaviour<R> {
    recon: R,
    peers: BTreeMap<PeerId, PeerInfo>,
}

/// Information about a specific peer and its sync status.
struct PeerInfo {
    status: PeerStatus,
    connection_id: ConnectionId,
    last_sync: Option<Instant>,
}

#[derive(PartialEq)]
enum PeerStatus {
    Idle,
    Started,
}

impl<R> Behaviour<R> {
    /// Create a new Behavior with the provided Recon implementation.
    pub fn new(recon: R) -> Self
    where
        R: Recon,
    {
        Self {
            recon,
            peers: BTreeMap::new(),
        }
    }
}

impl<R: Recon + Clone + Send + 'static> NetworkBehaviour for Behaviour<R> {
    type ConnectionHandler = Handler<R>;

    type OutEvent = Event;

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm<Self::ConnectionHandler>) {
        match event {
            libp2p::swarm::FromSwarm::ConnectionEstablished(info) => {
                self.peers.insert(
                    info.peer_id,
                    PeerInfo {
                        status: PeerStatus::Idle,
                        connection_id: info.connection_id,
                        last_sync: None,
                    },
                );
            }
            libp2p::swarm::FromSwarm::ConnectionClosed(info) => {
                self.peers.remove(&info.peer_id);
            }
            libp2p::swarm::FromSwarm::AddressChange(_) => {}
            libp2p::swarm::FromSwarm::DialFailure(_) => {}
            libp2p::swarm::FromSwarm::ListenFailure(_) => {}
            libp2p::swarm::FromSwarm::NewListener(_) => {}
            libp2p::swarm::FromSwarm::NewListenAddr(_) => {}
            libp2p::swarm::FromSwarm::ExpiredListenAddr(_) => {}
            libp2p::swarm::FromSwarm::ListenerError(_) => {}
            libp2p::swarm::FromSwarm::ListenerClosed(_) => {}
            libp2p::swarm::FromSwarm::NewExternalAddr(_) => {}
            libp2p::swarm::FromSwarm::ExpiredExternalAddr(_) => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: iroh_p2p::PeerId,
        _connection_id: libp2p::swarm::ConnectionId,
        event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
        match event {
            // The peer has synchronized with us, mark the time and record that the peer conneciton
            // is now idle.
            FromHandler::PeerSynchronized => self.peers.entry(peer_id).and_modify(|info| {
                info.last_sync = Some(Instant::now());
                info.status = PeerStatus::Idle;
            }),
        };
    }

    fn poll(
        &mut self,
        _cx: &mut std::task::Context<'_>,
        _params: &mut impl libp2p::swarm::PollParameters,
    ) -> std::task::Poll<libp2p::swarm::ToSwarm<Self::OutEvent, libp2p::swarm::THandlerInEvent<Self>>>
    {
        // Check each peer and start synchronization as needed.
        for (peer_id, info) in &mut self.peers {
            match info.status {
                PeerStatus::Idle => {
                    let should_sync = if let Some(last_sync) = &info.last_sync {
                        last_sync.elapsed() > Duration::from_millis(1000)
                    } else {
                        true
                    };
                    if should_sync {
                        info.status = PeerStatus::Started;
                        return Poll::Ready(ToSwarm::NotifyHandler {
                            peer_id: *peer_id,
                            handler: NotifyHandler::One(info.connection_id),
                            event: FromBehaviour::StartSync,
                        });
                    }
                }
                PeerStatus::Started => {}
            }
        }
        // TODO remove this demo code
        // Randomly insert a new key into the Recon key space.
        if rand::thread_rng().gen::<f64>() > 0.9 {
            debug!("adding new key");
            let s: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(12)
                .map(char::from)
                .collect();
            self.recon.insert_key(&s);
        }
        Poll::Pending
    }

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: libp2p::swarm::ConnectionId,
        peer: PeerId,
        _local_addr: &libp2p::Multiaddr,
        _remote_addr: &libp2p::Multiaddr,
    ) -> std::result::Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        debug!(%peer, "handle_established_inbound_connection");
        Ok(Handler::new(self.recon.clone()))
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: libp2p::swarm::ConnectionId,
        peer: PeerId,
        _addr: &libp2p::Multiaddr,
        _role_override: libp2p::core::Endpoint,
    ) -> std::result::Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        debug!(%peer, "handle_established_outbound_connection");
        Ok(Handler::new(self.recon.clone()))
    }
}

/// Events that the Behaviour can emit to the rest of the application.
pub struct Event;

// Implement the converstion from Event to an iroh_p2p::Event.
impl<R: Recon + Clone + Send + 'static> From<Event> for iroh_p2p::behaviour::Event<Behaviour<R>> {
    fn from(value: Event) -> Self {
        Self::Custom(value)
    }
}
