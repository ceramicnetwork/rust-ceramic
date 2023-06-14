//! Implementation of Recon over libp2p
//!
//! There are various types within this module and its children.
//!
//! Behavior - Responsible for coordinating the many concurrent Recon syncs
//! handler::Handler - Manages performing a Recon sync with a single peer
//! Recon - Manages the key space
//!
//! The Behavior and Handler communicate via message passing. The Behavior can instruct
//! the Handler to start a sync and the Handler reports to the behavior once it has
//! completed a sync.

mod handler;
mod protocol;
#[cfg(test)]
mod tests;

use libp2p::{
    core::ConnectedPoint,
    swarm::{ConnectionId, NetworkBehaviour, NotifyHandler, ToSwarm},
};
use libp2p_identity::PeerId;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    task::Poll,
    time::{Duration, Instant},
};
use tracing::{debug, warn};

use crate::{
    libp2p::handler::{FromBehaviour, FromHandler, Handler},
    recon::Response,
    AssociativeHash, Message, Sha256a,
};

/// Name of the Recon protocol
pub const PROTOCOL_NAME: &[u8] = b"/ceramic/recon/0.1.0";

/// Defines the Recon API.
pub trait Recon: Clone + Send + 'static {
    /// The specific Hash function to use.
    type Hash: AssociativeHash
        + std::fmt::Debug
        + Serialize
        + for<'de> Deserialize<'de>
        + Send
        + 'static;

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
    type Hash = Sha256a;

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

/// Config specifies the configurable properties of the Behavior.
#[derive(Clone, Debug)]
pub struct Config {
    /// Start a new sync once the duration has past in the failed or synchronized state.
    /// Defaults to 1 second.
    pub per_peer_sync_timeout: Duration,
    /// Duration to keep the connection alive, even when not in use.
    pub idle_keep_alive: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            per_peer_sync_timeout: Duration::from_millis(1000),
            idle_keep_alive: Duration::from_millis(1000),
        }
    }
}

/// Behaviour of Recon on the peer to peer network.
///
/// The Behavior tracks all peers on the network that speak the Recon protocol.
/// It is responsible for starting and stopping syncs with various peers depending on the needs of
/// the application.
#[derive(Debug)]
pub struct Behaviour<R> {
    recon: R,
    config: Config,
    peers: BTreeMap<PeerId, PeerInfo>,
    swarm_events_queue: VecDeque<Event>,
}

/// Information about a remote peer and its sync status.
#[derive(Clone, Debug)]
struct PeerInfo {
    status: PeerStatus,
    connection_id: ConnectionId,
    last_sync: Option<Instant>,
    dialer: bool,
}

/// Status of any synchronization operation with a remote peer.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum PeerStatus {
    /// Local peer has synchronized with the remote peer at one point.
    /// There is no ongoing sync operation with the remote peer.
    Synchronized,
    /// Local peer has started to synchronize with the remote peer.
    Started,
    /// The last attempt to synchronize with the remote peer resulted in an error.
    Failed,
    /// Local peer has stopped synchronizing with the remote peer and will not attempt to
    /// synchronize again.
    Stopped,
}

impl<R> Behaviour<R> {
    /// Create a new Behavior with the provided Recon implementation.
    pub fn new(recon: R, config: Config) -> Self
    where
        R: Recon,
    {
        Self {
            recon,
            config,
            peers: BTreeMap::new(),
            swarm_events_queue: VecDeque::new(),
        }
    }
}

impl<R: Recon> NetworkBehaviour for Behaviour<R> {
    type ConnectionHandler = Handler<R>;

    type OutEvent = Event;

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm<Self::ConnectionHandler>) {
        match event {
            libp2p::swarm::FromSwarm::ConnectionEstablished(info) => {
                let status = PeerStatus::Started;
                self.swarm_events_queue.push_front(Event::PeerEvent {
                    remote_peer_id: info.peer_id,
                    status,
                });
                self.peers.insert(
                    info.peer_id,
                    PeerInfo {
                        status,
                        connection_id: info.connection_id,
                        last_sync: None,
                        dialer: matches!(info.endpoint, ConnectedPoint::Dialer { .. }),
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
        peer_id: libp2p_identity::PeerId,
        _connection_id: libp2p::swarm::ConnectionId,
        event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
        match event {
            // The peer has started to synchronize with us.
            FromHandler::Started => self.peers.entry(peer_id).and_modify(|info| {
                if info.status != PeerStatus::Started {
                    info.status = PeerStatus::Started;
                    self.swarm_events_queue.push_front(Event::PeerEvent {
                        remote_peer_id: peer_id,
                        status: info.status,
                    })
                }
            }),
            // The peer has stopped synchronization and will never be able to resume.
            FromHandler::Stopped => self.peers.entry(peer_id).and_modify(|info| {
                info.status = PeerStatus::Stopped;
                self.swarm_events_queue.push_front(Event::PeerEvent {
                    remote_peer_id: peer_id,
                    status: info.status,
                })
            }),

            // The peer has synchronized with us, mark the time and record that the peer connection
            // is now idle.
            FromHandler::Succeeded => self.peers.entry(peer_id).and_modify(|info| {
                info.last_sync = Some(Instant::now());
                info.status = PeerStatus::Synchronized;
                self.swarm_events_queue.push_front(Event::PeerEvent {
                    remote_peer_id: peer_id,
                    status: info.status,
                })
            }),

            // The peer has failed to synchronized with us, mark the time and record that the peer connection
            // is now failed.
            FromHandler::Failed(error) => self.peers.entry(peer_id).and_modify(|info| {
                warn!(%peer_id, %error, "synchronization failed with peer");
                info.last_sync = Some(Instant::now());
                info.status = PeerStatus::Failed;
                self.swarm_events_queue.push_front(Event::PeerEvent {
                    remote_peer_id: peer_id,
                    status: info.status,
                })
            }),
        };
    }

    fn poll(
        &mut self,
        _cx: &mut std::task::Context<'_>,
        _params: &mut impl libp2p::swarm::PollParameters,
    ) -> std::task::Poll<libp2p::swarm::ToSwarm<Self::OutEvent, libp2p::swarm::THandlerInEvent<Self>>>
    {
        // Handle queue of swarm events.
        if let Some(event) = self.swarm_events_queue.pop_back() {
            debug!(?event, "swarm event");
            return Poll::Ready(ToSwarm::GenerateEvent(event));
        }
        // Check each peer and start synchronization as needed.
        for (peer_id, info) in &mut self.peers {
            debug!(%peer_id, ?info, "polling peer state");
            // Expected the initial dialer to initiate a new synchronization.
            if info.dialer {
                match info.status {
                    PeerStatus::Failed | PeerStatus::Synchronized => {
                        let should_sync = if let Some(last_sync) = &info.last_sync {
                            last_sync.elapsed() > self.config.per_peer_sync_timeout
                        } else {
                            false
                        };
                        if should_sync {
                            info.status = PeerStatus::Started;
                            self.swarm_events_queue.push_front(Event::PeerEvent {
                                remote_peer_id: *peer_id,
                                status: info.status,
                            });
                            return Poll::Ready(ToSwarm::NotifyHandler {
                                peer_id: *peer_id,
                                handler: NotifyHandler::One(info.connection_id),
                                event: FromBehaviour::StartSync,
                            });
                        }
                    }
                    PeerStatus::Started | PeerStatus::Stopped => {}
                }
            }
        }
        Poll::Pending
    }

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: libp2p::swarm::ConnectionId,
        peer: PeerId,
        _local_addr: &libp2p::Multiaddr,
        _remote_addr: &libp2p::Multiaddr,
    ) -> std::result::Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        debug!(%peer, ?connection_id, "handle_established_inbound_connection");
        Ok(Handler::new(
            peer,
            connection_id,
            handler::State::WaitingInbound,
            self.config.idle_keep_alive,
            self.recon.clone(),
        ))
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: libp2p::swarm::ConnectionId,
        peer: PeerId,
        _addr: &libp2p::Multiaddr,
        _role_override: libp2p::core::Endpoint,
    ) -> std::result::Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        debug!(%peer, ?connection_id, "handle_established_outbound_connection");
        Ok(Handler::new(
            peer,
            connection_id,
            handler::State::RequestOutbound,
            self.config.idle_keep_alive,
            self.recon.clone(),
        ))
    }
}

/// Events that the Behavior can emit to the rest of the application.
#[derive(Debug)]
pub enum Event {
    /// Event indicating we have synchronized with the specific peer.
    PeerEvent {
        /// Id of remote peer
        remote_peer_id: PeerId,

        /// New status of the peer
        status: PeerStatus,
    },
}

// Implement the conversion from Event to an iroh_p2p::Event.
impl<R: Recon + Clone + Send + 'static> From<Event> for iroh_p2p::behaviour::Event<Behaviour<R>> {
    fn from(value: Event) -> Self {
        Self::Custom(value)
    }
}
