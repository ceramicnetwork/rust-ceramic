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
mod stream_set;
#[cfg(test)]
mod tests;
mod upgrade;

use ceramic_core::{EventId, Interest};
use libp2p::{
    core::ConnectedPoint,
    swarm::{ConnectionId, NetworkBehaviour, NotifyHandler, ToSwarm},
};
use libp2p_identity::PeerId;
use std::{
    collections::{btree_map::Entry, BTreeMap},
    task::Poll,
    time::{Duration, Instant},
};
use tracing::{debug, trace, warn};

pub use crate::protocol::Recon;
use crate::{
    libp2p::{
        handler::{FromBehaviour, FromHandler, Handler},
        stream_set::StreamSet,
    },
    Sha256a,
};

/// Name of the Recon protocol for synchronizing interests
pub const PROTOCOL_NAME_INTEREST: &str = "/ceramic/recon/0.1.0/interest";
/// Name of the Recon protocol for synchronizing models
pub const PROTOCOL_NAME_MODEL: &str = "/ceramic/recon/0.1.0/model";

/// Config specifies the configurable properties of the Behavior.
#[derive(Clone, Debug)]
pub struct Config {
    /// Start a new sync once the duration has past in the failed or synchronized state.
    /// Defaults to 1 second.
    pub per_peer_sync_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            per_peer_sync_timeout: Duration::from_millis(1000),
        }
    }
}

/// Behaviour of Recon on the peer to peer network.
///
/// The Behavior tracks all peers on the network that speak the Recon protocol.
/// It is responsible for starting and stopping syncs with various peers depending on the needs of
/// the application.
#[derive(Debug)]
pub struct Behaviour<I, M> {
    interest: I,
    model: M,
    config: Config,
    peers: BTreeMap<PeerId, PeerInfo>,
    swarm_events_sender: tokio::sync::mpsc::Sender<ToSwarm<Event, FromBehaviour>>,
    swarm_events_receiver: tokio::sync::mpsc::Receiver<ToSwarm<Event, FromBehaviour>>,
}

/// Information about a remote peer and its sync status.
#[derive(Clone, Debug)]
struct PeerInfo {
    status: PeerStatus,
    connections: Vec<ConnectionInfo>,
    last_sync: Option<Instant>,
}

#[derive(Clone, Copy, Debug)]
struct ConnectionInfo {
    id: ConnectionId,
    dialer: bool,
}

/// Status of any synchronization operation with a remote peer.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum PeerStatus {
    /// Waiting on remote peer
    Waiting,
    /// Local peer has synchronized with the remote peer at one point.
    /// There is no ongoing sync operation with the remote peer.
    Synchronized {
        /// The stream_set that was synchronized
        stream_set: StreamSet,
    },
    /// Local peer has started to synchronize with the remote peer.
    Started {
        /// The stream_set that has begun synchronizing.
        stream_set: StreamSet,
    },
    /// The last attempt to synchronize with the remote peer resulted in an error.
    Failed,
    /// Local peer has stopped synchronizing with the remote peer and will not attempt to
    /// synchronize again.
    Stopped,
}

impl<I, M> Behaviour<I, M> {
    /// Create a new Behavior with the provided Recon implementation.
    pub fn new(interest: I, model: M, config: Config) -> Self
    where
        I: Recon<Key = Interest, Hash = Sha256a>,
        M: Recon<Key = EventId, Hash = Sha256a>,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        Self {
            interest,
            model,
            config,
            peers: BTreeMap::new(),
            swarm_events_sender: tx,
            swarm_events_receiver: rx,
        }
    }

    fn send_event(&self, event: ToSwarm<Event, FromBehaviour>) {
        let tx = self.swarm_events_sender.clone();
        let _ = tokio::task::block_in_place(move || {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async move { tx.send(event).await })
        });
    }
}

impl<I, M> NetworkBehaviour for Behaviour<I, M>
where
    I: Recon<Key = Interest, Hash = Sha256a>,
    M: Recon<Key = EventId, Hash = Sha256a>,
{
    type ConnectionHandler = Handler<I, M>;

    type ToSwarm = Event;

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm) {
        match event {
            libp2p::swarm::FromSwarm::ConnectionEstablished(info) => {
                trace!(?info, "connection established for peer");
                let connection_info = ConnectionInfo {
                    id: info.connection_id,
                    dialer: matches!(info.endpoint, ConnectedPoint::Dialer { .. }),
                };
                self.peers
                    .entry(info.peer_id)
                    .and_modify(|peer_info| peer_info.connections.push(connection_info))
                    .or_insert_with(|| PeerInfo {
                        status: PeerStatus::Waiting,
                        connections: vec![connection_info],
                        last_sync: None,
                    });
            }
            libp2p::swarm::FromSwarm::ConnectionClosed(info) => {
                self.peers.remove(&info.peer_id);
            }
            event => {
                trace!("ignored swarm event {:?}", event)
            }
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: libp2p_identity::PeerId,
        _connection_id: libp2p::swarm::ConnectionId,
        event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
        let ev = match event {
            // The peer has started to synchronize with us.
            FromHandler::Started { stream_set } => {
                if let Entry::Occupied(mut entry) = self.peers.entry(peer_id) {
                    let info = entry.get_mut();
                    info.status = PeerStatus::Started { stream_set };
                    Some(ToSwarm::GenerateEvent(Event::PeerEvent(PeerEvent {
                        remote_peer_id: peer_id,
                        status: info.status,
                    })))
                } else {
                    None
                }
            }
            // The peer has stopped synchronization and will never be able to resume.
            FromHandler::Stopped => {
                if let Entry::Occupied(mut entry) = self.peers.entry(peer_id) {
                    let info = entry.get_mut();
                    info.status = PeerStatus::Stopped;
                    Some(ToSwarm::GenerateEvent(Event::PeerEvent(PeerEvent {
                        remote_peer_id: peer_id,
                        status: info.status,
                    })))
                } else {
                    None
                }
            }

            // The peer has synchronized with us, mark the time and record that the peer connection
            // is now idle.
            FromHandler::Succeeded { stream_set } => {
                if let Entry::Occupied(mut entry) = self.peers.entry(peer_id) {
                    let info = entry.get_mut();
                    info.last_sync = Some(Instant::now());
                    info.status = PeerStatus::Synchronized { stream_set };
                    Some(ToSwarm::GenerateEvent(Event::PeerEvent(PeerEvent {
                        remote_peer_id: peer_id,
                        status: info.status,
                    })))
                } else {
                    None
                }
            }

            // The peer has failed to synchronized with us, mark the time and record that the peer connection
            // is now failed.
            FromHandler::Failed(error) => {
                if let Entry::Occupied(mut entry) = self.peers.entry(peer_id) {
                    let info = entry.get_mut();
                    warn!(%peer_id, %error, "synchronization failed with peer");
                    info.last_sync = Some(Instant::now());
                    info.status = PeerStatus::Failed;
                    Some(ToSwarm::GenerateEvent(Event::PeerEvent(PeerEvent {
                        remote_peer_id: peer_id,
                        status: info.status,
                    })))
                } else {
                    None
                }
            }
        };

        if let Some(ev) = ev {
            self.send_event(ev);
        }
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, libp2p::swarm::THandlerInEvent<Self>>> {
        if let Poll::Ready(Some(event)) = self.swarm_events_receiver.poll_recv(cx) {
            debug!(?event, "swarm event");
            return Poll::Ready(event);
        }
        // Check each peer and start synchronization as needed.
        for (peer_id, info) in &mut self.peers {
            trace!(remote_peer_id = %peer_id, ?info, "polling peer state");
            for connection_info in &info.connections {
                // Only start new synchronizations with peers we dialied
                if connection_info.dialer {
                    match info.status {
                        PeerStatus::Waiting | PeerStatus::Started { .. } | PeerStatus::Stopped => {}
                        PeerStatus::Failed => {
                            // Sync if its been a while since we last synchronized
                            let should_sync = if let Some(last_sync) = &info.last_sync {
                                last_sync.elapsed() > self.config.per_peer_sync_timeout
                            } else {
                                false
                            };
                            if should_sync {
                                info.status = PeerStatus::Waiting;
                                return Poll::Ready(ToSwarm::NotifyHandler {
                                    peer_id: *peer_id,
                                    handler: NotifyHandler::One(connection_info.id),
                                    event: FromBehaviour::StartSync {
                                        stream_set: StreamSet::Interest,
                                    },
                                });
                            }
                        }
                        PeerStatus::Synchronized { stream_set } => {
                            // Sync if we just finished an interest sync or its been a while since we
                            // last synchronized.
                            let should_sync = stream_set == StreamSet::Interest
                                || if let Some(last_sync) = &info.last_sync {
                                    last_sync.elapsed() > self.config.per_peer_sync_timeout
                                } else {
                                    false
                                };
                            if should_sync {
                                info.status = PeerStatus::Waiting;
                                let next_stream_set = match stream_set {
                                    StreamSet::Interest => StreamSet::Model,
                                    StreamSet::Model => StreamSet::Interest,
                                };
                                return Poll::Ready(ToSwarm::NotifyHandler {
                                    peer_id: *peer_id,
                                    handler: NotifyHandler::One(connection_info.id),
                                    event: FromBehaviour::StartSync {
                                        stream_set: next_stream_set,
                                    },
                                });
                            }
                        }
                    }
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
            self.interest.clone(),
            self.model.clone(),
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
            // Start synchronizing interests
            handler::State::RequestOutbound {
                stream_set: StreamSet::Interest,
            },
            self.interest.clone(),
            self.model.clone(),
        ))
    }
}

/// Events that the Behavior can emit to the rest of the application.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Event {
    /// Event indicating we have synchronized with the specific peer.
    PeerEvent(PeerEvent),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]

/// Event about a remote peer
pub struct PeerEvent {
    /// Id of remote peer
    pub remote_peer_id: PeerId,

    /// Status of the peer
    pub status: PeerStatus,
}
