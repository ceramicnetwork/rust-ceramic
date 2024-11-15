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
use futures::{future::BoxFuture, FutureExt};
use libp2p::{
    core::ConnectedPoint,
    swarm::{ConnectionId, NetworkBehaviour, NotifyHandler, ToSwarm},
};
use libp2p_identity::PeerId;
use std::{
    cmp::min,
    collections::{btree_map::Entry, BTreeMap},
    task::Poll,
    time::Duration,
};
use tokio::time::Instant;
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
    pub per_peer_sync_delay: Duration,
    /// Backoff sequential failures as multiples of delay.
    pub per_peer_sync_backoff: f64,
    /// Maximum delay between synchronization attempts.
    /// Defaults to 10 minutes
    pub per_peer_maximum_sync_delay: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            per_peer_sync_delay: Duration::from_millis(1000),
            per_peer_sync_backoff: 2.0,
            per_peer_maximum_sync_delay: Duration::from_secs(60 * 10),
        }
    }
}

/// Behaviour of Recon on the peer to peer network.
///
/// The Behavior tracks all peers on the network that speak the Recon protocol.
/// It is responsible for starting and stopping syncs with various peers depending on the needs of
/// the application.
pub struct Behaviour<I, M> {
    interest: I,
    model: M,
    config: Config,
    peers: BTreeMap<PeerId, PeerInfo>,
    swarm_events_sender: tokio::sync::mpsc::Sender<ToSwarm<Event, FromBehaviour>>,
    swarm_events_receiver: tokio::sync::mpsc::Receiver<ToSwarm<Event, FromBehaviour>>,
    next_sync: Option<BoxFuture<'static, ()>>,
}

impl<I: std::fmt::Debug, M: std::fmt::Debug> std::fmt::Debug for Behaviour<I, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Behaviour")
            .field("interest", &self.interest)
            .field("model", &self.model)
            .field("config", &self.config)
            .field("peers", &self.peers)
            .field("swarm_events_sender", &self.swarm_events_sender)
            .field("swarm_events_receiver", &self.swarm_events_receiver)
            .field("next_sync", &"_")
            .finish()
    }
}

/// Information about a remote peer and its sync status.
#[derive(Clone, Debug)]
struct PeerInfo {
    status: PeerStatus,
    connections: Vec<ConnectionInfo>,
    next_sync: BTreeMap<StreamSet, Instant>,
    sync_delay: BTreeMap<StreamSet, Duration>,
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
    Failed {
        /// The stream_set that has failed synchronizing.
        stream_set: StreamSet,
    },
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
            next_sync: None,
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
                        next_sync: BTreeMap::from_iter([
                            // Schedule all stream_sets initially
                            (StreamSet::Interest, Instant::now()),
                            // Schedule models after interests
                            (StreamSet::Model, Instant::now() + Duration::from_millis(1)),
                        ]),
                        sync_delay: Default::default(),
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
                    tracing::warn!(%peer_id, "peer not found in peers map when started syncronizing?");
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
                    tracing::warn!(%peer_id, "peer not found in peers map when stopped syncronizing?");
                    None
                }
            }

            // The peer has synchronized with us, mark the time and record that the peer connection
            // is now idle.
            FromHandler::Succeeded { stream_set } => {
                if let Entry::Occupied(mut entry) = self.peers.entry(peer_id) {
                    debug!(%peer_id, ?stream_set, "synchronization s
                        ucceeded with peer");
                    let info = entry.get_mut();
                    let sync_delay = *info
                        .sync_delay
                        .get(&stream_set)
                        .unwrap_or(&self.config.per_peer_sync_delay);
                    info.next_sync
                        .insert(stream_set, Instant::now() + sync_delay);
                    // On success reset delay
                    info.sync_delay
                        .insert(stream_set, self.config.per_peer_sync_delay);
                    info.status = PeerStatus::Synchronized { stream_set };
                    Some(ToSwarm::GenerateEvent(Event::PeerEvent(PeerEvent {
                        remote_peer_id: peer_id,
                        status: info.status,
                    })))
                } else {
                    tracing::warn!(%peer_id, "peer not found in peers map when succeeded syncronizing?");
                    None
                }
            }

            // The peer has failed to synchronized with us, mark the time and record that the peer connection
            // is now failed.
            FromHandler::Failed { stream_set, error } => {
                if let Entry::Occupied(mut entry) = self.peers.entry(peer_id) {
                    let info = entry.get_mut();
                    let sync_delay = *info
                        .sync_delay
                        .get(&stream_set)
                        .unwrap_or(&self.config.per_peer_sync_delay);
                    warn!(%peer_id, %error, ?sync_delay, ?stream_set, "synchronization failed with peer");
                    info.next_sync
                        .insert(stream_set, Instant::now() + sync_delay);
                    // On failure increase sync delay
                    info.sync_delay.insert(
                        stream_set,
                        min(
                            sync_delay.mul_f64(self.config.per_peer_sync_backoff),
                            self.config.per_peer_maximum_sync_delay,
                        ),
                    );
                    info.status = PeerStatus::Failed { stream_set };
                    Some(ToSwarm::GenerateEvent(Event::PeerEvent(PeerEvent {
                        remote_peer_id: peer_id,
                        status: info.status,
                    })))
                } else {
                    tracing::warn!(%peer_id, "peer not found in peers map when failed syncronizing?");
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
                        PeerStatus::Failed { .. } | PeerStatus::Synchronized { .. } => {
                            // Find earliest scheduled stream set
                            let (next_stream_set, next_sync) =
                                info.next_sync.iter().min_by_key(|(_, t)| *t).expect(
                                    "next_sync should always be initialized with stream sets",
                                );
                            debug!(?next_stream_set,?next_sync, now=?Instant::now(), "polling");
                            // Sync if enough time has passed since we synced the stream set.
                            if *next_sync < Instant::now() {
                                self.next_sync = None;
                                info.status = PeerStatus::Waiting;
                                return Poll::Ready(ToSwarm::NotifyHandler {
                                    peer_id: *peer_id,
                                    handler: NotifyHandler::One(connection_info.id),
                                    event: FromBehaviour::StartSync {
                                        stream_set: *next_stream_set,
                                    },
                                });
                            } else {
                                self.next_sync =
                                    Some(Box::pin(tokio::time::sleep_until(*next_sync)));
                            }
                        }
                    }
                }
            }
        }
        if let Some(ref mut next_sync) = &mut self.next_sync {
            let _ = next_sync.poll_unpin(cx);
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
