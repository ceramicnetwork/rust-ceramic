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

pub use crate::protocol::Recon;
pub use stream_set::StreamSet;

use ceramic_core::{EventId, PeerKey};
use futures::{future::BoxFuture, FutureExt};
use libp2p::{
    core::ConnectedPoint,
    swarm::{
        CloseConnection, ConnectionDenied, ConnectionId, NetworkBehaviour, NotifyHandler, ToSwarm,
    },
};
use libp2p_identity::PeerId;
use std::{
    cmp::min,
    collections::{btree_map::Entry, BTreeMap, HashSet},
    task::Poll,
    time::Duration,
};
use tokio::time::Instant;
use tracing::{debug, trace, warn};

use crate::{
    libp2p::handler::{FromBehaviour, FromHandler, Handler},
    metrics::{BlockedConnection, InboundSyncRejected},
    Sha256a,
};
use ceramic_metrics::Recorder;

/// Name of the Recon protocol for synchronizing peers
pub const PROTOCOL_NAME_PEER: &str = "/ceramic/recon/0.1.0/peer";
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
    /// Set of PeerIds that are permanently blocked.
    /// Connections from these peers will be denied.
    pub blocked_peers: HashSet<PeerId>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            per_peer_sync_delay: Duration::from_millis(1000),
            per_peer_sync_backoff: 2.0,
            per_peer_maximum_sync_delay: Duration::from_secs(60 * 10),
            blocked_peers: HashSet::new(),
        }
    }
}

/// Behaviour of Recon on the peer to peer network.
///
/// The Behavior tracks all peers on the network that speak the Recon protocol.
/// It is responsible for starting and stopping syncs with various peers depending on the needs of
/// the application.
pub struct Behaviour<P, M> {
    peer: P,
    model: M,
    config: Config,
    peers: BTreeMap<PeerId, PeerInfo>,
    /// Tracks backoff state for peers, persisting across disconnections.
    /// Maps peer ID to the time until which incoming syncs should be rejected.
    backoff_registry: BTreeMap<PeerId, Instant>,
    swarm_events_sender: tokio::sync::mpsc::Sender<ToSwarm<Event, FromBehaviour>>,
    swarm_events_receiver: tokio::sync::mpsc::Receiver<ToSwarm<Event, FromBehaviour>>,
    next_sync: Option<BoxFuture<'static, ()>>,
}

impl<P, M> std::fmt::Debug for Behaviour<P, M>
where
    P: std::fmt::Debug,
    M: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Behaviour")
            .field("model", &self.model)
            .field("config", &self.config)
            .field("peers", &self.peers)
            .field("backoff_registry", &self.backoff_registry)
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
        /// The number of new keys inserted during the synchronization.
        new_count: usize,
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
    /// Local peer was unable to negotiate a protocol with the remote peer.
    Stopped,
}

impl<P, M> Behaviour<P, M> {
    /// Create a new Behavior with the provided Recon implementation.
    pub fn new(peer: P, model: M, config: Config) -> Self
    where
        P: Recon<Key = PeerKey, Hash = Sha256a>,
        M: Recon<Key = EventId, Hash = Sha256a>,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        Self {
            peer,
            model,
            config,
            peers: BTreeMap::new(),
            backoff_registry: BTreeMap::new(),
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

    /// Calculate the reject_until timestamp for a peer
    fn calculate_reject_until(peer_info: &PeerInfo) -> Option<Instant> {
        peer_info
            .next_sync
            .values()
            .max()
            .copied()
            .filter(|t| *t > Instant::now())
    }
}

impl<P, M> NetworkBehaviour for Behaviour<P, M>
where
    P: Recon<Key = PeerKey, Hash = Sha256a>,
    M: Recon<Key = EventId, Hash = Sha256a>,
{
    type ConnectionHandler = Handler<P, M>;

    type ToSwarm = Event;

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm) {
        match event {
            libp2p::swarm::FromSwarm::ConnectionEstablished(info) => {
                trace!(?info, "connection established for peer");
                let connection_info = ConnectionInfo {
                    id: info.connection_id,
                    dialer: matches!(info.endpoint, ConnectedPoint::Dialer { .. }),
                };

                // Get or create peer info
                self.peers
                    .entry(info.peer_id)
                    .and_modify(|peer_info| peer_info.connections.push(connection_info))
                    .or_insert_with(|| PeerInfo {
                        status: PeerStatus::Waiting,
                        connections: vec![connection_info],
                        next_sync: BTreeMap::from_iter([
                            // Schedule all stream_sets initially
                            (StreamSet::Peer, Instant::now()),
                            // Schedule models after peers
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
        connection_id: libp2p::swarm::ConnectionId,
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
                    tracing::warn!(%peer_id, ?connection_id, "peer not found in peers map when started synchronizing? closing connection");
                    Some(ToSwarm::CloseConnection {
                        peer_id,
                        connection: CloseConnection::One(connection_id),
                    })
                }
            }

            // The peer failed to negotiate a protocol with the local peer.
            FromHandler::Stopped => {
                if let Entry::Occupied(mut entry) = self.peers.entry(peer_id) {
                    let info = entry.get_mut();
                    info.status = PeerStatus::Stopped;
                    Some(ToSwarm::GenerateEvent(Event::PeerEvent(PeerEvent {
                        remote_peer_id: peer_id,
                        status: info.status,
                    })))
                } else {
                    tracing::warn!(%peer_id, ?connection_id, "peer not found in peers map when stopped synchronizing? closing connection");
                    Some(ToSwarm::CloseConnection {
                        peer_id,
                        connection: CloseConnection::One(connection_id),
                    })
                }
            }

            // The peer has synchronized with us, mark the time and record that the peer connection
            // is now idle.
            FromHandler::Succeeded {
                stream_set,
                new_count,
            } => {
                if let Entry::Occupied(mut entry) = self.peers.entry(peer_id) {
                    debug!(%peer_id, ?stream_set, new_count, "synchronization succeeded with peer");
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
                    info.status = PeerStatus::Synchronized {
                        stream_set,
                        new_count,
                    };
                    Some(ToSwarm::GenerateEvent(Event::PeerEvent(PeerEvent {
                        remote_peer_id: peer_id,
                        status: info.status,
                    })))
                } else {
                    tracing::warn!(%peer_id, ?connection_id, "peer not found in peers map when succeeded synchronizing? closing connection");
                    Some(ToSwarm::CloseConnection {
                        peer_id,
                        connection: CloseConnection::One(connection_id),
                    })
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
                    // Collect data before releasing borrow
                    let reject_until = Self::calculate_reject_until(info);
                    let status = info.status;
                    let connection_ids: Vec<_> = info.connections.iter().map(|c| c.id).collect();

                    // Update backoff registry and notify handlers
                    if let Some(reject_until) = reject_until {
                        self.backoff_registry.insert(peer_id, reject_until);
                        // Notify handlers so they can reject incoming syncs
                        for conn_id in connection_ids {
                            self.send_event(ToSwarm::NotifyHandler {
                                peer_id,
                                handler: NotifyHandler::One(conn_id),
                                event: FromBehaviour::UpdateRejectUntil {
                                    reject_until: Some(reject_until),
                                },
                            });
                        }
                    }
                    Some(ToSwarm::GenerateEvent(Event::PeerEvent(PeerEvent {
                        remote_peer_id: peer_id,
                        status,
                    })))
                } else {
                    tracing::warn!(%peer_id, ?connection_id, "peer not found in peers map when failed synchronizing? closing connectoin");
                    Some(ToSwarm::CloseConnection {
                        peer_id,
                        connection: CloseConnection::One(connection_id),
                    })
                }
            }

            // An incoming sync was rejected due to backoff - just log it, no state change needed
            FromHandler::InboundRejected { stream_set } => {
                debug!(%peer_id, ?stream_set, "inbound sync rejected due to backoff");
                self.peer.metrics().record(&InboundSyncRejected);
                None
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
        // Clean up expired backoff entries
        let now = Instant::now();
        self.backoff_registry.retain(|_, expires| *expires > now);

        if let Poll::Ready(Some(event)) = self.swarm_events_receiver.poll_recv(cx) {
            trace!(?event, "swarm event");
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
                            trace!(?next_stream_set,?next_sync, now=?Instant::now(), "polling");
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
        // Ensure we are scheduled to be polled again when the next sync is ready
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
        // Check if peer is blocked
        if self.config.blocked_peers.contains(&peer) {
            debug!(%peer, ?connection_id, "rejecting inbound connection from blocked peer");
            self.peer.metrics().record(&BlockedConnection);
            return Err(ConnectionDenied::new(format!("peer {} is blocked", peer)));
        }
        // Check backoff registry for this peer
        let reject_inbound_until = self
            .backoff_registry
            .get(&peer)
            .copied()
            .filter(|t| *t > Instant::now());
        debug!(%peer, ?connection_id, ?reject_inbound_until, "handle_established_inbound_connection");
        Ok(Handler::new(
            peer,
            connection_id,
            handler::State::WaitingInbound,
            self.peer.clone(),
            self.model.clone(),
            reject_inbound_until,
        ))
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: libp2p::swarm::ConnectionId,
        peer: PeerId,
        _addr: &libp2p::Multiaddr,
        _role_override: libp2p::core::Endpoint,
    ) -> std::result::Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        // Check if peer is blocked
        if self.config.blocked_peers.contains(&peer) {
            debug!(%peer, ?connection_id, "rejecting outbound connection to blocked peer");
            self.peer.metrics().record(&BlockedConnection);
            return Err(ConnectionDenied::new(format!("peer {} is blocked", peer)));
        }
        // Check backoff registry for this peer
        let reject_inbound_until = self
            .backoff_registry
            .get(&peer)
            .copied()
            .filter(|t| *t > Instant::now());
        debug!(%peer, ?connection_id, ?reject_inbound_until, "handle_established_outbound_connection");
        Ok(Handler::new(
            peer,
            connection_id,
            // Start synchronizing peers
            handler::State::RequestOutbound {
                stream_set: StreamSet::Peer,
            },
            self.peer.clone(),
            self.model.clone(),
            reject_inbound_until,
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
