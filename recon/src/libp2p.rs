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

use anyhow::Result;
use ceramic_core::{EventId, Interest};
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
use tracing::{debug, trace, warn};

use crate::{
    libp2p::{
        handler::{FromBehaviour, FromHandler, Handler},
        stream_set::StreamSet,
    },
    recon::{InterestProvider, Key, Response, Store},
    AssociativeHash, Message, Sha256a,
};

/// Name of the Recon protocol for synchronizing interests
pub const PROTOCOL_NAME_INTEREST: &[u8] = b"/ceramic/recon/0.1.0/interest";
/// Name of the Recon protocol for synchronizing models
pub const PROTOCOL_NAME_MODEL: &[u8] = b"/ceramic/recon/0.1.0/model";

/// Defines the Recon API.
pub trait Recon: Clone + Send + Sync + 'static {
    /// The type of Key to communicate.
    type Key: Key + std::fmt::Debug + Serialize + for<'de> Deserialize<'de> + Send + 'static;
    /// The type of Hash to compute over the keys.
    type Hash: AssociativeHash
        + std::fmt::Debug
        + Serialize
        + for<'de> Deserialize<'de>
        + Send
        + 'static;

    /// Construct a message to send as the first message.
    fn initial_messages(&self) -> Result<Vec<Message<Self::Key, Self::Hash>>>;

    /// Process an incoming message and respond with a message reply.
    fn process_messages(
        &mut self,
        msg: &[Message<Self::Key, Self::Hash>],
    ) -> Result<Response<Self::Key, Self::Hash>>;

    /// Insert a new key into the key space.
    fn insert(&mut self, key: &Self::Key) -> Result<()>;

    /// Reports total number of keys
    fn len(&self) -> usize;

    /// Reports if the set is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// Implement the  Recon trait using crate::recon::Recon
//
// NOTE: We use a std::sync::Mutex because we are not doing any async
// logic within Recon itself, all async logic exists outside its scope.
// We should use a tokio::sync::Mutex if we introduce any async logic into Recon.
impl<S, K, H, I> Recon for Arc<Mutex<crate::recon::Recon<K, H, S, I>>>
where
    K: Key + std::fmt::Debug + Serialize + for<'de> Deserialize<'de> + Send + 'static,
    H: AssociativeHash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de> + Send + 'static,
    S: Store<Key = K, Hash = H> + Send + 'static,
    I: InterestProvider<Key = K> + Send + 'static,
{
    type Key = K;
    type Hash = H;

    fn initial_messages(&self) -> Result<Vec<Message<Self::Key, Self::Hash>>> {
        self.lock()
            .expect("should be able to acquire lock")
            .initial_messages()
    }

    fn process_messages(
        &mut self,
        messages: &[Message<Self::Key, Self::Hash>],
    ) -> Result<Response<Self::Key, Self::Hash>> {
        self.lock()
            .expect("should be able to acquire lock")
            .process_messages(messages)
    }

    fn insert(&mut self, key: &Self::Key) -> Result<()> {
        self.lock()
            .expect("should be able to acquire lock")
            .insert(key)
    }

    fn len(&self) -> usize {
        self.lock().expect("should be able to acquire lock").len()
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
pub struct Behaviour<IR, MR> {
    interest: IR,
    model: MR,
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

impl<IR, MR> Behaviour<IR, MR> {
    /// Create a new Behavior with the provided Recon implementation.
    pub fn new(interest: IR, model: MR, config: Config) -> Self
    where
        IR: Recon<Key = Interest, Hash = Sha256a>,
        MR: Recon<Key = EventId, Hash = Sha256a>,
    {
        Self {
            interest,
            model,
            config,
            peers: BTreeMap::new(),
            swarm_events_queue: VecDeque::new(),
        }
    }
}

impl<IR, MR> NetworkBehaviour for Behaviour<IR, MR>
where
    IR: Recon<Key = Interest, Hash = Sha256a>,
    MR: Recon<Key = EventId, Hash = Sha256a>,
{
    type ConnectionHandler = Handler<IR, MR>;

    type OutEvent = Event;

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm<Self::ConnectionHandler>) {
        match event {
            libp2p::swarm::FromSwarm::ConnectionEstablished(info) => {
                self.peers.insert(
                    info.peer_id,
                    PeerInfo {
                        status: PeerStatus::Waiting,
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
            FromHandler::Started { stream_set } => self.peers.entry(peer_id).and_modify(|info| {
                info.status = PeerStatus::Started { stream_set };
                self.swarm_events_queue
                    .push_front(Event::PeerEvent(PeerEvent {
                        remote_peer_id: peer_id,
                        status: info.status,
                    }))
            }),
            // The peer has stopped synchronization and will never be able to resume.
            FromHandler::Stopped => self.peers.entry(peer_id).and_modify(|info| {
                info.status = PeerStatus::Stopped;
                self.swarm_events_queue
                    .push_front(Event::PeerEvent(PeerEvent {
                        remote_peer_id: peer_id,
                        status: info.status,
                    }))
            }),

            // The peer has synchronized with us, mark the time and record that the peer connection
            // is now idle.
            FromHandler::Succeeded { stream_set } => self.peers.entry(peer_id).and_modify(|info| {
                info.last_sync = Some(Instant::now());
                info.status = PeerStatus::Synchronized { stream_set };
                self.swarm_events_queue
                    .push_front(Event::PeerEvent(PeerEvent {
                        remote_peer_id: peer_id,
                        status: info.status,
                    }));
            }),

            // The peer has failed to synchronized with us, mark the time and record that the peer connection
            // is now failed.
            FromHandler::Failed(error) => self.peers.entry(peer_id).and_modify(|info| {
                warn!(%peer_id, %error, "synchronization failed with peer");
                info.last_sync = Some(Instant::now());
                info.status = PeerStatus::Failed;
                self.swarm_events_queue
                    .push_front(Event::PeerEvent(PeerEvent {
                        remote_peer_id: peer_id,
                        status: info.status,
                    }))
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
            trace!(remote_peer_id = %peer_id, ?info, "polling peer state");
            // Expected the initial dialer to initiate a new synchronization.
            if info.dialer {
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
                                handler: NotifyHandler::One(info.connection_id),
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
                                handler: NotifyHandler::One(info.connection_id),
                                event: FromBehaviour::StartSync {
                                    stream_set: next_stream_set,
                                },
                            });
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
            self.config.idle_keep_alive,
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
            self.config.idle_keep_alive,
            self.interest.clone(),
            self.model.clone(),
        ))
    }
}

/// Events that the Behavior can emit to the rest of the application.
#[derive(Debug)]
pub enum Event {
    /// Event indicating we have synchronized with the specific peer.
    PeerEvent(PeerEvent),
}

#[derive(Debug)]

/// Event about a remote peer
pub struct PeerEvent {
    /// Id of remote peer
    pub remote_peer_id: PeerId,

    /// Status of the peer
    pub status: PeerStatus,
}
