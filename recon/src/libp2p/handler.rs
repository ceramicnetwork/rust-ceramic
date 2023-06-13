//! Implementation of the Recon connection handler
//!
//! A handler is created for each connected peer that speaks the Recon protocol.
//! A handler is responsible for performing Recon synchronization with a peer.
use std::{
    collections::VecDeque,
    task::Poll,
    time::{Duration, Instant},
};

use anyhow::Result;
use libp2p::{
    core::upgrade::ReadyUpgrade,
    futures::FutureExt,
    swarm::{
        handler::{FullyNegotiatedInbound, FullyNegotiatedOutbound},
        ConnectionHandler, ConnectionHandlerEvent, ConnectionId, KeepAlive, SubstreamProtocol,
    },
};
use libp2p_identity::PeerId;
use tracing::debug;

use crate::libp2p::{protocol, Recon, PROTOCOL_NAME};

pub struct Handler<R> {
    remote_peer_id: PeerId,
    connection_id: ConnectionId,
    recon: R,
    state: State,
    keep_alive_duration: Duration,
    keep_alive: KeepAlive,
    behavior_events_queue: VecDeque<FromHandler>,
}

impl<R> Handler<R> {
    pub fn new(
        peer_id: PeerId,
        connection_id: ConnectionId,
        state: State,
        keep_alive_duration: Duration,
        recon: R,
    ) -> Self {
        Self {
            remote_peer_id: peer_id,
            connection_id,
            recon,
            state,
            keep_alive_duration,
            keep_alive: KeepAlive::Yes,
            behavior_events_queue: VecDeque::new(),
        }
    }
    // Transition the state to a new state.
    //
    // See doc comment for State, each row of the transitions table
    // should map to exactly one call of this transition_state function.
    fn transition_state(&mut self, state: State) {
        debug!(
            %self.remote_peer_id,
            ?self.connection_id,
            previous_state = ?self.state,
            new_state = ?state,
            "state transition"
        );
        self.state = state;
        // Update KeepAlive
        self.keep_alive = match (&self.state, self.keep_alive) {
            (State::Idle, k @ KeepAlive::Until(_)) => k,
            (State::Idle, _) => KeepAlive::Until(Instant::now() + self.keep_alive_duration),
            (State::RequestOutbound, _)
            | (State::WaitingInbound, _)
            | (State::WaitingOutbound, _)
            | (State::Outbound(_), _)
            | (State::Inbound(_), _) => KeepAlive::Yes,
        };
    }
}

type SyncFuture = libp2p::futures::future::BoxFuture<'static, Result<()>>;

/// Current state of the handler.
///
/// State Transitions:
///
/// | State            | Event                                    | New State       |
/// | -----            | -----                                    | ---------       |
/// | Idle             | FromBehaviour::StartSync                 | RequestOutbound |
/// | Idle             | ConnectionEvent::FullyNegotiatedInbound  | Inbound         |
/// | WaitingInbound*  | ConnectionEvent::FullyNegotiatedInbound  | Inbound         |
/// | RequestOutbound* | poll                                     | WaitingOutbound |
/// | WaitingOutbound  | ConnectionEvent::FullyNegotiatedOutbound | Outbound        |
/// | WaitingOutbound  | ConnectionEvent::DialUpgradeError        | Idle            |
/// | WaitingInbound   | ConnectionEvent::ListenUpgradeError      | Idle            |
/// | Outbound         | Poll::Ready (i.e. future completed)      | Idle            |
/// | Inbound          | Poll::Ready (i.e. future completed)      | Idle            |
///
/// No other transitions are possible.
///
/// * Starting states
pub enum State {
    Idle,
    WaitingInbound,
    RequestOutbound,
    WaitingOutbound,
    Outbound(SyncFuture),
    Inbound(SyncFuture),
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "Idle"),
            Self::RequestOutbound => write!(f, "RequestOutbound"),
            Self::WaitingInbound => write!(f, "WaitingInbound"),
            Self::WaitingOutbound => write!(f, "WaitingOutbound"),
            Self::Outbound(_) => f.debug_tuple("Outbound").field(&"_").finish(),
            Self::Inbound(_) => f.debug_tuple("Inbound").field(&"_").finish(),
        }
    }
}

#[derive(Debug)]
pub enum FromBehaviour {
    StartSync,
}
#[derive(Debug)]
pub enum FromHandler {
    Started,
    Succeeded,
    Stopped,
    Failed(anyhow::Error),
}

#[derive(Debug)]
pub struct Failure {
    error: Box<dyn std::error::Error + Send + 'static>,
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error: {}", self.error)
    }
}

impl std::error::Error for Failure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.error)
    }
}

impl<R: Recon + Clone + Send + 'static> ConnectionHandler for Handler<R> {
    type InEvent = FromBehaviour;
    type OutEvent = FromHandler;
    type Error = Failure;
    type InboundProtocol = ReadyUpgrade<&'static [u8]>;
    type OutboundProtocol = ReadyUpgrade<&'static [u8]>;
    type OutboundOpenInfo = ();
    type InboundOpenInfo = ();

    fn listen_protocol(
        &self,
    ) -> libp2p::swarm::SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(ReadyUpgrade::new(PROTOCOL_NAME), ())
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        debug!(?self.keep_alive, "connection_keep_alive");
        self.keep_alive
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::OutEvent,
            Self::Error,
        >,
    > {
        if let Some(event) = self.behavior_events_queue.pop_back() {
            return Poll::Ready(ConnectionHandlerEvent::Custom(event));
        }
        match &mut self.state {
            State::Idle | State::WaitingOutbound | State::WaitingInbound => {}
            State::RequestOutbound => {
                self.transition_state(State::WaitingOutbound);
                // Start outbound connection
                let protocol = SubstreamProtocol::new(ReadyUpgrade::new(PROTOCOL_NAME), ());
                return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { protocol });
            }
            State::Outbound(stream) | State::Inbound(stream) => {
                if let Poll::Ready(result) = stream.poll_unpin(cx) {
                    self.transition_state(State::Idle);
                    match result {
                        Ok(_) => {
                            return Poll::Ready(ConnectionHandlerEvent::Custom(
                                FromHandler::Succeeded,
                            ))
                        }
                        Err(e) => {
                            return Poll::Ready(ConnectionHandlerEvent::Custom(
                                FromHandler::Failed(e),
                            ))
                        }
                    }
                }
            }
        };

        Poll::Pending
    }

    fn on_behaviour_event(&mut self, event: Self::InEvent) {
        match event {
            FromBehaviour::StartSync => {
                debug!(%self.remote_peer_id,  ?self.connection_id, "start sync from behavior");
                match self.state {
                    State::Idle => self.transition_state(State::RequestOutbound),
                    State::RequestOutbound
                    | State::WaitingOutbound
                    | State::WaitingInbound
                    | State::Outbound(_)
                    | State::Inbound(_) => {}
                }
            }
        }
    }

    fn on_connection_event(
        &mut self,
        event: libp2p::swarm::handler::ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        match event {
            libp2p::swarm::handler::ConnectionEvent::FullyNegotiatedInbound(
                FullyNegotiatedInbound {
                    protocol: stream, ..
                },
            ) => {
                debug!(%self.remote_peer_id, ?self.connection_id, "on_connection_event::FullyNegotiatedInbound");
                match self.state {
                    State::Idle | State::WaitingInbound => {
                        self.behavior_events_queue.push_front(FromHandler::Started);
                        self.transition_state(State::Inbound(
                            protocol::synchronize(
                                self.remote_peer_id,
                                self.connection_id,
                                self.recon.clone(),
                                stream,
                                false,
                            )
                            .boxed(),
                        ));
                    }
                    // Ignore inbound connection when we are not expecting it
                    State::RequestOutbound
                    | State::WaitingOutbound
                    | State::Inbound(_)
                    | State::Outbound(_) => {}
                }
            }
            libp2p::swarm::handler::ConnectionEvent::FullyNegotiatedOutbound(
                FullyNegotiatedOutbound {
                    protocol: stream, ..
                },
            ) => {
                debug!(%self.remote_peer_id, ?self.connection_id, "on_connection_event::FullyNegotiatedOutbound");
                match self.state {
                    State::WaitingOutbound => {
                        self.behavior_events_queue.push_front(FromHandler::Started);
                        self.transition_state(State::Outbound(
                            protocol::synchronize(
                                self.remote_peer_id,
                                self.connection_id,
                                self.recon.clone(),
                                stream,
                                true,
                            )
                            .boxed(),
                        ));
                    }
                    // Ignore outbound connection when we are not expecting it
                    State::Idle
                    | State::WaitingInbound
                    | State::RequestOutbound
                    | State::Outbound(_)
                    | State::Inbound(_) => {}
                }
            }
            libp2p::swarm::handler::ConnectionEvent::AddressChange(_) => {}
            // We failed to upgrade the inbound connection.
            libp2p::swarm::handler::ConnectionEvent::ListenUpgradeError(_) => {
                debug!(%self.remote_peer_id, ?self.connection_id, "on_connection_event::ListenUpgradeError");
                match self.state {
                    State::WaitingInbound => {
                        // We have stopped synchronization and cannot attempt again as we are unable to
                        // negotiate a protocol.
                        self.behavior_events_queue.push_front(FromHandler::Stopped);
                        self.transition_state(State::Idle)
                    }
                    State::Idle
                    | State::WaitingOutbound
                    | State::RequestOutbound
                    | State::Outbound(_)
                    | State::Inbound(_) => {}
                }
            }
            // We failed to upgrade the outbound connection.
            libp2p::swarm::handler::ConnectionEvent::DialUpgradeError(_) => {
                debug!(%self.remote_peer_id, ?self.connection_id, "on_connection_event::DialUpgradeError");
                match self.state {
                    State::WaitingOutbound => {
                        // We have stopped synchronization and cannot attempt again as we are unable to
                        // negotiate a protocol.
                        self.behavior_events_queue.push_front(FromHandler::Stopped);
                        self.transition_state(State::Idle)
                    }
                    State::Idle
                    | State::WaitingInbound
                    | State::RequestOutbound
                    | State::Outbound(_)
                    | State::Inbound(_) => {}
                }
            }
        }
    }
}
