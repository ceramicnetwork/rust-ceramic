//! Implementation of the Recon connection handler
//!
//! A handler is created for each connected peer that speaks the Recon protocol.
//! A handler is responsible for performing Recon synchronization with a peer.
use std::{collections::VecDeque, task::Poll};

use anyhow::Result;
use ceramic_core::{EventId, Interest};
use libp2p::{
    futures::FutureExt,
    swarm::{
        handler::{FullyNegotiatedInbound, FullyNegotiatedOutbound},
        ConnectionHandler, ConnectionHandlerEvent, ConnectionId, SubstreamProtocol,
    },
};
use libp2p_identity::PeerId;
use tracing::debug;

use crate::{
    libp2p::{protocol, stream_set::StreamSet, upgrade::MultiReadyUpgrade, Recon},
    Sha256a,
};

#[derive(Debug)]
pub struct Handler<I, M> {
    remote_peer_id: PeerId,
    connection_id: ConnectionId,
    interest: I,
    model: M,
    state: State,
    behavior_events_queue: VecDeque<FromHandler>,
}

impl<I, M> Handler<I, M>
where
    I: Recon<Key = Interest, Hash = Sha256a>,
    M: Recon<Key = EventId, Hash = Sha256a>,
{
    pub fn new(
        peer_id: PeerId,
        connection_id: ConnectionId,
        state: State,
        interest: I,
        model: M,
    ) -> Self {
        Self {
            remote_peer_id: peer_id,
            connection_id,
            interest,
            model,
            state,
            behavior_events_queue: VecDeque::new(),
        }
    }
    // Transition the state to a new state.
    //
    // See doc comment for State, each row of the transitions table
    // should map to exactly one call of this transition_state function.
    //
    fn transition_state(&mut self, state: State) {
        debug!(
            %self.remote_peer_id,
            ?self.connection_id,
            previous_state = ?self.state,
            new_state = ?state,
            "state transition"
        );
        self.state = state;
    }
}

type SyncFuture = libp2p::futures::future::BoxFuture<'static, Result<StreamSet>>;

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
    RequestOutbound { stream_set: StreamSet },
    WaitingOutbound { stream_set: StreamSet },
    Outbound(SyncFuture),
    Inbound(SyncFuture),
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "Idle"),
            Self::WaitingInbound => write!(f, "WaitingInbound"),
            Self::RequestOutbound { stream_set } => f
                .debug_struct("RequestOutbound")
                .field("stream_set", stream_set)
                .finish(),
            Self::WaitingOutbound { stream_set } => f
                .debug_struct("WaitingOutbound")
                .field("stream_set", stream_set)
                .finish(),
            Self::Outbound(_) => f.debug_tuple("Outbound").field(&"_").finish(),
            Self::Inbound(_) => f.debug_tuple("Inbound").field(&"_").finish(),
        }
    }
}

#[derive(Debug)]
pub enum FromBehaviour {
    StartSync { stream_set: StreamSet },
}
#[derive(Debug)]
pub enum FromHandler {
    Started { stream_set: StreamSet },
    Succeeded { stream_set: StreamSet },
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

impl<I, M> ConnectionHandler for Handler<I, M>
where
    I: Recon<Key = Interest, Hash = Sha256a> + Clone + Send + 'static,
    M: Recon<Key = EventId, Hash = Sha256a> + Clone + Send + 'static,
{
    type FromBehaviour = FromBehaviour;
    type ToBehaviour = FromHandler;
    type InboundProtocol = MultiReadyUpgrade<StreamSet>;
    type OutboundProtocol = MultiReadyUpgrade<StreamSet>;
    type OutboundOpenInfo = ();
    type InboundOpenInfo = ();

    fn listen_protocol(
        &self,
    ) -> libp2p::swarm::SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(
            MultiReadyUpgrade::new(vec![StreamSet::Interest, StreamSet::Model]),
            (),
        )
    }

    fn connection_keep_alive(&self) -> bool {
        // Only keep the connection alive if we are not idle
        !matches!(&self.state, State::Idle)
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        if let Some(event) = self.behavior_events_queue.pop_back() {
            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(event));
        }
        match &mut self.state {
            State::Idle | State::WaitingOutbound { .. } | State::WaitingInbound => {}
            State::RequestOutbound { stream_set } => {
                let stream_set = *stream_set;
                self.transition_state(State::WaitingOutbound { stream_set });

                // Start outbound connection
                let protocol = SubstreamProtocol::new(MultiReadyUpgrade::new(vec![stream_set]), ());
                return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { protocol });
            }
            State::Outbound(stream) | State::Inbound(stream) => {
                if let Poll::Ready(result) = stream.poll_unpin(cx) {
                    self.transition_state(State::Idle);
                    match result {
                        Ok(stream_set) => {
                            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                                FromHandler::Succeeded { stream_set },
                            ));
                        }
                        Err(e) => {
                            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                                FromHandler::Failed(e),
                            ))
                        }
                    }
                }
            }
        };

        Poll::Pending
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        match event {
            FromBehaviour::StartSync { stream_set } => {
                debug!("starting sync: {:?}", stream_set);
                match self.state {
                    State::Idle => self.transition_state(State::RequestOutbound { stream_set }),
                    State::RequestOutbound { .. }
                    | State::WaitingOutbound { .. }
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
                    protocol: (stream_set, stream),
                    ..
                },
            ) => {
                match self.state {
                    State::Idle | State::WaitingInbound => {
                        self.behavior_events_queue
                            .push_front(FromHandler::Started { stream_set });
                        let stream = match stream_set {
                            StreamSet::Interest => protocol::accept_synchronize(
                                self.remote_peer_id,
                                self.connection_id,
                                stream_set,
                                self.interest.clone(),
                                stream,
                            )
                            .boxed(),
                            StreamSet::Model => protocol::accept_synchronize(
                                self.remote_peer_id,
                                self.connection_id,
                                stream_set,
                                self.model.clone(),
                                stream,
                            )
                            .boxed(),
                        };
                        self.transition_state(State::Inbound(stream));
                    }
                    // Ignore inbound connection when we are not expecting it
                    State::RequestOutbound { .. }
                    | State::WaitingOutbound { .. }
                    | State::Inbound(_)
                    | State::Outbound(_) => {}
                }
            }
            libp2p::swarm::handler::ConnectionEvent::FullyNegotiatedOutbound(
                FullyNegotiatedOutbound {
                    protocol: stream, ..
                },
            ) => {
                match &self.state {
                    State::WaitingOutbound { stream_set } => {
                        self.behavior_events_queue.push_front(FromHandler::Started {
                            stream_set: *stream_set,
                        });
                        let stream = match stream_set {
                            StreamSet::Interest => protocol::initiate_synchronize(
                                self.remote_peer_id,
                                self.connection_id,
                                *stream_set,
                                self.interest.clone(),
                                stream,
                            )
                            .boxed(),
                            StreamSet::Model => protocol::initiate_synchronize(
                                self.remote_peer_id,
                                self.connection_id,
                                *stream_set,
                                self.model.clone(),
                                stream,
                            )
                            .boxed(),
                        };
                        self.transition_state(State::Outbound(stream));
                    }
                    // Ignore outbound connection when we are not expecting it
                    State::Idle
                    | State::WaitingInbound
                    | State::RequestOutbound { .. }
                    | State::Outbound(_)
                    | State::Inbound(_) => {}
                }
            }
            libp2p::swarm::handler::ConnectionEvent::AddressChange(_) => {}
            // We failed to upgrade the inbound connection.
            libp2p::swarm::handler::ConnectionEvent::ListenUpgradeError(_) => {
                match self.state {
                    State::WaitingInbound => {
                        // We have stopped synchronization and cannot attempt again as we are unable to
                        // negotiate a protocol.
                        self.behavior_events_queue.push_front(FromHandler::Stopped);
                        self.transition_state(State::Idle)
                    }
                    State::Idle
                    | State::WaitingOutbound { .. }
                    | State::RequestOutbound { .. }
                    | State::Outbound(_)
                    | State::Inbound(_) => {}
                }
            }
            // We failed to upgrade the outbound connection.
            libp2p::swarm::handler::ConnectionEvent::DialUpgradeError(_) => {
                match self.state {
                    State::WaitingOutbound { .. } => {
                        // We have stopped synchronization and cannot attempt again as we are unable to
                        // negotiate a protocol.
                        self.behavior_events_queue.push_front(FromHandler::Stopped);
                        self.transition_state(State::Idle)
                    }
                    State::Idle
                    | State::WaitingInbound
                    | State::RequestOutbound { .. }
                    | State::Outbound(_)
                    | State::Inbound(_) => {}
                }
            }
            libp2p::swarm::handler::ConnectionEvent::LocalProtocolsChange(changes) => {
                debug!(?changes, "local protocols change")
            }
            libp2p::swarm::handler::ConnectionEvent::RemoteProtocolsChange(changes) => {
                debug!(?changes, "remote protocols change")
            }
            _ => {
                debug!("ignoring unknown connection event")
            }
        }
    }
}
