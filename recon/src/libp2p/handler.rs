//! Implementation of the Recon connection handler
//!
//! A handler is created for each connected peer that speaks the Recon protocol.
//! A handler is responsible for performing Recon synchronization with a peer.
use std::{
    task::Poll,
    time::{Duration, Instant},
};

use anyhow::Result;
use libp2p::{
    core::upgrade::ReadyUpgrade,
    futures::FutureExt,
    swarm::{
        handler::{FullyNegotiatedInbound, FullyNegotiatedOutbound},
        ConnectionHandler, ConnectionHandlerEvent, KeepAlive, SubstreamProtocol,
    },
};
use tracing::debug;

use crate::libp2p::{protocol, Recon, PROTOCOL_NAME};

pub struct Handler<R> {
    state: State,
    recon: R,
    keep_alive: KeepAlive,
}

impl<R> Handler<R> {
    pub fn new(recon: R) -> Self {
        Self {
            state: State::default(),
            recon,
            keep_alive: KeepAlive::No,
        }
    }
    // Transition the state to a new state.
    //
    // See doc comment for State, each row of the transitions table
    // should map to exactly one call of this transition_state function.
    fn transition_state(&mut self, state: State) {
        debug!(
            previous_state = ?self.state,
            new_state = ?state,
            "state transition"
        );
        self.state = state;
        // Update KeepAlive
        self.keep_alive = match (&self.state, self.keep_alive) {
            (State::Idle, k @ KeepAlive::Until(_)) => k,
            (State::Idle, _) => KeepAlive::Until(Instant::now() + Duration::from_secs(2)),
            (State::RequestOutbound, _)
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
/// | State           | Event                                    | New State       |
/// | -----           | -----                                    | ---------       |
/// | Idle            | FromBehaviour::StartSync                 | RequestOutbound |
/// | Idle            | ConnectionEvent::FullyNegotiatedInbound  | Inbound         |
/// | RequestOutbound | poll                                     | WaitingOutbound |
/// | WaitingOutbound | ConnectionEvent::FullyNegotiatedOutbound | Outbound        |
/// | Outbound        | Poll::Ready (i.e. future completed)      | Idle            |
/// | Inbound         | Poll::Ready (i.e. future completed)      | Idle            |
///
/// No other transitions are possible
#[derive(Default)]
enum State {
    #[default]
    Idle,
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
    PeerSynchronized,
}

#[derive(Debug)]
pub enum Failure {
    Timeout,
    Unsupported,
    Other {
        error: Box<dyn std::error::Error + Send + 'static>,
    },
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::Timeout => f.write_str("Ping timeout"),
            Failure::Other { error } => write!(f, "Ping error: {error}"),
            Failure::Unsupported => write!(f, "Ping protocol not supported"),
        }
    }
}

impl std::error::Error for Failure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Failure::Timeout => None,
            Failure::Unsupported => None,
            Failure::Other { error } => Some(&**error),
        }
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
        match &mut self.state {
            State::Idle => {}
            State::RequestOutbound => {
                self.transition_state(State::WaitingOutbound);
                // Start outbound connection
                let protocol = SubstreamProtocol::new(ReadyUpgrade::new(PROTOCOL_NAME), ());
                return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { protocol });
            }
            State::WaitingOutbound => {}
            State::Outbound(outbound) => {
                if let Poll::Ready(result) = outbound.poll_unpin(cx) {
                    self.transition_state(State::Idle);
                    match result {
                        Ok(_) => {
                            return Poll::Ready(ConnectionHandlerEvent::Custom(
                                FromHandler::PeerSynchronized,
                            ))
                        }
                        Err(e) => {
                            // TODO handle error
                            eprintln!("{e:?}")
                        }
                    }
                }
            }
            State::Inbound(inbound) => {
                if let Poll::Ready(result) = inbound.poll_unpin(cx) {
                    self.transition_state(State::Idle);
                    match result {
                        Ok(_) => {
                            return Poll::Ready(ConnectionHandlerEvent::Custom(
                                FromHandler::PeerSynchronized,
                            ))
                        }
                        Err(e) => {
                            // TODO handle error
                            eprintln!("{e:?}")
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
                debug!("start sync from behavior");
                match self.state {
                    State::Idle => self.transition_state(State::RequestOutbound),
                    State::RequestOutbound
                    | State::WaitingOutbound
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
            ) => match self.state {
                State::Idle => self.transition_state(State::Inbound(
                    protocol::synchronize(self.recon.clone(), stream, false).boxed(),
                )),
                // Ignore inbound connection when we are not expecting it
                State::RequestOutbound
                | State::WaitingOutbound
                | State::Outbound(_)
                | State::Inbound(_) => {}
            },
            libp2p::swarm::handler::ConnectionEvent::FullyNegotiatedOutbound(
                FullyNegotiatedOutbound {
                    protocol: stream, ..
                },
            ) => match self.state {
                State::WaitingOutbound => self.transition_state(State::Outbound(
                    protocol::synchronize(self.recon.clone(), stream, true).boxed(),
                )),
                // Ignore outbound connection when we are not expecting it
                State::Idle | State::RequestOutbound | State::Outbound(_) | State::Inbound(_) => {}
            },
            libp2p::swarm::handler::ConnectionEvent::AddressChange(_) => {}
            libp2p::swarm::handler::ConnectionEvent::DialUpgradeError(_) => {}
            libp2p::swarm::handler::ConnectionEvent::ListenUpgradeError(_) => {}
        }
    }
}
