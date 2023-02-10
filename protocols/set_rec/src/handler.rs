use libp2p::{
    core::upgrade::ReadyUpgrade,
    futures::{future::BoxFuture, FutureExt},
    swarm::{
        handler::{ConnectionEvent, FullyNegotiatedInbound, FullyNegotiatedOutbound},
        ConnectionHandler, ConnectionHandlerEvent, KeepAlive, NegotiatedSubstream,
        SubstreamProtocol,
    },
};

use rand::{thread_rng, RngCore};
use std::{
    collections::BTreeSet, error::Error, fmt, io, num::NonZeroU32, task::Poll, time::Duration,
};
use tracing::error;
use void::Void;

use crate::protocol;
use crate::PROTOCOL_NAME;

#[derive(Debug, Clone)]
pub struct Config {
    /// Number of items in the set to generate
    set_size: NonZeroU32,
    interval: Duration,
}

impl Config {
    pub fn new() -> Self {
        Self {
            set_size: NonZeroU32::new(10).expect("10 != 0"),
            interval: Duration::from_secs(10),
        }
    }
    pub fn with_set_size(mut self, set_size: NonZeroU32) -> Self {
        self.set_size = set_size;
        self
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
}
impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}
pub struct Handler {
    config: Config,
    state: State,
    inbound: Option<SyncFuture>,
    outbound: Option<OutboundState>,
    set: BTreeSet<u8>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// We are inactive because the other peer doesn't support set_rec.
    Inactive {
        /// Whether or not we've reported the missing support yet.
        ///
        /// This is used to avoid repeated events being emitted for a specific connection.
        reported: bool,
    },
    /// We are actively reconciling sets with the other peer.
    Active,
}

/// The successful result of processing an inbound or outbound ping.
#[derive(Debug)]
pub enum Success {
    Set(BTreeSet<u8>),
}

/// An outbound ping failure.
#[derive(Debug)]
pub enum Failure {
    /// The ping timed out, i.e. no response was received within the
    /// configured ping timeout.
    Timeout,
    /// The peer does not support the ping protocol.
    Unsupported,
    /// The ping failed for reasons other than a timeout.
    Other {
        error: Box<dyn std::error::Error + Send + 'static>,
    },
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Timeout => f.write_str("Ping timeout"),
            Failure::Other { error } => write!(f, "Ping error: {error}"),
            Failure::Unsupported => write!(f, "Ping protocol not supported"),
        }
    }
}

impl Error for Failure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Failure::Timeout => None,
            Failure::Other { error } => Some(&**error),
            Failure::Unsupported => None,
        }
    }
}
impl Handler {
    /// Builds a new [`Handler`] with the given configuration.
    pub fn new(config: Config, set: BTreeSet<u8>) -> Self {
        Handler {
            config,
            state: State::Active,
            set,
            inbound: None,
            outbound: None,
        }
    }
}

impl ConnectionHandler for Handler {
    type InEvent = Void;
    type OutEvent = crate::Result;
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

    fn connection_keep_alive(&self) -> libp2p::swarm::KeepAlive {
        KeepAlive::Yes
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        libp2p::swarm::ConnectionHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::OutEvent,
            Self::Error,
        >,
    > {
        match self.state {
            State::Inactive { reported: true } => {
                return Poll::Pending; // nothing to do on this connection
            }
            State::Inactive { reported: false } => {
                self.state = State::Inactive { reported: true };
                return Poll::Ready(ConnectionHandlerEvent::Custom(Err(Failure::Unsupported)));
            }
            State::Active { .. } => {}
        }

        if let Some(fut) = self.inbound.as_mut() {
            match fut.poll_unpin(cx) {
                Poll::Pending => {}
                Poll::Ready(Err(e)) => {
                    error!("Inbound error: {:?}", e);
                    self.inbound = None;
                }
                Poll::Ready(Ok((_stream, set))) => {
                    self.inbound = None;
                    self.set = set.clone();
                    return Poll::Ready(ConnectionHandlerEvent::Custom(Ok(Success::Set(set))));
                }
            }
        }
        loop {
            match self.outbound.take() {
                Some(OutboundState::SendSet(mut sync)) => match sync.poll_unpin(cx) {
                    Poll::Pending => {
                        self.outbound = Some(OutboundState::SendSet(sync));
                        break;
                    }
                    Poll::Ready(Ok((_stream, set))) => {
                        self.set = set.clone();
                        self.outbound = Some(OutboundState::Synced);
                        return Poll::Ready(ConnectionHandlerEvent::Custom(Ok(Success::Set(set))));
                    }
                    Poll::Ready(Err(e)) => {
                        error!("send_set error: {}", e);
                        return Poll::Ready(ConnectionHandlerEvent::Close(Failure::Other {
                            error: Box::new(e),
                        }));
                    }
                },
                Some(OutboundState::OpenStream) => {
                    self.outbound = Some(OutboundState::OpenStream);
                    break;
                }
                Some(OutboundState::Synced) => {
                    self.outbound = Some(OutboundState::Synced);
                    break;
                }
                None => {
                    // Initiate outbound connection if we have not connected yet
                    self.outbound = Some(OutboundState::OpenStream);
                    let protocol = SubstreamProtocol::new(ReadyUpgrade::new(PROTOCOL_NAME), ());
                    return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                        protocol,
                    });
                }
            }
        }
        Poll::Pending
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol: stream,
                ..
            }) => {
                self.inbound = Some(protocol::recv_set(stream, self.set.clone()).boxed());
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol: stream,
                ..
            }) => {
                self.outbound = Some(OutboundState::SendSet(
                    protocol::send_set(stream, self.set.clone()).boxed(),
                ));
            }
            ConnectionEvent::DialUpgradeError(_dial_upgrade_error) => {
                self.state = State::Inactive { reported: true }
            }
            ConnectionEvent::AddressChange(_) | ConnectionEvent::ListenUpgradeError(_) => {}
        }
    }
}

type SyncFuture = BoxFuture<'static, Result<(NegotiatedSubstream, BTreeSet<u8>), io::Error>>;

/// The current state w.r.t. outbound pings.
enum OutboundState {
    /// A new substream is being negotiated for the ping protocol.
    OpenStream,
    SendSet(SyncFuture),
    Synced,
}
