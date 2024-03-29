use std::{
    collections::VecDeque,
    fmt::Debug,
    task::{Context, Poll},
    time::Duration,
};

use asynchronous_codec::Framed;
#[allow(deprecated)]
use ceramic_metrics::core::MRecorder;
use ceramic_metrics::{bitswap::BitswapMetrics, inc};
use futures::StreamExt;
use futures::{
    prelude::*,
    stream::{BoxStream, SelectAll},
};
use libp2p::swarm::handler::FullyNegotiatedInbound;
use libp2p::swarm::{
    handler::{DialUpgradeError, FullyNegotiatedOutbound},
    ConnectionHandler, ConnectionHandlerEvent, StreamUpgradeError, SubstreamProtocol,
};
use libp2p::PeerId;
use smallvec::SmallVec;
use tokio::sync::oneshot;
use tracing::{debug, trace, warn};

use crate::{
    error::Error,
    message::BitswapMessage,
    network,
    protocol::{BitswapCodec, ProtocolConfig, ProtocolId},
};

#[derive(thiserror::Error, Debug)]
pub enum BitswapHandlerError {
    /// The message exceeds the maximum transmission size.
    #[error("max transmission size")]
    MaxTransmissionSize,
    /// Protocol negotiation timeout.
    #[error("negotiation timeout")]
    NegotiationTimeout,
    /// Protocol negotiation failed.
    #[error("no protocol could be agreed upon")]
    NegotiationProtocolError,
    /// IO error.
    #[error("io {0}")]
    Io(#[from] std::io::Error),
    #[error("bitswap {0}")]
    Bitswap(#[from] Error),
}

/// The event emitted by the Handler. This informs the behaviour of various events created
/// by the handler.
#[derive(Debug)]
pub enum HandlerEvent {
    /// A Bitswap message has been received.
    Message {
        /// The Bitswap message.
        message: BitswapMessage,
        protocol: ProtocolId,
    },
    Connected {
        protocol: ProtocolId,
    },
    ProtocolNotSuppported,
    FailedToSendMessage {
        error: BitswapHandlerError,
    },
}

type BitswapMessageResponse = oneshot::Sender<Result<(), network::SendError>>;

/// A message sent from the behaviour to the handler.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum BitswapHandlerIn {
    /// A bitswap message to send.
    Message(BitswapMessage, BitswapMessageResponse),
    // TODO: do we need a close?
    Protect,
    Unprotect,
}

type BitswapConnectionHandlerEvent =
    ConnectionHandlerEvent<ProtocolConfig, (BitswapMessage, BitswapMessageResponse), HandlerEvent>;

/// Protocol Handler that manages a single long-lived substream with a peer.
pub struct BitswapHandler {
    remote_peer_id: PeerId,
    /// Upgrade configuration for the bitswap protocol.
    listen_protocol: SubstreamProtocol<ProtocolConfig, ()>,

    /// Outbound substreams.
    outbound_substreams: SelectAll<BoxStream<'static, BitswapConnectionHandlerEvent>>,
    /// Inbound substreams.
    inbound_substreams: SelectAll<BoxStream<'static, BitswapConnectionHandlerEvent>>,

    /// Pending events to yield.
    events: SmallVec<[BitswapConnectionHandlerEvent; 4]>,

    /// Queue of values that we want to send to the remote.
    send_queue: VecDeque<(BitswapMessage, BitswapMessageResponse)>,

    protocol: Option<ProtocolId>,

    /// The amount of time we allow idle connections before disconnecting.
    idle_timeout: Duration,

    /// Collection of errors from attempting an upgrade.
    upgrade_errors: VecDeque<StreamUpgradeError<BitswapHandlerError>>,

    /// Flag determining whether to maintain the connection to the peer.
    keep_alive: bool,
}

impl Debug for BitswapHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BitswapHandler")
            .field("listen_protocol", &self.listen_protocol)
            .field(
                "outbound_substreams",
                &format!("SelectAll<{} streams>", self.outbound_substreams.len()),
            )
            .field(
                "inbound_substreams",
                &format!("SelectAll<{} streams>", self.inbound_substreams.len()),
            )
            .field("events", &self.events)
            .field("send_queue", &self.send_queue)
            .field("protocol", &self.protocol)
            .field("idle_timeout", &self.idle_timeout)
            .field("upgrade_errors", &self.upgrade_errors)
            .field("keep_alive", &self.keep_alive)
            .finish()
    }
}

impl BitswapHandler {
    /// Builds a new [`BitswapHandler`].
    pub fn new(
        remote_peer_id: PeerId,
        protocol_config: ProtocolConfig,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            remote_peer_id,
            listen_protocol: SubstreamProtocol::new(protocol_config, ()),
            inbound_substreams: Default::default(),
            outbound_substreams: Default::default(),
            send_queue: Default::default(),
            protocol: None,
            idle_timeout,
            upgrade_errors: VecDeque::new(),
            keep_alive: true,
            events: Default::default(),
        }
    }
}

impl ConnectionHandler for BitswapHandler {
    type FromBehaviour = BitswapHandlerIn;
    type ToBehaviour = HandlerEvent;
    type InboundOpenInfo = ();
    type InboundProtocol = ProtocolConfig;
    type OutboundOpenInfo = (BitswapMessage, BitswapMessageResponse);
    type OutboundProtocol = ProtocolConfig;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        self.listen_protocol.clone()
    }

    fn connection_keep_alive(&self) -> bool {
        self.keep_alive
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<BitswapConnectionHandlerEvent> {
        inc!(BitswapMetrics::HandlerPollCount);
        if !self.events.is_empty() {
            return Poll::Ready(self.events.remove(0));
        }

        inc!(BitswapMetrics::HandlerPollEventCount);

        // Handle any upgrade errors
        if let Some(error) = self.upgrade_errors.pop_front() {
            inc!(BitswapMetrics::HandlerConnUpgradeErrors);
            let error = match error {
                StreamUpgradeError::Timeout => BitswapHandlerError::NegotiationTimeout,
                StreamUpgradeError::Apply(e) => e,
                StreamUpgradeError::NegotiationFailed => {
                    BitswapHandlerError::NegotiationProtocolError
                }
                StreamUpgradeError::Io(e) => e.into(),
            };
            debug!(%error, "connection upgrade failed");

            // We no longer want to use this connection.
            // Let the swarm close it if no one else is using it.
            self.keep_alive = false;
            return Poll::Pending;
        }

        // determine if we need to create the stream
        if let Some(message) = self.send_queue.pop_front() {
            inc!(BitswapMetrics::OutboundSubstreamsEvent);
            return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: self.listen_protocol.clone().map_info(|()| message),
            });
        }

        // Poll substreams

        if let Poll::Ready(Some(event)) = self.outbound_substreams.poll_next_unpin(cx) {
            return Poll::Ready(event);
        }

        if let Poll::Ready(Some(event)) = self.inbound_substreams.poll_next_unpin(cx) {
            if let ConnectionHandlerEvent::NotifyBehaviour(HandlerEvent::Message { .. }) = event {
                // Update keep alive as we have received a message
                self.keep_alive = true;
            }

            return Poll::Ready(event);
        }

        Poll::Pending
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
                FullyNegotiatedInbound { protocol, .. },
            ) => {
                let protocol_id = protocol.codec().protocol;
                if self.protocol.is_none() {
                    self.protocol = Some(protocol_id);
                }

                trace!(?protocol_id, "new inbound substream request");
                self.inbound_substreams
                    .push(Box::pin(inbound_substream(self.remote_peer_id, protocol)));
            }
            libp2p::swarm::handler::ConnectionEvent::FullyNegotiatedOutbound(
                FullyNegotiatedOutbound { protocol, info },
            ) => {
                let protocol_id = protocol.codec().protocol;
                if self.protocol.is_none() {
                    self.protocol = Some(protocol_id);
                }

                trace!(?protocol_id, "new outbound substream");
                self.outbound_substreams.push(Box::pin(outbound_substream(
                    self.remote_peer_id,
                    protocol,
                    info,
                )));
            }
            libp2p::swarm::handler::ConnectionEvent::AddressChange(_) => {}
            libp2p::swarm::handler::ConnectionEvent::DialUpgradeError(DialUpgradeError {
                error: err,
                ..
            }) => {
                warn!(%err, "dial upgrade error");
                self.upgrade_errors.push_back(err);
            }

            _ => {}
        }
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        match event {
            BitswapHandlerIn::Message(m, response) => {
                self.send_queue.push_back((m, response));

                // sending a message, ensure keep_alive is true
                self.keep_alive = true
            }
            BitswapHandlerIn::Protect => self.keep_alive = true,
            BitswapHandlerIn::Unprotect => self.keep_alive = false,
        }
    }
}

#[tracing::instrument(skip(substream))]
fn inbound_substream(
    // Include remote_peer_id for tracing context only
    remote_peer_id: PeerId,
    mut substream: Framed<libp2p::Stream, BitswapCodec>,
) -> impl Stream<Item = BitswapConnectionHandlerEvent> {
    async_stream::stream! {
        while let Some(message) = substream.next().await {
            match message {
                Ok((message, protocol)) => {
                    // reset keep alive idle timeout
                    yield ConnectionHandlerEvent::NotifyBehaviour(HandlerEvent::Message { message, protocol });
                }
                Err(err) => match err {
                    BitswapHandlerError::MaxTransmissionSize => {
                        warn!("message exceeded the maximum transmission size");
                    }
                    _ => {
                        warn!(%err, "inbound stream error");
                        // Stop using the connection, if we are the last protocol using the
                        // connection then it will be closed.
                        break;
                    }
                }
            }
        }

        // All responses received, close the stream.
        if let Err(err) = substream.flush().await {
            warn!(%err, "failed to flush stream");
        }
        if let Err(err) = substream.close().await {
            warn!(%err, "failed to close stream");
        }
    }
}

#[tracing::instrument(skip(substream))]
fn outbound_substream(
    // Include remote_peer_id for tracing context only
    remote_peer_id: PeerId,
    mut substream: Framed<libp2p::Stream, BitswapCodec>,
    (message, response): (BitswapMessage, BitswapMessageResponse),
) -> impl Stream<Item = BitswapConnectionHandlerEvent> {
    async_stream::stream! {
        if let Err(err) = substream.feed(message).await {
            warn!(%err, "failed to write item");
            response.send(Err(network::SendError::Other(err.to_string()))).ok();
            yield ConnectionHandlerEvent::NotifyBehaviour(
                HandlerEvent::FailedToSendMessage { error: err }
            );
        } else {
            // Message sent
            response.send(Ok(())).ok();
        }

        if let Err(err) = substream.flush().await {
            warn!(%err, "failed to flush stream");
        }

        if let Err(err) = substream.close().await {
            warn!(%err, "failed to close stream");
        }
    }
}
