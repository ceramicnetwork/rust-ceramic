use crate::libp2p::Event;
use libp2p::core::{ConnectedPoint, Endpoint};
use libp2p::swarm::handler::ConnectionEvent;
use libp2p::swarm::{
    ConnectionDenied, ConnectionHandler, ConnectionHandlerEvent, ConnectionId, FromSwarm,
    NetworkBehaviour, SubstreamProtocol, THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use libp2p::Multiaddr;
use libp2p_identity::PeerId;
use std::collections::HashMap;
use std::task::{Context, Poll};

type InEvent<N> = ToSwarm<<N as NetworkBehaviour>::ToSwarm, THandlerInEvent<N>>;
type Convert<N> = Box<
    dyn Fn(&mut N, InEvent<N>) -> Option<ToSwarm<Event, THandlerInEvent<InjectingBehavior<N>>>>
        + Send,
>;
type Api<N> = Box<dyn Fn(&mut N, &str) + Send>;

/// Wrapper a behaviour for testing
pub struct TestBehaviour<N>
where
    N: NetworkBehaviour,
{
    /// Inner behaviour to be wrapped for testing
    pub inner: N,
    /// How to convert inner behaviour events to swarm events
    pub convert: Convert<N>,
    /// How to handle api events
    pub api: Api<N>,
}

/// Allow events to be injected into a swarm
pub enum InjectedEvent {
    /// Inject a request to perform an API call
    Api(String),
    /// Inject a behaviour event
    BehaviourEvent(Event),
    /// Inject an inbound connection
    InboundConnection(PeerId, ConnectionId, ConnectedPoint),
    /// Inject an outbound connection
    OutboundConnection(PeerId, ConnectionId, ConnectedPoint),
    /// Inject a connection closed event
    ConnectionClosed(PeerId, ConnectionId, ConnectedPoint),
}

/// Wrapper to check connection behaviour polling
pub struct WrapConnectionHandler<C> {
    pub(crate) handler: C,
    pub(crate) polled: usize,
}

impl<C> ConnectionHandler for WrapConnectionHandler<C>
where
    C: ConnectionHandler,
{
    type FromBehaviour = C::FromBehaviour;
    type ToBehaviour = C::ToBehaviour;
    type InboundOpenInfo = C::InboundOpenInfo;
    type OutboundOpenInfo = C::OutboundOpenInfo;
    type InboundProtocol = C::InboundProtocol;
    type OutboundProtocol = C::OutboundProtocol;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        self.handler.listen_protocol()
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        self.polled += 1;
        self.handler.poll(cx)
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        self.handler.on_behaviour_event(event)
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
        self.handler.on_connection_event(event)
    }
}

/// Stats associated with injectors
pub struct InjectorStats {
    pub polled: usize,
    pub handler_polled: HashMap<ConnectionId, usize>,
}

/// Wrapper to allow event injection for a swarm
pub struct InjectingBehavior<N: NetworkBehaviour> {
    pub(crate) events: tokio::sync::mpsc::Receiver<InjectedEvent>,
    pub(crate) behaviour: TestBehaviour<N>,
    polled: usize,
    established_inbound:
        HashMap<ConnectionId, WrapConnectionHandler<<N as NetworkBehaviour>::ConnectionHandler>>,
    established_outbound:
        HashMap<ConnectionId, WrapConnectionHandler<<N as NetworkBehaviour>::ConnectionHandler>>,
}

impl<N: NetworkBehaviour> InjectingBehavior<N> {
    /// Create a new swarm injector
    pub fn new(
        events: tokio::sync::mpsc::Receiver<InjectedEvent>,
        behaviour: TestBehaviour<N>,
    ) -> Self {
        InjectingBehavior {
            events,
            behaviour,
            polled: 0,
            established_inbound: HashMap::new(),
            established_outbound: HashMap::new(),
        }
    }

    /// Get the number of established connections
    pub fn established(&self) -> usize {
        self.established_inbound.len() + self.established_outbound.len()
    }

    /// Get stats associated with the injector
    pub fn stats(&self) -> InjectorStats {
        let mut handler_polled = HashMap::new();
        for (id, handler) in &self.established_inbound {
            handler_polled.insert(*id, handler.polled);
        }
        for (id, handler) in &self.established_outbound {
            handler_polled.insert(*id, handler.polled);
        }
        InjectorStats {
            polled: self.polled,
            handler_polled,
        }
    }
}

impl<N> NetworkBehaviour for InjectingBehavior<N>
where
    N: NetworkBehaviour,
{
    type ConnectionHandler = <N as NetworkBehaviour>::ConnectionHandler;
    type ToSwarm = Event;

    fn handle_pending_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        self.behaviour.inner.handle_pending_inbound_connection(
            connection_id,
            local_addr,
            remote_addr,
        )
    }

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.behaviour.inner.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        )
    }

    fn handle_pending_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        addresses: &[Multiaddr],
        effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        self.behaviour.inner.handle_pending_outbound_connection(
            connection_id,
            maybe_peer,
            addresses,
            effective_role,
        )
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.behaviour.inner.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
        )
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        self.behaviour.inner.on_swarm_event(event)
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        self.behaviour
            .inner
            .on_connection_handler_event(peer_id, connection_id, event)
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        self.polled += 1;
        loop {
            if let Poll::Ready(Some(ev)) = self.events.poll_recv(cx) {
                match ev {
                    InjectedEvent::Api(desc) => {
                        (self.behaviour.api)(&mut self.behaviour.inner, &desc);
                    }
                    InjectedEvent::BehaviourEvent(ev) => {
                        return Poll::Ready(ToSwarm::GenerateEvent(ev));
                    }
                    InjectedEvent::InboundConnection(peer_id, connection_id, endpoint) => {
                        if let Ok(handler) =
                            self.behaviour.inner.handle_established_inbound_connection(
                                connection_id,
                                peer_id,
                                &Multiaddr::empty(),
                                &Multiaddr::empty(),
                            )
                        {
                            self.established_inbound.insert(
                                connection_id,
                                WrapConnectionHandler { handler, polled: 0 },
                            );
                            self.on_swarm_event(FromSwarm::ConnectionEstablished(
                                libp2p::swarm::behaviour::ConnectionEstablished {
                                    peer_id,
                                    connection_id,
                                    endpoint: &endpoint,
                                    failed_addresses: &[],
                                    other_established: self.established(),
                                },
                            ));
                        }
                    }
                    InjectedEvent::OutboundConnection(peer_id, connection_id, endpoint) => {
                        if let Ok(handler) =
                            self.behaviour.inner.handle_established_outbound_connection(
                                connection_id,
                                peer_id,
                                &Multiaddr::empty(),
                                endpoint.to_endpoint(),
                            )
                        {
                            self.established_outbound.insert(
                                connection_id,
                                WrapConnectionHandler { handler, polled: 0 },
                            );
                            self.on_swarm_event(FromSwarm::ConnectionEstablished(
                                libp2p::swarm::behaviour::ConnectionEstablished {
                                    peer_id,
                                    connection_id,
                                    endpoint: &endpoint,
                                    failed_addresses: &[],
                                    other_established: self.established(),
                                },
                            ));
                        }
                    }
                    InjectedEvent::ConnectionClosed(peer_id, connection_id, endpoint) => {
                        self.on_swarm_event(FromSwarm::ConnectionClosed(
                            libp2p::swarm::behaviour::ConnectionClosed {
                                peer_id,
                                connection_id,
                                endpoint: &endpoint,
                                remaining_established: self.established(),
                            },
                        ));
                    }
                }
            } else {
                match self.behaviour.inner.poll(cx) {
                    Poll::Ready(e) => {
                        if let Some(e) = (self.behaviour.convert)(&mut self.behaviour.inner, e) {
                            return Poll::Ready(e);
                        }
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "Event")]
pub struct TestNetworkBehaviour<N: NetworkBehaviour> {
    pub(crate) inner: InjectingBehavior<N>,
}

impl<N: NetworkBehaviour> TestNetworkBehaviour<N> {
    pub fn stats(&self) -> InjectorStats {
        self.inner.stats()
    }
}
