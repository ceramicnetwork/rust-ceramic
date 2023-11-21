use std::{
    fmt::{self, Debug, Formatter},
    future,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use ahash::AHashMap;
use anyhow::{anyhow, Result};
use backoff::{backoff::Backoff, ExponentialBackoff, ExponentialBackoffBuilder};
#[allow(deprecated)]
use ceramic_metrics::core::MRecorder;
use ceramic_metrics::{inc, p2p::P2PMetrics, Recorder};
use futures_util::{future::BoxFuture, FutureExt, Stream, StreamExt};
use libp2p::swarm::{
    dial_opts::{DialOpts, PeerCondition},
    ToSwarm,
};
use libp2p::{
    identify::Info as IdentifyInfo,
    multiaddr::Protocol,
    swarm::{dummy, ConnectionId, DialError, NetworkBehaviour, PollParameters},
    Multiaddr, PeerId,
};
use lru::LruCache;
use tokio::time;
use tracing::{info, warn};

use crate::metrics::{self, Metrics};

pub struct PeerManager {
    info: AHashMap<PeerId, Info>,
    bad_peers: LruCache<PeerId, ()>,
    bootstrap_peer_manager: BootstrapPeerManager,
    supported_protocols: Vec<String>,
}

#[derive(Default, Debug, Clone)]
pub struct Info {
    pub last_rtt: Option<Duration>,
    pub last_info: Option<IdentifyInfo>,
}

impl Info {
    pub fn latency(&self) -> Option<Duration> {
        // only approximation, this is wrong but the best we have for now
        self.last_rtt.map(|rtt| rtt / 2)
    }
}

const DEFAULT_BAD_PEER_CAP: Option<NonZeroUsize> = NonZeroUsize::new(10 * 4096);
const BOOTSTRAP_MIN_DIAL_SECS: Duration = Duration::from_secs(1); // 1 second min between redials
const BOOTSTRAP_MAX_DIAL_SECS: Duration = Duration::from_secs(300); // 5 minutes max between redials
const BOOTSTRAP_DIAL_BACKOFF: f64 = 1.4;
const BOOTSTRAP_DIAL_JITTER: f64 = 0.1;

#[derive(Debug)]
pub enum PeerManagerEvent {}

impl PeerManager {
    pub fn new(bootstrap_peers: &[Multiaddr], metrics: Metrics) -> Result<Self> {
        Ok(Self {
            info: Default::default(),
            bad_peers: LruCache::new(DEFAULT_BAD_PEER_CAP.unwrap()),
            bootstrap_peer_manager: BootstrapPeerManager::new(bootstrap_peers, metrics)?,
            supported_protocols: Default::default(),
        })
    }

    pub fn is_bad_peer(&self, peer_id: &PeerId) -> bool {
        self.bad_peers.contains(peer_id)
    }

    pub fn inject_identify_info(&mut self, peer_id: PeerId, new_info: IdentifyInfo) {
        self.info.entry(peer_id).or_default().last_info = Some(new_info);
    }

    pub fn inject_ping(&mut self, peer_id: PeerId, rtt: Duration) {
        self.info.entry(peer_id).or_default().last_rtt = Some(rtt);
    }

    pub fn info_for_peer(&self, peer_id: &PeerId) -> Option<&Info> {
        self.info.get(peer_id)
    }

    pub fn supported_protocols(&self) -> Vec<String> {
        self.supported_protocols.clone()
    }
}

impl NetworkBehaviour for PeerManager {
    type ConnectionHandler = dummy::ConnectionHandler;
    type ToSwarm = PeerManagerEvent;

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm<Self::ConnectionHandler>) {
        match event {
            libp2p::swarm::FromSwarm::ConnectionEstablished(event) => {
                // First connection
                if event.other_established == 0 {
                    let p = self.bad_peers.pop(&event.peer_id);
                    if p.is_some() {
                        inc!(P2PMetrics::BadPeerRemoved);
                    }
                    self.bootstrap_peer_manager
                        .handle_connection_established(&event.peer_id)
                }

                if let Some(info) = self.info.get_mut(&event.peer_id) {
                    if let Some(ref mut info) = info.last_info {
                        info.listen_addrs
                            .retain(|addr| !event.failed_addresses.contains(addr))
                    }
                }
            }
            libp2p::swarm::FromSwarm::ConnectionClosed(event) => {
                // Last connection
                if event.remaining_established == 0 {
                    self.bootstrap_peer_manager
                        .handle_connection_closed(&event.peer_id)
                }
            }
            libp2p::swarm::FromSwarm::DialFailure(event) => {
                if let Some(peer_id) = event.peer_id {
                    match event.error {
                        // TODO check that the denied cause is because of a connection limit.
                        DialError::Denied { cause: _ } | DialError::DialPeerConditionFalse(_) => {}
                        _ => {
                            if self.bad_peers.put(peer_id, ()).is_none() {
                                inc!(P2PMetrics::BadPeer);
                            }
                            self.info.remove(&peer_id);
                        }
                    }
                    self.bootstrap_peer_manager.handle_dial_failure(&peer_id)
                }
            }
            // Not interested in any other events
            _ => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        _peer_id: PeerId,
        _connection_id: ConnectionId,
        _event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
        params: &mut impl PollParameters,
    ) -> Poll<libp2p::swarm::ToSwarm<Self::ToSwarm, libp2p::swarm::THandlerInEvent<Self>>> {
        // TODO(nathanielc):
        // We can only get the supported protocols of the local node by examining the
        // `PollParameters`, which mean you can only get the supported protocols by examining the
        // `PollParameters` in this method (`poll`) of a network behaviour.
        // I injected this responsibility in the `peer_manager`, because it's the only "simple"
        // network behaviour we have implemented.
        // There is an issue up to remove `PollParameters`, and a discussion into how to instead
        // get the `supported_protocols` of the node:
        // https://github.com/libp2p/rust-libp2p/issues/3124
        // When that is resolved, we can hopefully remove this responsibility from the `peer_manager`,
        // where it, frankly, doesn't belong.
        //
        // With libp2p 0.52.4 supported_protocols has been deprecated. We should now be able to
        // refactor the peer_manager to not need to handle supported_protocols.
        #[allow(deprecated)]
        if self.supported_protocols.is_empty() {
            self.supported_protocols = params
                .supported_protocols()
                .map(|p| String::from_utf8_lossy(&p).to_string())
                .collect();
        }

        // Check if a bootstrap peer needs to be dialed
        match self.bootstrap_peer_manager.poll_next_unpin(cx) {
            Poll::Ready(Some((peer_id, multiaddr))) => Poll::Ready(ToSwarm::Dial {
                opts: DialOpts::peer_id(peer_id)
                    .addresses(vec![multiaddr])
                    .condition(PeerCondition::Disconnected)
                    .build(),
            }),
            _ => Poll::Pending,
        }
    }

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _peer: PeerId,
        _local_addr: &libp2p::Multiaddr,
        _remote_addr: &libp2p::Multiaddr,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        Ok(dummy::ConnectionHandler)
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _peer: PeerId,
        _addr: &libp2p::Multiaddr,
        _role_override: libp2p::core::Endpoint,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        Ok(dummy::ConnectionHandler)
    }
}

pub struct BootstrapPeerManager {
    bootstrap_peers: AHashMap<PeerId, BootstrapPeer>,
    metrics: Metrics,
}

impl Debug for BootstrapPeerManager {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        f.debug_struct("BootstrapPeerManager")
            .field("bootstrap_peers", &self.bootstrap_peers)
            .finish()
    }
}

pub struct BootstrapPeer {
    peer_id: PeerId,
    multiaddr: Multiaddr,
    dial_backoff: ExponentialBackoff,
    dial_future: Option<BoxFuture<'static, ()>>,
}

impl Debug for BootstrapPeer {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        f.debug_struct("BootstrapPeer")
            .field("multiaddr", &self.multiaddr)
            .field("dial_backoff", &self.dial_backoff)
            .field("dial_future", &self.dial_future.is_some())
            .finish()
    }
}

impl BootstrapPeerManager {
    fn new(bootstrap_peers: &[Multiaddr], metrics: Metrics) -> Result<Self> {
        let bootstrap_peers = bootstrap_peers
            .iter()
            .map(|multiaddr| {
                if let Some(peer) = multiaddr.iter().find_map(|proto| match proto {
                    Protocol::P2p(peer_id) => {
                        Some((peer_id, BootstrapPeer::new(peer_id, multiaddr.to_owned())))
                    }
                    _ => None,
                }) {
                    Ok(peer)
                } else {
                    Err(anyhow!("Could not parse bootstrap addr {}", multiaddr))
                }
            })
            .collect::<Result<AHashMap<PeerId, BootstrapPeer>, anyhow::Error>>()?;
        Ok(Self {
            bootstrap_peers,
            metrics,
        })
    }

    fn handle_connection_established(&mut self, peer_id: &PeerId) {
        if let Some(peer) = self.bootstrap_peers.get_mut(peer_id) {
            info!(
                multiaddr = %peer.multiaddr,
                "connection established, stop dialing bootstrap peer",
            );
            peer.stop_redial();
            self.metrics.record(&metrics::PeeringEvent::Connected);
        }
    }

    fn handle_connection_closed(&mut self, peer_id: &PeerId) {
        if let Some(peer) = self.bootstrap_peers.get_mut(peer_id) {
            warn!(
                multiaddr = %peer.multiaddr,
                "Connection closed, redial bootstrap peer",
            );
            peer.start_redial();
            self.metrics.record(&metrics::PeeringEvent::Disconnected);
        }
    }

    fn handle_dial_failure(&mut self, peer_id: &PeerId) {
        if let Some(peer) = self.bootstrap_peers.get_mut(peer_id) {
            warn!(
                multiaddr = %peer.multiaddr,
                "Dail failed, redial bootstrap peer"
            );
            peer.backoff_redial();
            self.metrics.record(&metrics::PeeringEvent::DialFailure);
        }
    }
}

impl Stream for BootstrapPeerManager {
    type Item = (PeerId, Multiaddr);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        for (_, peer) in self.bootstrap_peers.iter_mut() {
            if let Some(mut dial_future) = peer.dial_future.take() {
                match dial_future.as_mut().poll_unpin(cx) {
                    Poll::Ready(()) => {
                        return Poll::Ready(Some((peer.peer_id, peer.multiaddr.clone())));
                    }
                    Poll::Pending => {
                        // Put the future back
                        peer.dial_future.replace(dial_future);
                    }
                }
            }
        }
        Poll::Pending
    }
}

impl BootstrapPeer {
    fn new(peer_id: PeerId, multiaddr: Multiaddr) -> Self {
        let dial_backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(BOOTSTRAP_MIN_DIAL_SECS)
            .with_multiplier(BOOTSTRAP_DIAL_BACKOFF)
            .with_randomization_factor(BOOTSTRAP_DIAL_JITTER)
            .with_max_interval(BOOTSTRAP_MAX_DIAL_SECS)
            .with_max_elapsed_time(None)
            .build();
        // Expire initial future so that we dial peers immediately
        let dial_future = Some(future::ready(()).boxed());
        Self {
            peer_id,
            multiaddr,
            dial_backoff,
            dial_future,
        }
    }

    fn start_redial(&mut self) {
        self.dial_backoff.reset();
        let next_backoff = self.dial_backoff.next_backoff();
        self.update_dial_future(next_backoff);
    }

    fn stop_redial(&mut self) {
        self.dial_backoff.reset();
        self.update_dial_future(None);
    }

    fn backoff_redial(&mut self) {
        let next_backoff = self.dial_backoff.next_backoff();
        self.update_dial_future(next_backoff);
    }

    fn update_dial_future(&mut self, duration: Option<Duration>) {
        // This will drop the existing sleep future, if present, thereby canceling it.
        self.dial_future = None;
        if let Some(duration) = duration {
            self.dial_future = Some(Box::pin(time::sleep(duration)));
        }
    }
}
