use std::{
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use ahash::AHashMap;
use backoff::{backoff::Backoff, ExponentialBackoff};
#[allow(deprecated)]
use ceramic_metrics::core::MRecorder;
use ceramic_metrics::{dec, inc, p2p::P2PMetrics};
use futures_util::{Future, Stream, StreamExt};
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::ToSwarm;
use libp2p::{
    identify::Info as IdentifyInfo,
    multiaddr::Protocol,
    swarm::{dummy, ConnectionId, DialError, NetworkBehaviour, PollParameters},
    Multiaddr, PeerId,
};
use lru::LruCache;
use tokio::time;
use tracing::{info, warn};

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

impl Default for PeerManager {
    fn default() -> Self {
        PeerManager {
            info: Default::default(),
            bad_peers: LruCache::new(DEFAULT_BAD_PEER_CAP.unwrap()),
            bootstrap_peer_manager: Default::default(),
            supported_protocols: Default::default(),
        }
    }
}

#[derive(Debug)]
pub enum PeerManagerEvent {}

impl PeerManager {
    pub fn new(bootstrap_peers: &[Multiaddr]) -> Self {
        Self {
            bootstrap_peer_manager: BootstrapPeerManager::new(bootstrap_peers),
            ..Default::default()
        }
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
            Poll::Ready(Some(multiaddr)) => Poll::Ready(ToSwarm::Dial {
                opts: DialOpts::unknown_peer_id().address(multiaddr).build(),
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

#[derive(Debug)]
pub struct BootstrapPeerManager {
    bootstrap_peers: AHashMap<PeerId, BootstrapPeer>,
}

#[derive(Debug)]
pub struct BootstrapPeer {
    multiaddr: Multiaddr,
    dial_backoff: ExponentialBackoff,
    dial_future: Option<Pin<Box<time::Sleep>>>,
}

impl Default for BootstrapPeerManager {
    fn default() -> Self {
        BootstrapPeerManager {
            bootstrap_peers: Default::default(),
        }
    }
}

impl BootstrapPeerManager {
    fn new(bootstrap_peers: &[Multiaddr]) -> Self {
        Self {
            bootstrap_peers: bootstrap_peers
                .iter()
                .filter_map(|multiaddr| {
                    let mut addr = multiaddr.to_owned();
                    if let Some(Protocol::P2p(peer_id)) = addr.pop() {
                        Some((peer_id, BootstrapPeer::new(multiaddr.to_owned())))
                    } else {
                        warn!("Could not parse bootstrap addr {}", multiaddr);
                        None
                    }
                })
                .collect(),
        }
    }

    fn handle_connection_established(&mut self, peer_id: &PeerId) {
        if let Some(peer) = self.bootstrap_peers.get_mut(peer_id) {
            info!(
                "Connection established, stop dialing bootstrap peer {}",
                peer.multiaddr
            );
            peer.stop_redial();
            inc!(P2PMetrics::BootstrapPeersConnected);
        }
    }

    fn handle_connection_closed(&mut self, peer_id: &PeerId) {
        if let Some(peer) = self.bootstrap_peers.get_mut(peer_id) {
            warn!(
                "Connection closed, redial bootstrap peer {}",
                peer.multiaddr
            );
            peer.start_redial();
            dec!(P2PMetrics::BootstrapPeersConnected);
        }
    }

    fn handle_dial_failure(&mut self, peer_id: &PeerId) {
        if let Some(peer) = self.bootstrap_peers.get_mut(peer_id) {
            warn!("Dail failed, redial bootstrap peer {}", peer.multiaddr);
            peer.backoff_redial();
        }
    }
}

impl Stream for BootstrapPeerManager {
    type Item = Multiaddr;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        for (_, peer) in self.bootstrap_peers.iter_mut() {
            if let Some(mut dial_future) = peer.dial_future.take() {
                match dial_future.as_mut().poll(cx) {
                    Poll::Ready(()) => return Poll::Ready(Some(peer.multiaddr.clone())),
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
    fn new(multiaddr: Multiaddr) -> Self {
        let mut dial_backoff = ExponentialBackoff {
            initial_interval: BOOTSTRAP_MIN_DIAL_SECS,
            randomization_factor: BOOTSTRAP_DIAL_JITTER,
            multiplier: BOOTSTRAP_DIAL_BACKOFF,
            max_interval: BOOTSTRAP_MAX_DIAL_SECS,
            max_elapsed_time: None,
            ..ExponentialBackoff::default()
        };
        let dial_future = Some(Box::pin(time::sleep(dial_backoff.next_backoff().unwrap())));
        Self {
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
