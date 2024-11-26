use std::{
    collections::HashSet,
    fmt::{self, Debug, Formatter},
    future,
    task::{Context, Poll},
    time::Duration,
};

use ahash::AHashMap;
use anyhow::{anyhow, Result};
use backoff::{backoff::Backoff, ExponentialBackoff, ExponentialBackoffBuilder};
use ceramic_core::PeerEntry;
#[allow(deprecated)]
use ceramic_metrics::Recorder;
use futures_util::{future::BoxFuture, FutureExt};
use libp2p::{
    identify::Info as IdentifyInfo,
    swarm::{dummy, ConnectionId, DialError, NetworkBehaviour},
    Multiaddr, PeerId,
};
use libp2p::{
    multiaddr::Protocol,
    swarm::{
        dial_opts::{DialOpts, PeerCondition},
        ToSwarm,
    },
};
use tokio::{
    sync::{mpsc::Sender, oneshot},
    time,
};
use tracing::{info, warn};

use crate::{
    metrics::{self, Metrics},
    peers,
};

/// Manages state for Ceramic peers.
/// Ceramic peers are peers that participate in the Ceramic network.
///
/// Not all connected peers will be Ceramic peers, for example a peer may be participating in the
/// DHT without being a Ceramic peer.
pub struct CeramicPeerManager {
    metrics: Metrics,
    info: AHashMap<PeerId, Info>,
    ceramic_peers: AHashMap<PeerId, CeramicPeer>,
    // Use a message passing technique to get peers so that we do not use the current task to do
    // DB/IO work.
    peers_tx: Sender<peers::Message>,
    peers_fut: Option<BoxFuture<'static, Result<Vec<PeerEntry>>>>,
}

#[derive(Default, Debug, Clone)]
pub struct Info {
    pub last_rtt: Option<Duration>,
    pub last_info: Option<IdentifyInfo>,
    pub connected_point: Option<Multiaddr>,
}

impl Info {
    pub fn latency(&self) -> Option<Duration> {
        // only approximation, this is wrong but the best we have for now
        self.last_rtt.map(|rtt| rtt / 2)
    }
}

const PEERING_MIN_DIAL_SECS: Duration = Duration::from_secs(1); // 1 second min between redials
const PEERING_MAX_DIAL_SECS: Duration = Duration::from_secs(300); // 5 minutes max between redials
const PEERING_DIAL_BACKOFF: f64 = 1.4;
const PEERING_DIAL_JITTER: f64 = 0.1;

#[derive(Debug)]
pub enum PeerManagerEvent {}

impl CeramicPeerManager {
    pub fn new(
        peers_tx: Sender<peers::Message>,
        ceramic_peers: &[Multiaddr],
        metrics: Metrics,
    ) -> Result<Self> {
        let ceramic_peers = ceramic_peers
            .iter()
            // Extract peer id from multiaddr
            .map(|multiaddr| {
                if let Some(peer) = multiaddr.iter().find_map(|proto| match proto {
                    Protocol::P2p(peer_id) => {
                        Some((peer_id, CeramicPeer::new(vec![multiaddr.to_owned()])))
                    }
                    _ => None,
                }) {
                    Ok(peer)
                } else {
                    Err(anyhow!("could not parse bootstrap addr {}", multiaddr))
                }
            })
            .collect::<Result<AHashMap<PeerId, CeramicPeer>, anyhow::Error>>()?;
        Ok(Self {
            metrics,
            info: Default::default(),
            ceramic_peers,
            peers_tx,
            peers_fut: None,
        })
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

    pub fn is_ceramic_peer(&self, peer_id: &PeerId) -> bool {
        self.ceramic_peers.contains_key(peer_id)
    }
    pub fn new_peers(&mut self) {
        if self.peers_fut.is_none() {
            let (tx, rx) = oneshot::channel();
            // Construct future that will resolve to the set of all known remote peers
            let peers_tx = self.peers_tx.clone();
            self.peers_fut = Some(
                async move {
                    futures::future::join(peers_tx.send(peers::Message::AllRemotePeers(tx)), rx)
                        .map(|(send, peers)| {
                            send.map_err(anyhow::Error::from)
                                .and(peers.map_err(anyhow::Error::from).and_then(|inner| inner))
                        })
                        .await
                }
                .boxed(),
            )
        } // else do nothing because we will get all peers anyways
    }

    fn handle_connection_established(&mut self, peer_id: &PeerId) {
        if let Some(peer) = self.ceramic_peers.get_mut(peer_id) {
            info!(
               %peer_id,
                multiaddr = ?peer.addrs,
                "connection established, stop dialing ceramic peer",
            );
            peer.stop_redial();
            self.metrics.record(&metrics::PeeringEvent::Connected);
        }
    }

    fn handle_connection_closed(&mut self, peer_id: &PeerId) {
        if let Some(peer) = self.ceramic_peers.get_mut(peer_id) {
            warn!(
               %peer_id,
                multiaddr = ?peer.addrs,
                "connection closed, redial ceramic peer",
            );
            peer.start_redial();
            self.metrics.record(&metrics::PeeringEvent::Disconnected);
        }
    }

    fn handle_dial_failure(&mut self, peer_id: &PeerId) {
        if let Some(peer) = self.ceramic_peers.get_mut(peer_id) {
            warn!(
               %peer_id,
                multiaddr = ?peer.addrs,
                "dial failed, redial ceramic peer"
            );
            peer.backoff_redial();
            self.metrics.record(&metrics::PeeringEvent::DialFailure);
        }
    }
}

impl NetworkBehaviour for CeramicPeerManager {
    type ConnectionHandler = dummy::ConnectionHandler;
    type ToSwarm = PeerManagerEvent;

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm) {
        match event {
            libp2p::swarm::FromSwarm::ConnectionEstablished(event) => {
                // First connection
                if event.other_established == 0 {
                    self.handle_connection_established(&event.peer_id)
                }

                let info = self.info.entry(event.peer_id).or_default();
                info.connected_point = Some(match event.endpoint {
                    libp2p::core::ConnectedPoint::Dialer { address, .. } => address.clone(),
                    libp2p::core::ConnectedPoint::Listener { send_back_addr, .. } => {
                        send_back_addr.clone()
                    }
                });
                if let Some(ref mut info) = info.last_info {
                    info.listen_addrs
                        .retain(|addr| !event.failed_addresses.contains(addr))
                }
            }
            libp2p::swarm::FromSwarm::ConnectionClosed(event) => {
                // Last connection
                if event.remaining_established == 0 {
                    self.handle_connection_closed(&event.peer_id)
                }
            }
            libp2p::swarm::FromSwarm::DialFailure(event) => {
                if let Some(peer_id) = event.peer_id {
                    match event.error {
                        DialError::DialPeerConditionFalse(_) => {
                            // Ignore dial failures that failed because of a peer condition.
                            // These are not an indication that something was wrong with the peer
                            // rather we didn't even attempt to dial the peer because we were
                            // already connected or attempting to dial concurrently etc.
                        }
                        // For any other dial failures, increase the backoff
                        _ => self.handle_dial_failure(&peer_id),
                    }
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
    ) -> Poll<libp2p::swarm::ToSwarm<Self::ToSwarm, libp2p::swarm::THandlerInEvent<Self>>> {
        if let Some(mut peers) = self.peers_fut.take() {
            match peers.poll_unpin(cx) {
                Poll::Ready(peers) => match peers {
                    Ok(peers) => {
                        for peer_entry in peers {
                            self.ceramic_peers
                                .entry(peer_entry.id().peer_id())
                                .and_modify(|peer| {
                                    let count = peer.addrs.len();
                                    peer.addrs.extend(peer_entry.addresses().iter().cloned());
                                    if count != peer.addrs.len() {
                                        peer.start_redial()
                                    }
                                })
                                .or_insert(CeramicPeer::new(peer_entry.addresses().to_vec()));
                        }
                    }
                    Err(err) => warn!(%err,"failed to get set of remote peers"),
                },
                Poll::Pending => {
                    self.peers_fut.replace(peers);
                }
            }
        }
        for (peer_id, peer) in self.ceramic_peers.iter_mut() {
            if let Some(mut dial_future) = peer.dial_future.take() {
                match dial_future.as_mut().poll_unpin(cx) {
                    Poll::Ready(()) => {
                        return Poll::Ready(ToSwarm::Dial {
                            opts: DialOpts::peer_id(*peer_id)
                                .addresses(peer.addrs.iter().cloned().collect())
                                .condition(PeerCondition::Disconnected)
                                .build(),
                        })
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

// State of Ceramic peer.
struct CeramicPeer {
    addrs: HashSet<Multiaddr>,
    dial_backoff: ExponentialBackoff,
    dial_future: Option<BoxFuture<'static, ()>>,
}

impl Debug for CeramicPeer {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        f.debug_struct("BootstrapPeer")
            .field("multiaddr", &self.addrs)
            .field("dial_backoff", &self.dial_backoff)
            .field("dial_future", &self.dial_future.is_some())
            .finish()
    }
}

impl CeramicPeer {
    fn new(addrs: Vec<Multiaddr>) -> Self {
        let dial_backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(PEERING_MIN_DIAL_SECS)
            .with_multiplier(PEERING_DIAL_BACKOFF)
            .with_randomization_factor(PEERING_DIAL_JITTER)
            .with_max_interval(PEERING_MAX_DIAL_SECS)
            .with_max_elapsed_time(None)
            .build();
        // Expire initial future so that we dial peers immediately
        let dial_future = Some(future::ready(()).boxed());
        Self {
            addrs: addrs.into_iter().collect(),
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
