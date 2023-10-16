use std::{
    num::NonZeroUsize,
    task::{Context, Poll},
    time::Duration,
};

use ahash::AHashMap;
use iroh_metrics::{core::MRecorder, inc, p2p::P2PMetrics};
use libp2p::{
    identify::Info as IdentifyInfo,
    swarm::{dummy, ConnectionId, DialError, NetworkBehaviour, PollParameters},
    PeerId,
};
use lru::LruCache;

pub struct PeerManager {
    info: AHashMap<PeerId, Info>,
    bad_peers: LruCache<PeerId, ()>,
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

impl Default for PeerManager {
    fn default() -> Self {
        PeerManager {
            info: Default::default(),
            bad_peers: LruCache::new(DEFAULT_BAD_PEER_CAP.unwrap()),
            supported_protocols: Default::default(),
        }
    }
}

#[derive(Debug)]
pub enum PeerManagerEvent {}

impl PeerManager {
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
                if event.other_established == 0 {
                    let p = self.bad_peers.pop(&event.peer_id);
                    if p.is_some() {
                        inc!(P2PMetrics::BadPeerRemoved);
                    }
                }

                if let Some(info) = self.info.get_mut(&event.peer_id) {
                    if let Some(ref mut info) = info.last_info {
                        info.listen_addrs
                            .retain(|addr| !event.failed_addresses.contains(addr))
                    }
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
        _cx: &mut Context<'_>,
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
