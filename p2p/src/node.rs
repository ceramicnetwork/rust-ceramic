use std::{
    collections::{HashMap, HashSet},
    sync::atomic::AtomicBool,
};
use std::{fmt, sync::Arc};
use std::{sync::atomic::Ordering, time::Duration};

use ahash::AHashMap;
use anyhow::{anyhow, bail, Context, Result};
use ceramic_core::{EventId, Interest};
use ceramic_metrics::{libp2p_metrics, Recorder};
use cid::Cid;
use futures_util::stream::StreamExt;
use iroh_bitswap::{BitswapEvent, Block};
use iroh_rpc_client::Client as RpcClient;
use iroh_rpc_client::Lookup;
use iroh_rpc_types::p2p::P2pAddr;
use libp2p::{
    autonat::{self, OutboundProbeEvent},
    core::Multiaddr,
    identify,
    identity::Keypair,
    kad::{
        self, BootstrapOk, GetClosestPeersError, GetClosestPeersOk, GetProvidersOk, QueryId,
        QueryResult,
    },
    mdns,
    metrics::Recorder as _,
    multiaddr::Protocol,
    swarm::{dial_opts::DialOpts, NetworkBehaviour, SwarmEvent},
    PeerId, StreamProtocol, Swarm,
};
use tokio::sync::oneshot::{self, Sender as OneShotSender};
use tokio::task::JoinHandle;
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    time::Instant,
};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    behaviour::{Event, NodeBehaviour},
    keys::{Keychain, Storage},
    metrics::{LoopEvent, Metrics},
    providers::Providers,
    rpc::{self, RpcMessage},
    rpc::{P2p, ProviderRequestKey},
    swarm::build_swarm,
    Config,
};
use recon::{libp2p::Recon, Sha256a};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum NetworkEvent {
    PeerConnected(PeerId),
    PeerDisconnected(PeerId),
    CancelLookupQuery(PeerId),
}

/// Node implements a peer to peer node that participates on the Ceramic network.
///
/// Node provides an external API via RpcMessages.
pub struct Node<I, M, S>
where
    I: Recon<Key = Interest, Hash = Sha256a>,
    M: Recon<Key = EventId, Hash = Sha256a>,
    S: iroh_bitswap::Store,
{
    metrics: Metrics,
    swarm: Swarm<NodeBehaviour<I, M, S>>,
    supported_protocols: HashSet<String>,
    net_receiver_in: Receiver<RpcMessage>,
    dial_queries: AHashMap<PeerId, Vec<OneShotSender<Result<()>>>>,
    lookup_queries: AHashMap<PeerId, Vec<oneshot::Sender<Result<identify::Info>>>>,
    // TODO(ramfox): use new providers queue instead
    find_on_dht_queries: AHashMap<Vec<u8>, DHTQuery>,
    network_events: Vec<(Arc<AtomicBool>, Sender<NetworkEvent>)>,
    #[allow(dead_code)]
    rpc_client: RpcClient,
    rpc_task: JoinHandle<()>,
    use_dht: bool,
    bitswap_sessions: BitswapSessions,
    providers: Providers,
    listen_addrs: Vec<Multiaddr>,
    trust_observed_addrs: bool,
    failed_external_addresses: HashSet<Multiaddr>,
    active_address_probe: Option<Multiaddr>,
}

impl<I, M, S> fmt::Debug for Node<I, M, S>
where
    I: Recon<Key = Interest, Hash = Sha256a>,
    M: Recon<Key = EventId, Hash = Sha256a>,
    S: iroh_bitswap::Store,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("swarm", &"Swarm<NodeBehaviour>")
            .field("net_receiver_in", &self.net_receiver_in)
            .field("dial_queries", &self.dial_queries)
            .field("lookup_queries", &self.lookup_queries)
            .field("find_on_dht_queries", &self.find_on_dht_queries)
            .field("network_events", &self.network_events)
            .field("rpc_client", &self.rpc_client)
            .field("rpc_task", &self.rpc_task)
            .field("use_dht", &self.use_dht)
            .field("bitswap_sessions", &self.bitswap_sessions)
            .field("providers", &self.providers)
            .finish()
    }
}

// TODO(ramfox): use new providers queue instead
type DHTQuery = (PeerId, Vec<oneshot::Sender<Result<()>>>);

type BitswapSessions = AHashMap<u64, Vec<(oneshot::Sender<()>, JoinHandle<()>)>>;

pub(crate) const DEFAULT_PROVIDER_LIMIT: usize = 10;
const NICE_INTERVAL: Duration = Duration::from_secs(6);
const BOOTSTRAP_INTERVAL: Duration = Duration::from_secs(5 * 60);
const EXPIRY_INTERVAL: Duration = Duration::from_secs(1);

impl<I, M, S> Drop for Node<I, M, S>
where
    I: Recon<Key = Interest, Hash = Sha256a>,
    M: Recon<Key = EventId, Hash = Sha256a>,
    S: iroh_bitswap::Store,
{
    fn drop(&mut self) {
        self.rpc_task.abort();
    }
}

// Allow IntoConnectionHandler deprecated associated type.
// We are not using IntoConnectionHandler directly only referencing the type as part of this event signature.
type NodeSwarmEvent<I, M, S> = SwarmEvent<<NodeBehaviour<I, M, S> as NetworkBehaviour>::ToSwarm>;
impl<I, M, S> Node<I, M, S>
where
    I: Recon<Key = Interest, Hash = Sha256a> + Send + Sync,
    M: Recon<Key = EventId, Hash = Sha256a> + Send + Sync,
    S: iroh_bitswap::Store + Send + Sync,
{
    pub async fn new(
        config: Config,
        rpc_addr: P2pAddr,
        keypair: Keypair,
        recons: Option<(I, M)>,
        block_store: Arc<S>,
        metrics: Metrics,
    ) -> Result<Self> {
        let (network_sender_in, network_receiver_in) = channel(1024); // TODO: configurable

        let Config {
            libp2p: libp2p_config,
            rpc_client,
            ..
        } = config;

        let mut swarm = build_swarm(
            &libp2p_config,
            keypair,
            recons,
            block_store,
            metrics.clone(),
        )
        .await?;
        info!("iroh-p2p peerid: {}", swarm.local_peer_id());

        for addr in &libp2p_config.external_multiaddrs {
            swarm.add_external_address(addr.clone());
        }

        let mut listen_addrs = vec![];
        for addr in &libp2p_config.listening_multiaddrs {
            Swarm::listen_on(&mut swarm, addr.clone())?;
            listen_addrs.push(addr.clone());
        }

        // The following two statements were intentionally placed right before the return. Having them sooner caused the
        // daemon to get stuck in a loop during shutdown, unable to bind to a listen address, if there was some
        // initialization error after the RPC task was spawned, e.g. while parsing bootstrap peer multiaddrs in the
        // PeerManager.
        let rpc_task = tokio::task::spawn(async move {
            // TODO: handle error
            rpc::new(rpc_addr, P2p::new(network_sender_in))
                .await
                .unwrap()
        });

        let rpc_client = RpcClient::new(rpc_client)
            .await
            .context("failed to create rpc client")?;

        Ok(Node {
            metrics,
            swarm,
            // TODO(WS1-1364): Determine psuedo-dynamically the set of locally supported protocols.
            // For now hard code all protocols.
            // https://github.com/libp2p/rust-libp2p/discussions/4982
            supported_protocols: HashSet::from_iter(
                [
                    "/ipfs/bitswap",
                    "/ipfs/bitswap/1.0.0",
                    "/ipfs/bitswap/1.1.0",
                    "/ipfs/bitswap/1.2.0",
                    "/ipfs/id/1.0.0",
                    "/ipfs/id/push/1.0.0",
                    "/ipfs/kad/1.0.0",
                    "/ipfs/ping/1.0.0",
                    "/libp2p/autonat/1.0.0",
                    "/libp2p/circuit/relay/0.2.0/hop",
                    "/libp2p/circuit/relay/0.2.0/stop",
                    "/meshsub/1.0.0",
                    "/meshsub/1.1.0",
                ]
                .iter()
                .map(|p| p.to_string()),
            ),
            net_receiver_in: network_receiver_in,
            dial_queries: Default::default(),
            lookup_queries: Default::default(),
            // TODO(ramfox): use new providers queue instead
            find_on_dht_queries: Default::default(),
            network_events: Vec::new(),
            rpc_client,
            rpc_task,
            use_dht: libp2p_config.kademlia,
            bitswap_sessions: Default::default(),
            providers: Providers::new(4),
            listen_addrs,
            trust_observed_addrs: libp2p_config.trust_observed_addrs,
            failed_external_addresses: Default::default(),
            active_address_probe: Default::default(),
        })
    }

    pub fn listen_addrs(&self) -> &Vec<Multiaddr> {
        &self.listen_addrs
    }

    pub fn local_peer_id(&self) -> &PeerId {
        self.swarm.local_peer_id()
    }

    /// Starts the libp2p service networking stack. This Future resolves when shutdown occurs.
    #[instrument(skip_all)]
    pub async fn run(&mut self) -> Result<()> {
        info!("Listen addrs: {:?}", self.listen_addrs());
        info!("Local Peer ID: {}", self.local_peer_id());

        let mut nice_interval = self.use_dht.then(|| tokio::time::interval(NICE_INTERVAL));
        // Initialize bootstrap_interval to not start immediately but at now + interval.
        // This is because we know that initially there are no nodes with whom to bootstrap.
        // This interval can be reset if we find a kademlia node before the first tick.
        let mut bootstrap_interval =
            tokio::time::interval_at(Instant::now() + BOOTSTRAP_INTERVAL, BOOTSTRAP_INTERVAL);
        let mut expiry_interval = tokio::time::interval(EXPIRY_INTERVAL);

        #[derive(Debug)]
        enum KadBootstrapState {
            // Kademlia is idle as it does not have any peers to communicate with.
            Idle,
            // Kademlia has begun the process of bootstrapping with at least one peer.
            Bootstrapping,
            // Kademlia has finished the bootstrap process.
            Bootstrapped,
        }

        let mut kad_state = KadBootstrapState::Idle;
        loop {
            self.metrics.record(&LoopEvent);
            tokio::select! {
                swarm_event = self.swarm.next() => {
                    let swarm_event = swarm_event.expect("the swarm will never die");
                    match self.handle_swarm_event(swarm_event).await {
                        Ok(Some(SwarmEventResult::KademliaBoostrapSuccess)) => {
                            kad_state = KadBootstrapState::Bootstrapped;
                        }
                        Ok(Some(SwarmEventResult::KademliaAddressAdded)) => {
                            if matches!(kad_state, KadBootstrapState::Idle) {
                                kad_state = KadBootstrapState::Bootstrapping;
                                bootstrap_interval.reset_immediately();
                            }
                        }
                        Ok(None) => {},
                        Err(err) => error!("swarm error: {:?}",err),
                    };

                    if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                        self.providers.poll(kad);
                    }
                }
                rpc_message = self.net_receiver_in.recv() => {
                    match rpc_message {
                        Some(rpc_message) => {
                            match self.handle_rpc_message(rpc_message).await {
                                Ok(true) => {
                                    // shutdown
                                    return Ok(());
                                }
                                Ok(false) => {
                                    continue;
                                }
                                Err(err) => {
                                    warn!("rpc: {:?}", err);
                                }
                            }
                        }
                        None => {
                            // shutdown
                            return Ok(());
                        }
                    }
                }
                _ = async {
                    if let Some(ref mut nice_interval) = nice_interval {
                        nice_interval.tick().await
                    } else {
                        unreachable!()
                    }
                }, if nice_interval.is_some() => {
                    // Print peer count on an interval.
                    info!("Peers connected: {:?}", self.swarm.connected_peers().count());
                }
                _ = bootstrap_interval.tick() => {
                    if let Err(e) = self.swarm.behaviour_mut().kad_bootstrap() {
                        warn!("kad bootstrap failed: {:?}", e);
                        kad_state = KadBootstrapState::Idle;
                    } else {
                        debug!("kad bootstrap succeeded");
                    }
                }
                _ = expiry_interval.tick() => {
                    if let Err(err) = self.expiry() {
                        warn!("expiry error {:?}", err);
                    }
                }
            }
        }
    }

    fn expiry(&mut self) -> Result<()> {
        // Cleanup bitswap sessions
        let mut to_remove = Vec::new();
        for (session_id, workers) in &mut self.bitswap_sessions {
            // Check if the workers are still active
            workers.retain(|(_, worker)| !worker.is_finished());

            if workers.is_empty() {
                to_remove.push(*session_id);
            }

            // Only do a small chunk of cleanup on each iteration
            // TODO(arqu): magic number
            if to_remove.len() >= 10 {
                break;
            }
        }

        for session_id in to_remove {
            let (s, _r) = oneshot::channel();
            self.destroy_session(session_id, s);
        }

        Ok(())
    }

    /// Subscribe to [`NetworkEvent`]s.
    #[tracing::instrument(skip(self))]
    pub fn network_events(&mut self) -> Receiver<NetworkEvent> {
        let (s, r) = channel(512);
        self.network_events
            .push((Arc::new(AtomicBool::new(true)), s));
        r
    }

    fn destroy_session(&mut self, ctx: u64, response_channel: oneshot::Sender<Result<()>>) {
        if let Some(bs) = self.swarm.behaviour().bitswap.as_ref() {
            let workers = self.bitswap_sessions.remove(&ctx);
            let client = bs.client().clone();
            tokio::task::spawn(async move {
                debug!("stopping session {}", ctx);
                if let Some(workers) = workers {
                    debug!("stopping workers {} for session {}", workers.len(), ctx);
                    // first shutdown workers
                    for (closer, worker) in workers {
                        if closer.send(()).is_ok() {
                            worker.await.ok();
                        }
                    }
                    debug!("all workers stopped for session {}", ctx);
                }
                if let Err(err) = client.stop_session(ctx).await {
                    warn!("failed to stop session {}: {:?}", ctx, err);
                }
                // Ignore error if the otherside already hung up.
                let _ = response_channel.send(Ok(()));
                debug!("session {} stopped", ctx);
            });
        } else {
            let _ = response_channel.send(Err(anyhow!("no bitswap available")));
        }
    }

    /// Send a request for data over bitswap
    fn want_block(
        &mut self,
        ctx: u64,
        cid: Cid,
        providers: HashSet<PeerId>,
        mut chan: OneShotSender<Result<Block, String>>,
    ) -> Result<()> {
        if let Some(bs) = self.swarm.behaviour().bitswap.as_ref() {
            let client = bs.client().clone();
            let (closer_s, closer_r) = oneshot::channel();

            let entry = self.bitswap_sessions.entry(ctx).or_default();

            let providers: Vec<_> = providers.into_iter().collect();
            let worker = tokio::task::spawn(async move {
                tokio::select! {
                    _ = closer_r => {
                        // Explicit session stop.
                        debug!("session {}: stopped: closed", ctx);
                    }
                    _ = chan.closed() => {
                        // RPC dropped
                        debug!("session {}: stopped: request canceled", ctx);
                    }
                    block = client.get_block_with_session_id(ctx, &cid, &providers) => match block {
                        Ok(block) => {
                            if let Err(e) = chan.send(Ok(block)) {
                                warn!("failed to send block response: {:?}", e);
                            }
                        }
                        Err(err) => {
                            chan.send(Err(err.to_string())).ok();
                        }
                    },
                }
            });
            entry.push((closer_s, worker));

            Ok(())
        } else {
            bail!("no bitswap available");
        }
    }

    // TODO fix skip_all
    #[tracing::instrument(skip_all)]
    async fn handle_swarm_event(
        &mut self,
        event: NodeSwarmEvent<I, M, S>,
    ) -> Result<Option<SwarmEventResult>> {
        libp2p_metrics().record(&event);
        match event {
            // outbound events
            SwarmEvent::Behaviour(event) => self.handle_node_event(event).await,
            SwarmEvent::ConnectionEstablished {
                peer_id,
                num_established,
                ..
            } => {
                if let Some(channels) = self.dial_queries.get_mut(&peer_id) {
                    while let Some(channel) = channels.pop() {
                        channel.send(Ok(())).ok();
                    }
                }

                if num_established == 1.try_into().unwrap() {
                    self.emit_network_event(NetworkEvent::PeerConnected(peer_id));
                }
                trace!("ConnectionEstablished: {:}", peer_id);
                Ok(None)
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                ..
            } => {
                if num_established == 0 {
                    self.emit_network_event(NetworkEvent::PeerDisconnected(peer_id));
                }

                trace!("ConnectionClosed: {:}", peer_id);
                Ok(None)
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                trace!("failed to dial: {:?}, {:?}", peer_id, error);

                if let Some(peer_id) = peer_id {
                    if let Some(channels) = self.dial_queries.get_mut(&peer_id) {
                        while let Some(channel) = channels.pop() {
                            channel
                                .send(Err(anyhow!("Error dialing peer {:?}: {}", peer_id, error)))
                                .ok();
                        }
                    }
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    #[tracing::instrument(skip(self))]
    fn emit_network_event(&mut self, ev: NetworkEvent) {
        let mut to_remove = Vec::new();
        for (i, (open, sender)) in self.network_events.iter_mut().enumerate() {
            if !open.load(Ordering::Relaxed) {
                to_remove.push(i);
                continue;
            }
            let ev = ev.clone();
            let sender = sender.clone();
            let open = open.clone();
            tokio::task::spawn(async move {
                if let Err(_e) = sender.send(ev.clone()).await {
                    // Mark sender as closed so we stop sending events to it
                    open.store(false, Ordering::Relaxed);
                }
            });
        }
        for idx in to_remove.iter().rev() {
            self.network_events.swap_remove(*idx);
        }
    }

    #[tracing::instrument(skip_all)]
    async fn handle_node_event(&mut self, event: Event) -> Result<Option<SwarmEventResult>> {
        match event {
            Event::Bitswap(e) => {
                match e {
                    BitswapEvent::Provide { key } => {
                        info!("bitswap provide {}", key);
                        if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                            match kad.start_providing(key.hash().to_bytes().into()) {
                                Ok(_query_id) => {
                                    // TODO: track query?
                                }
                                Err(err) => {
                                    error!("failed to provide {}: {:?}", key, err);
                                }
                            }
                        };
                        Ok(None)
                    }
                    BitswapEvent::FindProviders {
                        key,
                        response,
                        limit,
                    } => {
                        info!("bitswap find providers {}", key);
                        self.handle_rpc_message(RpcMessage::ProviderRequest {
                            key: ProviderRequestKey::Dht(key.hash().to_bytes().into()),
                            response_channel: response,
                            limit,
                        })
                        .await?;
                        Ok(None)
                    }
                    BitswapEvent::Ping { peer, response } => {
                        match self.swarm.behaviour().peer_manager.info_for_peer(&peer) {
                            Some(info) => {
                                response.send(info.latency()).ok();
                                Ok(None)
                            }
                            None => {
                                response.send(None).ok();
                                Ok(None)
                            }
                        }
                    }
                }
            }
            Event::Kademlia(e) => {
                libp2p_metrics().record(&e);

                if let kad::Event::OutboundQueryProgressed {
                    id, result, step, ..
                } = e
                {
                    match result {
                        QueryResult::StartProviding(_result) => Ok(None),
                        QueryResult::GetProviders(Ok(p)) => {
                            match p {
                                GetProvidersOk::FoundProviders { key, providers } => {
                                    let behaviour = self.swarm.behaviour_mut();
                                    if let Some(kad) = behaviour.kad.as_mut() {
                                        debug!(
                                            "provider results for {:?} last: {}",
                                            key, step.last
                                        );

                                        self.providers.handle_get_providers_ok(
                                            id, step.last, key, providers, kad,
                                        );
                                    }
                                }
                                GetProvidersOk::FinishedWithNoAdditionalRecord { .. } => {
                                    let swarm = self.swarm.behaviour_mut();
                                    if let Some(kad) = swarm.kad.as_mut() {
                                        debug!(
                                            "FinishedWithNoAdditionalRecord for query {:#?}",
                                            id
                                        );
                                        self.providers.handle_no_additional_records(id, kad);
                                    }
                                }
                            }
                            Ok(None)
                        }
                        QueryResult::GetProviders(Err(error)) => {
                            if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                                self.providers.handle_get_providers_error(id, error, kad);
                            }
                            Ok(None)
                        }
                        QueryResult::Bootstrap(Ok(BootstrapOk {
                            peer,
                            num_remaining,
                        })) => {
                            debug!(
                                "kad bootstrap done {:?}, remaining: {}",
                                peer, num_remaining
                            );
                            Ok(Some(SwarmEventResult::KademliaBoostrapSuccess))
                        }
                        QueryResult::Bootstrap(Err(e)) => {
                            warn!("kad bootstrap error: {:?}", e);
                            Ok(None)
                        }
                        QueryResult::GetClosestPeers(Ok(GetClosestPeersOk { key, peers })) => {
                            debug!("GetClosestPeers ok {:?}", key);
                            if let Some((peer_id, channels)) = self.find_on_dht_queries.remove(&key)
                            {
                                let have_peer = peers.contains(&peer_id);
                                // if this is not the last step we will have more chances to find
                                // the peer
                                if !have_peer && !step.last {
                                    return Ok(None);
                                }
                                let res = move || {
                                    if have_peer {
                                        Ok(())
                                    } else {
                                        Err(anyhow!("Failed to find peer {:?} on the DHT", peer_id))
                                    }
                                };
                                tokio::task::spawn(async move {
                                    for chan in channels.into_iter() {
                                        chan.send(res()).ok();
                                    }
                                });
                            }
                            Ok(None)
                        }
                        QueryResult::GetClosestPeers(Err(GetClosestPeersError::Timeout {
                            key,
                            ..
                        })) => {
                            debug!("GetClosestPeers Timeout: {:?}", key);
                            if let Some((peer_id, channels)) = self.find_on_dht_queries.remove(&key)
                            {
                                tokio::task::spawn(async move {
                                    for chan in channels.into_iter() {
                                        chan.send(Err(anyhow!(
                                            "Failed to find peer {:?} on the DHT: Timeout",
                                            peer_id
                                        )))
                                        .ok();
                                    }
                                });
                            }
                            Ok(None)
                        }
                        other => {
                            debug!("Libp2p => Unhandled Kademlia query result: {:?}", other);
                            Ok(None)
                        }
                    }
                } else {
                    Ok(None)
                }
            }
            Event::Identify(e) => {
                libp2p_metrics().record(&*e);
                trace!("tick: identify {:?}", e);
                match *e {
                    identify::Event::Received { peer_id, info } => {
                        // Did we learn about a new external address?
                        if !self
                            .swarm
                            .external_addresses()
                            .any(|addr| addr == &info.observed_addr)
                            && !self.failed_external_addresses.contains(&info.observed_addr)
                        {
                            if self.trust_observed_addrs {
                                debug!(
                                    address=%info.observed_addr,
                                    %peer_id,
                                    "adding trusted external address observed from peer",
                                );
                                // Explicily trust any observed address from any peer.
                                self.swarm.add_external_address(info.observed_addr.clone());
                            } else if let Some(autonat) =
                                self.swarm.behaviour_mut().autonat.as_mut()
                            {
                                // Probe the observed addr for external connectivity.
                                // Only probe one address at a time.
                                //
                                // This logic is run very frequently because any new peer connection
                                // for a new observed address triggers this path. Its typical to have
                                // only a few external addresses, in which cases its likely that the
                                // in-progress address probe is one that will succeed.
                                //
                                // In cases where there are lots of different observed addresses its
                                // likely that NAT hasn't been setup and so the peer doesn't have an
                                // external address. Therefore we do not want to waste resources on
                                // probing many different addresses that are likely to fail.
                                if self.active_address_probe.is_none() {
                                    self.active_address_probe = Some(info.observed_addr.clone());
                                    debug!(
                                        address=%info.observed_addr,
                                        %peer_id,
                                        "probing observed address from peer for external connectivity",
                                    );
                                    autonat.probe_address(info.observed_addr.clone());
                                }
                            };
                        };

                        let mut kad_address_added = false;
                        for protocol in &info.protocols {
                            // Sometimes peers do not report that they support the kademlia protocol.
                            // Here we assume that all ceramic peers do support the protocol.
                            // Therefore we add all ceramic peers and any peers that explicitly support
                            // kademlia to the kademlia routing table.
                            if self
                                .swarm
                                .behaviour()
                                .peer_manager
                                .is_ceramic_peer(&peer_id)
                                || protocol == &kad::PROTOCOL_NAME
                            {
                                for addr in &info.listen_addrs {
                                    if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                                        kad.add_address(&peer_id, addr.clone());
                                        kad_address_added = true;
                                    }
                                }
                            } else if protocol == &StreamProtocol::new("/libp2p/autonat/1.0.0") {
                                // TODO: expose protocol name on `libp2p::autonat`.
                                // TODO: should we remove them at some point?
                                for addr in &info.listen_addrs {
                                    if let Some(autonat) =
                                        self.swarm.behaviour_mut().autonat.as_mut()
                                    {
                                        autonat.add_server(peer_id, Some(addr.clone()));
                                    }
                                }
                            }
                        }
                        if let Some(bitswap) = self.swarm.behaviour().bitswap.as_ref() {
                            bitswap.on_identify(&peer_id, &info.protocols);
                        }

                        self.swarm
                            .behaviour_mut()
                            .peer_manager
                            .inject_identify_info(peer_id, info.clone());

                        if let Some(channels) = self.lookup_queries.remove(&peer_id) {
                            for chan in channels {
                                chan.send(Ok(info.clone())).ok();
                            }
                        }
                        if kad_address_added {
                            Ok(Some(SwarmEventResult::KademliaAddressAdded))
                        } else {
                            Ok(None)
                        }
                    }
                    identify::Event::Error { peer_id, error } => {
                        if let Some(channels) = self.lookup_queries.remove(&peer_id) {
                            for chan in channels {
                                chan.send(Err(anyhow!(
                                    "error upgrading connection to peer {:?}: {}",
                                    peer_id,
                                    error
                                )))
                                .ok();
                            }
                        }
                        Ok(None)
                    }
                    identify::Event::Sent { .. } | identify::Event::Pushed { .. } => Ok(None),
                }
            }
            Event::Ping(e) => {
                libp2p_metrics().record(&e);
                if let Ok(rtt) = e.result {
                    self.swarm
                        .behaviour_mut()
                        .peer_manager
                        .inject_ping(e.peer, rtt);
                }
                Ok(None)
            }
            Event::Relay(e) => {
                libp2p_metrics().record(&e);
                Ok(None)
            }
            Event::Dcutr(e) => {
                libp2p_metrics().record(&e);
                Ok(None)
            }
            Event::Mdns(e) => {
                match e {
                    mdns::Event::Discovered(peers) => {
                        for (peer_id, addr) in peers {
                            let is_connected = self.swarm.is_connected(&peer_id);
                            debug!(
                                "mdns: discovered {} at {} (connected: {:?})",
                                peer_id, addr, is_connected
                            );
                            if !is_connected {
                                let dial_opts =
                                    DialOpts::peer_id(peer_id).addresses(vec![addr]).build();
                                if let Err(e) = self.swarm.dial(dial_opts) {
                                    debug!("mdns dial failed: {:?}", e);
                                }
                            }
                        }
                    }
                    mdns::Event::Expired(_) => {}
                };
                Ok(None)
            }
            Event::Autonat(autonat::Event::OutboundProbe(OutboundProbeEvent::Response {
                address,
                ..
            })) => {
                if !self.swarm.external_addresses().any(|addr| addr == &address) {
                    debug!(
                        %address,
                        "adding external address after successful autonat probe",
                    );
                    self.swarm.add_external_address(address);
                }
                Ok(None)
            }
            Event::Autonat(autonat::Event::OutboundProbe(OutboundProbeEvent::Error { .. })) => {
                if let Some(addr) = self.active_address_probe.take() {
                    self.failed_external_addresses.insert(addr);
                }
                Ok(None)
            }
            _ => {
                // TODO: check all important events are handled
                Ok(None)
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn handle_rpc_message(&mut self, message: RpcMessage) -> Result<bool> {
        // Inbound messages
        match message {
            RpcMessage::ExternalAddrs(response_channel) => {
                response_channel
                    .send(self.swarm.external_addresses().cloned().collect())
                    .ok();
            }
            RpcMessage::Listeners(response_channel) => {
                response_channel
                    .send(self.swarm.listeners().cloned().collect())
                    .ok();
            }
            RpcMessage::LocalPeerId(response_channel) => {
                response_channel.send(*self.swarm.local_peer_id()).ok();
            }
            RpcMessage::BitswapRequest {
                ctx,
                cids,
                response_channels,
                providers,
            } => {
                trace!("context:{} bitswap_request", ctx);
                for (cid, response_channel) in cids.into_iter().zip(response_channels.into_iter()) {
                    self.want_block(ctx, cid, providers.clone(), response_channel)
                        .map_err(|err| anyhow!("Failed to send a bitswap want_block: {:?}", err))?;
                }
            }
            RpcMessage::BitswapNotifyNewBlocks {
                blocks,
                response_channel,
            } => {
                self.swarm.behaviour().notify_new_blocks(blocks);
                response_channel.send(Ok(())).ok();
            }
            RpcMessage::BitswapStopSession {
                ctx,
                response_channel,
            } => {
                self.destroy_session(ctx, response_channel);
            }
            RpcMessage::ProviderRequest {
                key,
                limit,
                response_channel,
            } => match key {
                ProviderRequestKey::Dht(key) => {
                    debug!("fetching providers for: {:?}", key);
                    if self.swarm.behaviour().kad.is_enabled() {
                        if !self.providers.push(key.clone(), limit, response_channel) {
                            warn!("provider query dropped because the queue is full {:?}", key);
                        }
                    } else {
                        tokio::task::spawn(async move {
                            response_channel
                                .send(Err("kademlia is not available".into()))
                                .await
                                .ok();
                        });
                    }
                }
                ProviderRequestKey::Bitswap(_, _) => {
                    debug!(
                        "RpcMessage::ProviderRequest: getting providers for {:?}",
                        key
                    );

                    // TODO
                }
            },
            RpcMessage::StartProviding(response_channel, key) => {
                if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                    debug!("kad.start_providing {:?}", key);
                    let res: Result<QueryId> = kad.start_providing(key).map_err(|e| e.into());
                    // TODO: wait for kad to process the query request before returning
                    response_channel.send(res).ok();
                } else {
                    response_channel
                        .send(Err(anyhow!("kademlia is not available")))
                        .ok();
                }
            }
            RpcMessage::StopProviding(response_channel, key) => {
                if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                    kad.stop_providing(&key);
                    response_channel.send(Ok(())).ok();
                } else {
                    response_channel
                        .send(Err(anyhow!("kademlia is not available")))
                        .ok();
                }
            }
            RpcMessage::NetListeningAddrs(response_channel) => {
                let mut listeners: Vec<_> = Swarm::listeners(&self.swarm).cloned().collect();
                let peer_id = *Swarm::local_peer_id(&self.swarm);
                listeners.extend(Swarm::external_addresses(&self.swarm).cloned());

                response_channel
                    .send((peer_id, listeners))
                    .map_err(|_| anyhow!("Failed to get Libp2p listeners"))?;
            }
            RpcMessage::NetPeers(response_channel) => {
                #[allow(clippy::needless_collect)]
                let peers = self.swarm.connected_peers().copied().collect::<Vec<_>>();
                let peer_addresses: HashMap<PeerId, Vec<Multiaddr>> = peers
                    .into_iter()
                    .map(|pid| {
                        (
                            pid,
                            self.swarm
                                .behaviour_mut()
                                .peer_manager
                                .info_for_peer(&pid)
                                .map(|info| {
                                    info.last_info
                                        .as_ref()
                                        .map(|last_info| last_info.listen_addrs.clone())
                                        .unwrap_or_default()
                                })
                                .unwrap_or_default(),
                        )
                    })
                    .collect();

                response_channel
                    .send(peer_addresses)
                    .map_err(|_| anyhow!("Failed to get Libp2p peers"))?;
            }
            RpcMessage::NetConnect(response_channel, peer_id, addrs) => {
                if self.swarm.is_connected(&peer_id) {
                    response_channel.send(Ok(())).ok();
                } else {
                    let channels = self.dial_queries.entry(peer_id).or_default();
                    channels.push(response_channel);

                    // when using DialOpts::peer_id, having the `P2p` protocol as part of the
                    // added addresses throws an error
                    // we can filter out that protocol before adding the addresses to the dial opts
                    let addrs = addrs
                        .iter()
                        .map(|a| {
                            a.iter()
                                .filter(|p| !matches!(*p, Protocol::P2p(_)))
                                .collect()
                        })
                        .collect();
                    let dial_opts = DialOpts::peer_id(peer_id)
                        .addresses(addrs)
                        .condition(libp2p::swarm::dial_opts::PeerCondition::Always)
                        .build();
                    if let Err(e) = Swarm::dial(&mut self.swarm, dial_opts) {
                        warn!("invalid dial options: {:?}", e);
                        while let Some(channel) = channels.pop() {
                            channel
                                .send(Err(anyhow!("error dialing peer {:?}: {}", peer_id, e)))
                                .ok();
                        }
                    }
                }
            }
            RpcMessage::NetConnectByPeerId(response_channel, peer_id) => {
                if self.swarm.is_connected(&peer_id) {
                    response_channel.send(Ok(())).ok();
                } else {
                    let channels = self.dial_queries.entry(peer_id).or_default();
                    channels.push(response_channel);

                    let dial_opts = DialOpts::peer_id(peer_id)
                        .condition(libp2p::swarm::dial_opts::PeerCondition::Always)
                        .build();
                    if let Err(e) = Swarm::dial(&mut self.swarm, dial_opts) {
                        while let Some(channel) = channels.pop() {
                            channel
                                .send(Err(anyhow!("error dialing peer {:?}: {}", peer_id, e)))
                                .ok();
                        }
                    }
                }
            }
            RpcMessage::AddressesOfPeer(response_channel, peer_id) => {
                let addrs = self
                    .swarm
                    .behaviour_mut()
                    .peer_manager
                    .info_for_peer(&peer_id)
                    .map(|info| {
                        info.last_info
                            .as_ref()
                            .map(|last_info| last_info.listen_addrs.clone())
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();
                response_channel.send(addrs).ok();
            }
            RpcMessage::NetDisconnect(response_channel, _peer_id) => {
                warn!("NetDisconnect API not yet implemented"); // TODO: implement NetDisconnect

                response_channel
                    .send(())
                    .map_err(|_| anyhow!("sender dropped"))?;
            }
            RpcMessage::ListenForIdentify(response_channel, peer_id) => {
                let channels = self.lookup_queries.entry(peer_id).or_default();
                channels.push(response_channel);
            }
            RpcMessage::LookupPeerInfo(response_channel, peer_id) => {
                if let Some(info) = self.swarm.behaviour().peer_manager.info_for_peer(&peer_id) {
                    let info = info.last_info.clone();
                    response_channel.send(info).ok();
                } else {
                    response_channel.send(None).ok();
                }
            }
            RpcMessage::LookupLocalPeerInfo(response_channel) => {
                let peer_id = self.swarm.local_peer_id();
                let listen_addrs = self.swarm.listeners().cloned().collect();
                let observed_addrs = self.swarm.external_addresses().cloned().collect();
                let protocol_version = String::from(crate::behaviour::PROTOCOL_VERSION);
                let agent_version = String::from(crate::behaviour::AGENT_VERSION);
                let mut protocols: Vec<String> = self.supported_protocols.iter().cloned().collect();
                protocols.sort();

                response_channel
                    .send(Lookup {
                        peer_id: *peer_id,
                        listen_addrs,
                        observed_addrs,
                        agent_version,
                        protocol_version,
                        protocols,
                    })
                    .ok();
            }
            RpcMessage::CancelListenForIdentify(response_channel, peer_id) => {
                self.lookup_queries.remove(&peer_id);
                self.emit_network_event(NetworkEvent::CancelLookupQuery(peer_id));
                response_channel.send(()).ok();
            }
            RpcMessage::FindPeerOnDHT(response_channel, peer_id) => {
                debug!("find closest peers for: {:?}", peer_id);
                if let Some(kad) = self.swarm.behaviour_mut().kad.as_mut() {
                    match self.find_on_dht_queries.entry(peer_id.to_bytes()) {
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            let (_, channels) = entry.get_mut();
                            channels.push(response_channel);
                        }
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            kad.get_closest_peers(peer_id);
                            entry.insert((peer_id, vec![response_channel]));
                        }
                    }
                } else {
                    tokio::task::spawn(async move {
                        response_channel
                            .send(Err(anyhow!("kademlia is not available")))
                            .ok();
                    });
                }
            }
            RpcMessage::Shutdown => {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

#[derive(Debug)]
enum SwarmEventResult {
    KademliaAddressAdded,
    KademliaBoostrapSuccess,
}

pub async fn load_identity<S: Storage>(kc: &mut Keychain<S>) -> Result<Keypair> {
    if kc.is_empty().await? {
        info!("no identity found, creating",);
        kc.create_ed25519_key().await?;
    }

    // for now we just use the first key
    let first_key = kc.keys().next().await;
    if let Some(keypair) = first_key {
        let keypair: Keypair = keypair?.into();
        info!("identity loaded: {}", PeerId::from(keypair.public()));
        return Ok(keypair);
    }

    Err(anyhow!("inconsistent key state"))
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use crate::keys::Keypair;

    use async_trait::async_trait;
    use ceramic_core::RangeOpen;
    use ceramic_store::{SqliteEventStore, SqlitePool};
    use futures::TryStreamExt;
    use rand::prelude::*;
    use rand_chacha::ChaCha8Rng;
    use recon::{Range, Result as ReconResult, Sha256a, SyncState};
    use ssh_key::private::Ed25519Keypair;

    use libp2p::{identity::Keypair as Libp2pKeypair, kad::RecordKey};

    use super::*;
    use anyhow::Result;
    use iroh_rpc_client::P2pClient;
    use iroh_rpc_types::{p2p::P2pAddr, Addr};
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    #[tokio::test]
    #[ignore]
    async fn test_fetch_providers_grpc_dht() -> Result<()> {
        let server_addr = "irpc://0.0.0.0:4401".parse().unwrap();
        let client_addr = "irpc://0.0.0.0:4401".parse().unwrap();
        fetch_providers(
            "/ip4/0.0.0.0/tcp/5001".parse().unwrap(),
            server_addr,
            client_addr,
        )
        .await
        .unwrap();
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_fetch_providers_mem_dht() -> Result<()> {
        tracing_subscriber::registry()
            .with(fmt::layer().pretty())
            .with(EnvFilter::from_default_env())
            .init();

        let client_addr = Addr::new_mem();
        let server_addr = client_addr.clone();
        fetch_providers(
            "/ip4/0.0.0.0/tcp/5003".parse().unwrap(),
            server_addr,
            client_addr,
        )
        .await?;
        Ok(())
    }

    #[derive(Debug)]
    struct TestRunnerBuilder {
        /// An Optional listening address for this node
        /// When `None`, the swarm will connect to a random tcp port.
        addrs: Option<Vec<Multiaddr>>,
        /// The listening addresses for the p2p client.
        /// When `None`, the client will communicate over a memory rpc channel
        rpc_addrs: Option<(P2pAddr, P2pAddr)>,
        /// When `true`, allow bootstrapping to the network.
        /// Otherwise, don't provide any addresses from which to bootstrap.
        bootstrap: bool,
        /// An optional seed to use when building a peer_id.
        /// When `None`, it will use a previously derived peer_id `12D3KooWFma2D63TG9ToSiRsjFkoNm2tTihScTBAEdXxinYk5rwE`. // cspell:disable-line
        seed: Option<ChaCha8Rng>,
        /// Optional `Keys` the node should provide to the DHT on start up.
        keys: Option<Vec<RecordKey>>,
        /// Pass through to node.trust_observed_addrs
        trust_observed_addrs: bool,
    }

    #[derive(Clone)]
    struct DummyRecon<K>(PhantomData<K>);

    #[async_trait]
    impl<K> Recon for DummyRecon<K>
    where
        K: recon::Key
            + std::fmt::Debug
            + serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + Send
            + Sync
            + 'static,
    {
        type Key = K;
        type Hash = Sha256a;

        async fn insert(&self, _key: Self::Key, _value: Option<Vec<u8>>) -> ReconResult<()> {
            unreachable!()
        }

        async fn range(
            &self,
            _left_fencepost: Self::Key,
            _right_fencepost: Self::Key,
            _offset: usize,
            _limit: usize,
        ) -> ReconResult<Vec<Self::Key>> {
            unreachable!()
        }

        async fn len(&self) -> ReconResult<usize> {
            unreachable!()
        }

        async fn value_for_key(&self, _key: Self::Key) -> ReconResult<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn keys_with_missing_values(
            &self,
            _range: RangeOpen<Self::Key>,
        ) -> ReconResult<Vec<Self::Key>> {
            unreachable!()
        }
        async fn interests(&self) -> ReconResult<Vec<RangeOpen<Self::Key>>> {
            unreachable!()
        }

        async fn process_interests(
            &self,
            _interests: Vec<RangeOpen<Self::Key>>,
        ) -> ReconResult<Vec<RangeOpen<Self::Key>>> {
            unreachable!()
        }

        async fn initial_range(
            &self,
            _interest: RangeOpen<Self::Key>,
        ) -> ReconResult<Range<Self::Key, Self::Hash>> {
            unreachable!()
        }

        async fn process_range(
            &self,
            _range: Range<Self::Key, Self::Hash>,
        ) -> ReconResult<(SyncState<Self::Key, Self::Hash>, Vec<Self::Key>)> {
            unreachable!()
        }

        fn metrics(&self) -> recon::Metrics {
            unreachable!()
        }
    }

    impl TestRunnerBuilder {
        fn new() -> Self {
            Self {
                addrs: None,
                rpc_addrs: None,
                bootstrap: true,
                seed: None,
                keys: None,
                trust_observed_addrs: false,
            }
        }

        fn with_addrs(mut self, addrs: Vec<Multiaddr>) -> Self {
            self.addrs = Some(addrs);
            self
        }

        fn with_rpc_addrs(mut self, rpc_server_addr: P2pAddr, rpc_client_addr: P2pAddr) -> Self {
            self.rpc_addrs = Some((rpc_server_addr, rpc_client_addr));
            self
        }

        fn no_bootstrap(mut self) -> Self {
            self.bootstrap = false;
            self
        }

        fn with_seed(mut self, seed: ChaCha8Rng) -> Self {
            self.seed = Some(seed);
            self
        }
        fn with_trust_observed_addrs(mut self, trust_observed_addrs: bool) -> Self {
            self.trust_observed_addrs = trust_observed_addrs;
            self
        }

        async fn build(self) -> Result<TestRunner> {
            let (rpc_server_addr, rpc_client_addr) = match self.rpc_addrs {
                Some((rpc_server_addr, rpc_client_addr)) => (rpc_server_addr, rpc_client_addr),
                None => {
                    let x = Addr::new_mem();
                    (x.clone(), x)
                }
            };
            let mut network_config = Config::default_with_rpc(rpc_client_addr.clone());
            network_config.libp2p.trust_observed_addrs = self.trust_observed_addrs;

            if let Some(addrs) = self.addrs {
                network_config.libp2p.listening_multiaddrs = addrs;
            } else {
                network_config.libp2p.listening_multiaddrs =
                    vec!["/ip4/0.0.0.0/tcp/0".parse().unwrap()];
            }

            if !self.bootstrap {
                network_config.libp2p.ceramic_peers = vec![];
            }
            let keypair = if let Some(seed) = self.seed {
                Ed25519Keypair::random(seed)
            } else {
                // public key 12D3KooWFma2D63TG9ToSiRsjFkoNm2tTihScTBAEdXxinYk5rwE
                Ed25519Keypair::from_bytes(&[
                    76, 8, 66, 244, 198, 186, 191, 7, 108, 12, 45, 193, 111, 155, 197, 0, 2, 9, 43,
                    174, 135, 222, 200, 126, 94, 73, 205, 84, 201, 4, 70, 60, 88, 110, 199, 251,
                    116, 51, 223, 7, 47, 24, 92, 233, 253, 5, 82, 72, 156, 214, 211, 143, 182, 206,
                    76, 207, 121, 235, 48, 31, 50, 60, 219, 157,
                ])
                .unwrap()
            };
            let keypair = Keypair::Ed25519(keypair);
            let libp2p_keypair: Libp2pKeypair = keypair.clone().into();
            let peer_id = PeerId::from(libp2p_keypair.public());

            // Using an in memory DB for the tests for realistic benchmark disk DB is needed.
            let sql_pool = SqlitePool::connect_in_memory().await.unwrap();

            let metrics = Metrics::register(&mut prometheus_client::registry::Registry::default());
            let mut p2p = Node::new(
                network_config,
                rpc_server_addr,
                keypair.into(),
                None::<(DummyRecon<Interest>, DummyRecon<EventId>)>,
                Arc::new(SqliteEventStore::new(sql_pool).await?),
                metrics,
            )
            .await?;
            let cfg = iroh_rpc_client::Config {
                p2p_addr: Some(rpc_client_addr),
                channels: Some(1),
                ..Default::default()
            };

            if let Some(keys) = self.keys {
                if let Some(kad) = p2p.swarm.behaviour_mut().kad.as_mut() {
                    for k in keys {
                        kad.start_providing(k)?;
                    }
                } else {
                    anyhow::bail!("expected kad behaviour to exist");
                }
            }

            let client = RpcClient::new(cfg).await?;

            let network_events = p2p.network_events();
            let task = tokio::task::spawn(async move { p2p.run().await.unwrap() });

            let client = client.try_p2p()?;

            let addr =
                tokio::time::timeout(Duration::from_millis(500), get_addr_loop(client.clone()))
                    .await
                    .context("timed out before getting a listening address for the node")??;
            let mut dial_addr = addr.clone();
            dial_addr.push(Protocol::P2p(peer_id));
            Ok(TestRunner {
                task,
                client,
                peer_id,
                network_events,
                addr,
                dial_addr,
            })
        }
    }

    async fn get_addr_loop(client: P2pClient) -> Result<Multiaddr> {
        loop {
            let l = client.listeners().await?;
            if let Some(a) = l.first() {
                return Ok(a.clone());
            }
        }
    }

    struct TestRunner {
        /// The task that runs the p2p node.
        task: JoinHandle<()>,
        /// The RPC client
        /// Used to communicate with the p2p node.
        client: P2pClient,
        /// The node's peer_id
        peer_id: PeerId,
        /// A channel to read the network events received by the node.
        network_events: Receiver<NetworkEvent>,
        /// The listening address for this node.
        addr: Multiaddr,
        /// A multiaddr that is a combination of the listening addr and peer_id.
        /// This address can be used by another node to dial this peer directly.
        dial_addr: Multiaddr,
    }

    impl Drop for TestRunner {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn fetch_providers(
        addr: Multiaddr,
        rpc_server_addr: P2pAddr,
        rpc_client_addr: P2pAddr,
    ) -> Result<()> {
        let test_runner = TestRunnerBuilder::new()
            .with_addrs(vec![addr])
            .with_rpc_addrs(rpc_server_addr, rpc_client_addr)
            .build()
            .await?;

        {
            // Make sure we are bootstrapped.
            tokio::time::sleep(Duration::from_millis(2500)).await;
            let c = "QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR" // cspell:disable-line
                .parse()
                .unwrap();

            let mut providers = Vec::new();
            let mut chan = test_runner.client.fetch_providers_dht(&c).await?;
            while let Some(new_providers) = chan.next().await {
                let new_providers = new_providers.unwrap();
                println!("providers found: {}", new_providers.len());
                assert!(!new_providers.is_empty());

                for p in &new_providers {
                    println!("{p}");
                    providers.push(*p);
                }
            }

            println!("{providers:?}");
            assert!(!providers.is_empty());
            assert!(
                providers.len() >= DEFAULT_PROVIDER_LIMIT,
                "{} < {}",
                providers.len(),
                DEFAULT_PROVIDER_LIMIT
            );
        };

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_local_peer_id() -> Result<()> {
        let test_runner = TestRunnerBuilder::new().no_bootstrap().build().await?;
        let got_peer_id = test_runner.client.local_peer_id().await?;
        let expect_peer_id: PeerId = "12D3KooWFma2D63TG9ToSiRsjFkoNm2tTihScTBAEdXxinYk5rwE"
            .parse()
            .unwrap();
        assert_eq!(expect_peer_id, got_peer_id);
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_two_nodes() -> Result<()> {
        let test_runner_a = TestRunnerBuilder::new().no_bootstrap().build().await?;
        // peer_id 12D3KooWLo6JTNKXfjkZtKf8ooLQoXVXUEeuu4YDY3CYqK6rxHXt
        let test_runner_b = TestRunnerBuilder::new()
            .no_bootstrap()
            .with_seed(ChaCha8Rng::from_seed([0; 32]))
            .build()
            .await?;
        let addrs_b = vec![test_runner_b.addr.clone()];

        let peer_id_a = test_runner_a.client.local_peer_id().await?;
        assert_eq!(test_runner_a.peer_id, peer_id_a);
        let peer_id_b = test_runner_b.client.local_peer_id().await?;
        assert_eq!(test_runner_b.peer_id, peer_id_b);

        let lookup_a = test_runner_a.client.lookup_local().await?;
        // since we aren't connected to any other nodes, we should not
        // have any information about our observed addresses
        assert!(lookup_a.observed_addrs.is_empty());
        assert_lookup(lookup_a, test_runner_a.peer_id, &test_runner_a.addr, &[])?;

        // connect
        test_runner_a.client.connect(peer_id_b, addrs_b).await?;

        // Make sure the peers have had time to negotiate protocols
        tokio::time::sleep(Duration::from_millis(2500)).await;

        // Make sure we have exchanged identity information
        // peer b should be in the list of peers that peer a is connected to
        let peers = test_runner_a.client.get_peers().await?;
        assert!(peers.len() == 1);
        let got_peer_addrs = peers.get(&peer_id_b).unwrap();
        assert!(got_peer_addrs.contains(&test_runner_b.dial_addr));

        // lookup
        let lookup_b = test_runner_a.client.lookup(peer_id_b, None).await?;
        // Expected protocols are only the ones negotiated with a connected peer.
        // NOTE: dcutr is not in the list because it is not negotiated with the peer.
        let expected_protocols = [
            "/ipfs/ping/1.0.0",
            "/ipfs/id/1.0.0",
            "/ipfs/id/push/1.0.0",
            "/ipfs/bitswap/1.2.0",
            "/ipfs/bitswap/1.1.0",
            "/ipfs/bitswap/1.0.0",
            "/ipfs/bitswap",
            "/ipfs/kad/1.0.0",
            "/libp2p/autonat/1.0.0",
            "/libp2p/circuit/relay/0.2.0/hop",
            "/libp2p/circuit/relay/0.2.0/stop",
            "/meshsub/1.1.0",
            "/meshsub/1.0.0",
        ];
        assert_lookup(
            lookup_b,
            test_runner_b.peer_id,
            &test_runner_b.addr,
            &expected_protocols[..],
        )?;
        // now that we are connected & have exchanged identity information,
        // we should now be able to view the node's external addrs
        // these are the addresses that other nodes tell you "this is the address I see for you"
        let external_addrs_a = test_runner_a.client.external_addresses().await?;
        assert_eq!(vec![test_runner_a.addr.clone()], external_addrs_a);

        // peer_disconnect NOT YET IMPLEMENTED
        // test_runner_a.client.disconnect(peer_id_b).await?;
        // let peers = test_runner_a.client.get_peers().await?;
        // assert!(peers.len() == 0);

        Ok(())
    }

    // assert_lookup ensures each part of the lookup is equal
    fn assert_lookup(
        got: Lookup,
        expected_peer_id: PeerId,
        expected_addr: &Multiaddr,
        expected_protocols: &[&str],
    ) -> Result<()> {
        let expected_protocol_version = "ipfs/0.1.0";
        let expected_agent_version =
            format!("iroh/{}", std::env::var("CARGO_PKG_VERSION").unwrap());

        assert_eq!(expected_peer_id, got.peer_id);
        assert!(got.listen_addrs.contains(expected_addr));
        assert_eq!(expected_protocols, got.protocols);
        assert_eq!(expected_protocol_version, got.protocol_version);
        assert_eq!(expected_agent_version, got.agent_version);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cancel_listen_for_identify() -> Result<()> {
        let mut test_runner_a = TestRunnerBuilder::new().no_bootstrap().build().await?;
        let peer_id: PeerId = "12D3KooWFma2D63TG9ToSiRsjFkoNm2tTihScTBAEdXxinYk5rwE"
            .parse()
            .unwrap();
        test_runner_a
            .client
            .lookup(peer_id, None)
            .await
            .unwrap_err();
        // when lookup ends in error, we must ensure we
        // have canceled the lookup
        let event = test_runner_a.network_events.recv().await.unwrap();
        if let NetworkEvent::CancelLookupQuery(got_peer_id) = event {
            assert_eq!(peer_id, got_peer_id);
        } else {
            anyhow::bail!("unexpected NetworkEvent {:#?}", event);
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[cfg_attr(target_os = "macos", ignore = "on MacOS")]
    async fn test_dht() -> Result<()> {
        // set up three nodes
        // two connect to one
        // try to connect via id
        let cid: Cid = "bafkreieq5jui4j25lacwomsqgjeswwl3y5zcdrresptwgmfylxo2depppq" // cspell:disable-line
            .parse()
            .unwrap();

        let test_runner_a = TestRunnerBuilder::new()
            .no_bootstrap()
            // We can trust all peers as they are the other test runners.
            //
            // We need to trust the observed_addrs because otherwise kademlia will not switch into server mode for the
            // established connections because there is no external address to be used.
            .with_trust_observed_addrs(true)
            .build()
            .await?;
        println!("peer_a: {:?}", test_runner_a.peer_id);

        // peer_id 12D3KooWLo6JTNKXfjkZtKf8ooLQoXVXUEeuu4YDY3CYqK6rxHXt
        let mut test_runner_b = TestRunnerBuilder::new()
            .no_bootstrap()
            .with_trust_observed_addrs(true)
            .with_seed(ChaCha8Rng::from_seed([0; 32]))
            .build()
            .await?;
        let addrs = vec![test_runner_b.addr.clone()];

        println!("peer_b: {:?}", test_runner_b.peer_id);

        let test_runner_c = TestRunnerBuilder::new()
            .no_bootstrap()
            .with_trust_observed_addrs(true)
            .with_seed(ChaCha8Rng::from_seed([1; 32]))
            .build()
            .await?;

        println!("peer_c: {:?}", test_runner_c.peer_id);

        // connect a and c to b
        test_runner_a
            .client
            .connect(test_runner_b.peer_id, addrs.clone())
            .await?;

        // expect a network event showing a & b have connected
        match test_runner_b.network_events.recv().await {
            Some(NetworkEvent::PeerConnected(peer_id)) => {
                assert_eq!(test_runner_a.peer_id, peer_id);
            }
            Some(n) => {
                anyhow::bail!("unexpected network event: {:?}", n);
            }
            None => {
                anyhow::bail!("expected NetworkEvent::PeerConnected, received no event");
            }
        };

        test_runner_c
            .client
            .connect(test_runner_b.peer_id, addrs.clone())
            .await?;

        // expect a network event showing b & c have connected
        match test_runner_b.network_events.recv().await {
            Some(NetworkEvent::PeerConnected(peer_id)) => {
                assert_eq!(test_runner_c.peer_id, peer_id);
            }
            Some(n) => {
                anyhow::bail!("unexpected network event: {:?}", n);
            }
            None => {
                anyhow::bail!("expected NetworkEvent::PeerConnected, received no event");
            }
        };

        // c start providing
        test_runner_c.client.start_providing(&cid).await?;

        // when `start_providing` waits for the record to make it to the dht
        // we can remove this polling
        let providers = tokio::time::timeout(
            Duration::from_secs(6),
            poll_for_providers(test_runner_a.client.clone(), &cid),
        )
        .await
        .context("timed out before finding providers for the given cid")??;

        assert!(providers.len() == 1);
        assert!(providers.first().unwrap().contains(&test_runner_c.peer_id));

        // c stop providing
        test_runner_c.client.stop_providing(&cid).await?;

        // a fetch providers dht should not get any providers
        let stream = test_runner_a.client.fetch_providers_dht(&cid).await?;
        let providers: Vec<_> = stream.try_collect().await.unwrap();

        assert!(providers.is_empty());

        // try to connect a to c using only peer_id
        test_runner_a
            .client
            .connect(test_runner_c.peer_id, vec![])
            .await?;
        Ok(())
    }

    async fn poll_for_providers(client: P2pClient, cid: &Cid) -> Result<Vec<HashSet<PeerId>>> {
        loop {
            let stream = client.fetch_providers_dht(cid).await?;
            let providers: Vec<_> = stream.try_collect().await.unwrap();
            if providers.is_empty() {
                continue;
            }
            return Ok(providers);
        }
    }
}
