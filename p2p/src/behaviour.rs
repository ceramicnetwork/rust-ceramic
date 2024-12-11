use std::time::Duration;

use anyhow::Result;
use ceramic_core::{EventId, PeerKey};
use iroh_bitswap::{Bitswap, Block, Config as BitswapConfig};
use libp2p::{
    autonat,
    connection_limits::{self, ConnectionLimits},
    dcutr, identify,
    kad::{
        self,
        store::{MemoryStore, MemoryStoreConfig},
    },
    mdns::tokio::Behaviour as Mdns,
    ping::Behaviour as Ping,
    relay,
    swarm::behaviour::toggle::Toggle,
    swarm::NetworkBehaviour,
};
use libp2p_identity::Keypair;
use recon::{libp2p::Recon, Sha256a};
use std::sync::Arc;
use tracing::{info, warn};

use self::ceramic_peer_manager::CeramicPeerManager;
pub use self::event::Event;
use crate::Metrics;
use crate::{config::Libp2pConfig, peers};

mod ceramic_peer_manager;
mod event;

pub const PROTOCOL_VERSION: &str = "ipfs/0.1.0";
pub const AGENT_VERSION: &str = concat!("ceramic-one/", env!("CARGO_PKG_VERSION"));

/// Libp2p behaviour for the node.
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "Event")]
pub(crate) struct NodeBehaviour<P, M, S>
where
    S: iroh_bitswap::Store + Send + Sync,
{
    // Place limits first in the behaviour tree.
    // Behaviours are called in order and the limits behaviour can deny connections etc.
    // It keeps things simpler in other behaviours if they are never called for connections that
    // end up being denied because of the limits.
    // See https://github.com/libp2p/rust-libp2p/pull/4777#discussion_r1391833734 for more context.
    limits: connection_limits::Behaviour,
    pub(crate) peer_manager: CeramicPeerManager,
    ping: Ping,
    pub(crate) identify: identify::Behaviour,
    pub(crate) bitswap: Toggle<Bitswap<S>>,
    pub(crate) kad: Toggle<kad::Behaviour<MemoryStore>>,
    mdns: Toggle<Mdns>,
    pub(crate) autonat: Toggle<autonat::Behaviour>,
    relay: Toggle<relay::Behaviour>,
    relay_client: Toggle<relay::client::Behaviour>,
    dcutr: Toggle<dcutr::Behaviour>,
    recon: Toggle<recon::libp2p::Behaviour<P, M>>,
}

impl<P, M, S> NodeBehaviour<P, M, S>
where
    P: Recon<Key = PeerKey, Hash = Sha256a> + Send + Sync,
    M: Recon<Key = EventId, Hash = Sha256a> + Send + Sync,
    S: iroh_bitswap::Store + Send + Sync,
{
    pub async fn new(
        local_key: &Keypair,
        config: &Libp2pConfig,
        relay_client: Option<relay::client::Behaviour>,
        recons: Option<(P, M)>,
        block_store: Arc<S>,
        peers_tx: tokio::sync::mpsc::Sender<peers::Message>,
        metrics: Metrics,
    ) -> Result<Self> {
        let pub_key = local_key.public();
        let peer_id = pub_key.to_peer_id();

        let bitswap = if config.bitswap_client || config.bitswap_server {
            info!("init bitswap");
            // TODO(dig): server only mode is not implemented yet
            let bs_config = if config.bitswap_server {
                BitswapConfig::default()
            } else {
                BitswapConfig::default_client_mode()
            };
            Some(Bitswap::<S>::new(peer_id, block_store, bs_config).await)
        } else {
            None
        }
        .into();

        let mdns = if config.mdns {
            info!("init mdns");
            Some(Mdns::new(Default::default(), peer_id)?)
        } else {
            None
        }
        .into();

        let kad = if config.kademlia {
            info!("init kademlia");
            let mem_store_config = MemoryStoreConfig {
                // We do not store records.
                max_records: 0,
                max_provided_keys: 10_000_000,
                max_providers_per_key: config.kademlia_replication_factor.into(),
                ..Default::default()
            };
            let store = MemoryStore::with_config(peer_id, mem_store_config);

            let mut kad_config = kad::Config::default();
            kad_config
                .set_replication_factor(config.kademlia_replication_factor)
                .set_parallelism(config.kademlia_parallelism)
                .set_query_timeout(config.kademlia_query_timeout)
                .set_provider_record_ttl(config.kademlia_provider_record_ttl)
                // Disable record (re)-replication and (re)-publication
                .set_replication_interval(None)
                .set_publication_interval(None)
                // Disable provider record (re)-publication
                // Provider records are re-published via the [`crate::publisher::Publisher`].
                .set_provider_publication_interval(None);

            Some(kad::Behaviour::with_config(
                pub_key.to_peer_id(),
                store,
                kad_config,
            ))
        } else {
            None
        }
        .into();

        let autonat = if config.autonat {
            info!("init autonat");
            let pub_key = local_key.public();
            let config = autonat::Config {
                use_connected: true,
                boot_delay: Duration::from_secs(0),
                refresh_interval: Duration::from_secs(5),
                retry_interval: Duration::from_secs(5),
                ..Default::default()
            }; // TODO: configurable
            let autonat = autonat::Behaviour::new(pub_key.to_peer_id(), config);
            Some(autonat)
        } else {
            None
        }
        .into();

        let relay = if config.relay_server {
            info!("init relay server");
            let config = relay::Config::default();
            let r = relay::Behaviour::new(local_key.public().to_peer_id(), config);
            Some(r)
        } else {
            None
        }
        .into();

        let (dcutr, relay_client) = if config.relay_client {
            info!("init relay client");
            let relay_client =
                relay_client.expect("missing relay client even though it was enabled");
            let dcutr = dcutr::Behaviour::new(peer_id);
            (Some(dcutr), Some(relay_client))
        } else {
            (None, None)
        };

        let identify = {
            let config = identify::Config::new(PROTOCOL_VERSION.into(), local_key.public())
                .with_agent_version(String::from(AGENT_VERSION))
                .with_cache_size(64 * 1024);
            identify::Behaviour::new(config)
        };

        let limits = connection_limits::Behaviour::new(
            ConnectionLimits::default()
                .with_max_established_outgoing(Some(config.max_conns_out))
                .with_max_established_incoming(Some(config.max_conns_in))
                .with_max_pending_outgoing(Some(config.max_conns_pending_out))
                .with_max_pending_incoming(Some(config.max_conns_pending_in))
                .with_max_established_per_peer(Some(config.max_conns_per_peer)),
        );
        let recon = recons.map(|(peer, model)| {
            recon::libp2p::Behaviour::new(peer, model, recon::libp2p::Config::default())
        });
        Ok(NodeBehaviour {
            ping: Ping::default(),
            identify,
            bitswap,
            mdns,
            kad,
            autonat,
            relay,
            dcutr: dcutr.into(),
            relay_client: relay_client.into(),
            peer_manager: CeramicPeerManager::new(peers_tx, &config.ceramic_peers, metrics)?,
            limits,
            recon: recon.into(),
        })
    }

    pub fn notify_new_blocks(&self, blocks: Vec<Block>) {
        if let Some(bs) = self.bitswap.as_ref() {
            let client = bs.client().clone();
            let handle = tokio::runtime::Handle::current();
            tokio::task::block_in_place(move || {
                if let Err(err) = handle.block_on(client.notify_new_blocks(&blocks)) {
                    warn!("failed to notify bitswap about blocks: {:?}", err);
                }
            });
        }
    }

    pub fn kad_bootstrap(&mut self) -> Result<()> {
        if let Some(kad) = self.kad.as_mut() {
            kad.bootstrap()?;
        }
        Ok(())
    }
}
