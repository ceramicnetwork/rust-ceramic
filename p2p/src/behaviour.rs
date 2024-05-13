use std::time::Duration;

use anyhow::Result;
use ceramic_core::{EventId, Interest};
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
use crate::config::Libp2pConfig;
use crate::Metrics;

mod ceramic_peer_manager;
mod event;

pub const PROTOCOL_VERSION: &str = "ipfs/0.1.0";
pub const AGENT_VERSION: &str = concat!("ceramic-one/", env!("CARGO_PKG_VERSION"));

/// Libp2p behaviour for the node.
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "Event")]
pub(crate) struct NodeBehaviour<I, M, S>
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
    recon: Toggle<recon::libp2p::Behaviour<I, M>>,
}

impl<I, M, S> NodeBehaviour<I, M, S>
where
    I: Recon<Key = Interest, Hash = Sha256a> + Send + Sync,
    M: Recon<Key = EventId, Hash = Sha256a> + Send + Sync,
    S: iroh_bitswap::Store + Send + Sync,
{
    pub async fn new(
        local_key: &Keypair,
        config: &Libp2pConfig,
        relay_client: Option<relay::client::Behaviour>,
        recons: Option<(I, M)>,
        block_store: Arc<S>,
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
        let recon = recons.map(|(interest, model)| {
            recon::libp2p::Behaviour::new(interest, model, recon::libp2p::Config::default())
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
            peer_manager: CeramicPeerManager::new(&config.ceramic_peers, metrics)?,
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

#[cfg(test)]
mod tests {
    use super::*;
    use cid::Cid;
    use libp2p::swarm::{ConnectionId, ToSwarm};
    use mockall::mock;
    use prometheus_client::registry::Registry;
    use recon::test_utils::{
        InjectedEvent, MockReconForEventId, MockReconForInterest, TestBehaviour, TestSwarm,
    };

    fn convert_behaviour_event(ev: Event) -> Option<recon::libp2p::Event> {
        match ev {
            Event::Ping(_ev) => None,
            Event::Identify(_ev) => None,
            Event::Kademlia(_ev) => None,
            Event::Mdns(_ev) => None,
            Event::Bitswap(_ev) => None,
            Event::Autonat(_ev) => None,
            Event::Relay(_ev) => None,
            Event::RelayClient(_ev) => None,
            Event::Dcutr(_ev) => None,
            Event::PeerManager(_ev) => None,
            Event::Recon(ev) => Some(ev),
            Event::Void => None,
        }
    }

    mock! {
        BitswapStore {}

        #[async_trait::async_trait]
        impl iroh_bitswap::Store for BitswapStore {
            async fn get_size(&self, cid: &Cid) -> Result<usize>;
            async fn get(&self, cid: &Cid) -> Result<Block>;
            async fn has(&self, cid: &Cid) -> Result<bool>;
            async fn put(&self, block: &Block) -> Result<bool>;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn behavior_runs() {
        let mut cfg = Libp2pConfig::default();
        cfg.autonat = false;
        cfg.bitswap_server = false;
        cfg.bitswap_client = false;
        cfg.mdns = false;
        cfg.relay_client = false;
        cfg.relay_server = false;
        let behavior = NodeBehaviour::new(
            &Keypair::generate_ed25519(),
            &cfg,
            None,
            Some((
                MockReconForInterest::default(),
                MockReconForEventId::default(),
            )),
            Arc::new(MockBitswapStore::default()),
            Metrics::register(&mut Registry::default()),
        )
        .await
        .unwrap();
        let swarm = TestSwarm::from_behaviour(TestBehaviour {
            inner: behavior,
            convert: Box::new(|_, ev| {
                if let ToSwarm::GenerateEvent(ev) = ev {
                    if let Some(ev) = convert_behaviour_event(ev) {
                        Some(ToSwarm::GenerateEvent(ev))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }),
            api: Box::new(|_, _| ()),
        });
        let driver = swarm.drive();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let stats = driver.stop().await;
        assert!(stats.polled >= 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn behavior_polled_when_using_public_api() {
        let mut mock_interest = MockReconForInterest::default();
        mock_interest
            .expect_clone()
            .returning(|| MockReconForInterest::default());
        let mut mock_event = MockReconForEventId::default();
        mock_event
            .expect_clone()
            .returning(|| MockReconForEventId::default());
        let mut cfg = Libp2pConfig::default();
        cfg.autonat = false;
        cfg.bitswap_server = false;
        cfg.bitswap_client = false;
        cfg.mdns = false;
        cfg.relay_client = false;
        cfg.relay_server = false;
        let behavior = NodeBehaviour::new(
            &Keypair::generate_ed25519(),
            &cfg,
            None,
            Some((mock_interest, mock_event)),
            Arc::new(MockBitswapStore::default()),
            Metrics::register(&mut Registry::default()),
        )
        .await
        .unwrap();
        let swarm = TestSwarm::from_behaviour(TestBehaviour {
            inner: behavior,
            convert: Box::new(|_, ev| {
                if let ToSwarm::GenerateEvent(ev) = ev {
                    if let Some(ev) = convert_behaviour_event(ev) {
                        Some(ToSwarm::GenerateEvent(ev))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }),
            api: Box::new(|beh, _| {
                beh.handle_established_inbound_connection(
                    ConnectionId::new_unchecked(0),
                    recon::test_utils::PEER_ID.parse().unwrap(),
                    &"/ip4/1.2.3.4/tcp/443".parse().unwrap(),
                    &"/ip4/1.2.3.4/tcp/443".parse().unwrap(),
                )
                .unwrap();
            }),
        });
        let driver = swarm.drive();

        tokio::time::sleep(Duration::from_secs(1)).await;

        driver
            .inject(InjectedEvent::Api("connect".to_string()))
            .await;

        tokio::time::sleep(Duration::from_secs(1)).await;

        let stats = driver.stop().await;
        assert!(stats.polled >= 2);
    }
}
