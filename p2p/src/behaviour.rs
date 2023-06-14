use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use cid::Cid;
use iroh_bitswap::{Bitswap, Block, Config as BitswapConfig, Store};
use iroh_rpc_client::Client;
use libp2p::connection_limits::{self, ConnectionLimits};
use libp2p::core::identity::Keypair;
use libp2p::gossipsub::{self, MessageAuthenticity};
use libp2p::identify;
use libp2p::kad::store::{MemoryStore, MemoryStoreConfig};
use libp2p::kad::{Kademlia, KademliaConfig};
use libp2p::mdns::tokio::Behaviour as Mdns;
use libp2p::multiaddr::Protocol;
use libp2p::ping::Behaviour as Ping;
use libp2p::relay;
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{autonat, dcutr};
use libp2p_identity::PeerId;
use tracing::{info, warn};

use recon::libp2p::Recon;

pub use self::event::Event;
use self::peer_manager::PeerManager;
use crate::config::Libp2pConfig;

mod event;
mod peer_manager;

pub const PROTOCOL_VERSION: &str = "ipfs/0.1.0";
pub const AGENT_VERSION: &str = concat!("iroh/", env!("CARGO_PKG_VERSION"));

/// Libp2p behaviour for the node.
#[derive(NetworkBehaviour)]
#[behaviour(out_event = "Event")]
pub(crate) struct NodeBehaviour<R> {
    ping: Ping,
    identify: identify::Behaviour,
    pub(crate) bitswap: Toggle<Bitswap<BitswapStore>>,
    pub(crate) kad: Toggle<Kademlia<MemoryStore>>,
    mdns: Toggle<Mdns>,
    pub(crate) autonat: Toggle<autonat::Behaviour>,
    relay: Toggle<relay::Behaviour>,
    relay_client: Toggle<relay::client::Behaviour>,
    dcutr: Toggle<dcutr::Behaviour>,
    pub(crate) gossipsub: Toggle<gossipsub::Behaviour>,
    pub(crate) peer_manager: PeerManager,
    limits: connection_limits::Behaviour,
    recon: Toggle<recon::libp2p::Behaviour<R>>,
}

#[derive(Debug, Clone)]
pub(crate) struct BitswapStore(Client);

#[async_trait]
impl Store for BitswapStore {
    async fn get(&self, cid: &Cid) -> Result<Block> {
        let store = self.0.try_store()?;
        let cid = *cid;
        let data = store
            .get(cid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("not found"))?;
        Ok(Block::new(data, cid))
    }

    async fn get_size(&self, cid: &Cid) -> Result<usize> {
        let store = self.0.try_store()?;
        let cid = *cid;
        let size = store
            .get_size(cid)
            .await?
            .ok_or_else(|| anyhow::anyhow!("not found"))?;
        Ok(size as usize)
    }

    async fn has(&self, cid: &Cid) -> Result<bool> {
        let store = self.0.try_store()?;
        let cid = *cid;
        let res = store.has(cid).await?;
        Ok(res)
    }
}

impl<R: Recon> NodeBehaviour<R> {
    pub async fn new(
        local_key: &Keypair,
        config: &Libp2pConfig,
        relay_client: Option<relay::client::Behaviour>,
        rpc_client: Client,
        recon: Option<R>,
    ) -> Result<Self> {
        let peer_manager = PeerManager::default();
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
            Some(Bitswap::new(peer_id, BitswapStore(rpc_client), bs_config).await)
        } else {
            None
        }
        .into();

        let mdns = if config.mdns {
            info!("init mdns");
            Some(Mdns::new(Default::default(), peer_id.clone())?)
        } else {
            None
        }
        .into();

        let kad = if config.kademlia {
            info!("init kademlia");
            // TODO: persist to store
            let mem_store_config = MemoryStoreConfig {
                // enough for >10gb of unixfs files at the default chunk size
                max_records: 1024 * 64,
                max_provided_keys: 1024 * 64,
                ..Default::default()
            };
            let store = MemoryStore::with_config(peer_id, mem_store_config);

            // TODO: make user configurable
            let mut kad_config = KademliaConfig::default();
            kad_config.set_parallelism(16usize.try_into().unwrap());
            // TODO: potentially lower (this is per query)
            kad_config.set_query_timeout(Duration::from_secs(60));

            let mut kademlia = Kademlia::with_config(pub_key.to_peer_id(), store, kad_config);
            for multiaddr in &config.bootstrap_peers {
                // TODO: move parsing into config
                let mut addr = multiaddr.to_owned();
                if let Some(Protocol::P2p(mh)) = addr.pop() {
                    let peer_id = PeerId::from_multihash(mh).unwrap();
                    kademlia.add_address(&peer_id, addr);
                } else {
                    warn!("Could not parse bootstrap addr {}", multiaddr);
                }
            }

            // Trigger initial bootstrap
            if let Err(e) = kademlia.bootstrap() {
                warn!("Kademlia bootstrap failed: {}", e);
            }

            Some(kademlia)
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
            let dcutr = dcutr::Behaviour::new(peer_id.clone());
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

        let gossipsub = if config.gossipsub {
            info!("init gossipsub");
            let gossipsub_config = gossipsub::Config::default();
            let message_authenticity = MessageAuthenticity::Signed(local_key.clone());
            Some(
                gossipsub::Behaviour::new(message_authenticity, gossipsub_config)
                    .map_err(|e| anyhow::anyhow!("{}", e))?,
            )
        } else {
            None
        }
        .into();

        let limits = connection_limits::Behaviour::new(
            ConnectionLimits::default()
                .with_max_established_outgoing(Some(config.max_conns_out))
                .with_max_established_incoming(Some(config.max_conns_in))
                .with_max_pending_outgoing(Some(config.max_conns_pending_out))
                .with_max_pending_incoming(Some(config.max_conns_pending_in))
                .with_max_established_per_peer(Some(config.max_conns_per_peer)),
        );
        let recon =
            recon.map(|r| recon::libp2p::Behaviour::new(r, recon::libp2p::Config::default()));
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
            gossipsub,
            peer_manager,
            limits,
            recon: recon.into(),
        })
    }

    pub fn notify_new_blocks(&self, blocks: Vec<Block>) {
        if let Some(bs) = self.bitswap.as_ref() {
            let client = bs.client().clone();
            tokio::task::spawn(async move {
                if let Err(err) = client.notify_new_blocks(&blocks).await {
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
    use libp2p::swarm::dummy;

    use super::*;

    fn assert_send<T: Send>() {}

    #[test]
    fn test_traits() {
        assert_send::<Bitswap<BitswapStore>>();
        assert_send::<NodeBehaviour<Toggle<dummy::Behaviour>>>();
        assert_send::<&Bitswap<BitswapStore>>();
    }
}
