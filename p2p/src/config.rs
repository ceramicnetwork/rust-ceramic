use std::{
    num::{NonZeroU8, NonZeroUsize},
    time::Duration,
};

use iroh_rpc_client::Config as RpcClientConfig;
use iroh_rpc_types::p2p::P2pAddr;
use libp2p::Multiaddr;
use serde::{Deserialize, Serialize};

/// Configuration for the [`iroh-p2p`] node.
#[derive(PartialEq, Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// Configuration for libp2p.
    pub libp2p: Libp2pConfig,
    /// Configuration of RPC to other iroh services.
    pub rpc_client: RpcClientConfig,
}

impl Config {
    pub fn default_with_rpc(client_addr: P2pAddr) -> Self {
        Self {
            libp2p: Libp2pConfig::default(),
            rpc_client: RpcClientConfig {
                p2p_addr: Some(client_addr),
                ..Default::default()
            },
        }
    }
}

/// Libp2p config for the node.
#[derive(PartialEq, Eq, Debug, Clone, Deserialize, Serialize)]
#[non_exhaustive]
pub struct Libp2pConfig {
    /// External address.
    pub external_multiaddrs: Vec<Multiaddr>,
    /// Local address.
    pub listening_multiaddrs: Vec<Multiaddr>,
    /// Bootstrap peer list.
    pub bootstrap_peers: Vec<Multiaddr>,
    /// Mdns discovery enabled.
    pub mdns: bool,
    /// Bitswap server mode enabled.
    pub bitswap_server: bool,
    /// Bitswap client mode enabled.
    pub bitswap_client: bool,
    /// Kademlia discovery enabled.
    pub kademlia: bool,
    /// Autonat holepunching enabled.
    pub autonat: bool,
    /// Relay server enabled.
    pub relay_server: bool,
    /// Relay client enabled.
    pub relay_client: bool,
    /// Gossipsub enabled.
    pub gossipsub: bool,
    pub max_conns_out: u32,
    pub max_conns_in: u32,
    pub max_conns_pending_out: u32,
    pub max_conns_pending_in: u32,
    pub max_conns_per_peer: u32,
    pub notify_handler_buffer_size: NonZeroUsize,
    pub connection_event_buffer_size: usize,
    pub dial_concurrency_factor: NonZeroU8,
    pub idle_connection_timeout: Duration,
    /// Trust as a confirmed external address any reported observed address.
    ///
    /// NOTE: It is generally not safe to trust observed addresses received from arbitrary peers.
    /// Only enable this option if its known that all connecting peers can be trusted.
    pub trust_observed_addrs: bool,
}

impl Default for Libp2pConfig {
    fn default() -> Self {
        Self {
            external_multiaddrs: vec![],
            listening_multiaddrs: vec![
                "/ip4/0.0.0.0/tcp/4444".parse().unwrap(),
                "/ip4/0.0.0.0/udp/4445/quic-v1".parse().unwrap(),
            ],
            bootstrap_peers: vec![],
            mdns: false,
            kademlia: true,
            autonat: true,
            relay_server: true,
            relay_client: true,
            gossipsub: true,
            bitswap_client: true,
            bitswap_server: true,
            max_conns_pending_out: 256,
            max_conns_pending_in: 256,
            max_conns_in: 256,
            max_conns_out: 512,
            max_conns_per_peer: 8,
            notify_handler_buffer_size: NonZeroUsize::new(256).expect("should not be zero"),
            connection_event_buffer_size: 256,
            dial_concurrency_factor: NonZeroU8::new(8).expect("should not be zero"),
            idle_connection_timeout: Duration::from_secs(30),
            trust_observed_addrs: false,
        }
    }
}
