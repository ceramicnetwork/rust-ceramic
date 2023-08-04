use std::time::Duration;

use anyhow::Result;
use ceramic_core::{EventId, Interest};
use futures::future::Either;
use libp2p::{
    core::{
        self,
        muxing::StreamMuxerBox,
        transport::{Boxed, OrTransport},
    },
    dns,
    identity::Keypair,
    noise,
    swarm::{Executor, SwarmBuilder},
    tcp, websocket,
    yamux::{self, WindowUpdateMode},
    PeerId, Swarm, Transport,
};
use recon::{libp2p::Recon, Sha256a};
use sqlx::SqlitePool;

use crate::{behaviour::NodeBehaviour, Libp2pConfig};

/// Builds the transport stack that LibP2P will communicate over.
async fn build_transport(
    keypair: &Keypair,
    config: &Libp2pConfig,
) -> (
    Boxed<(PeerId, StreamMuxerBox)>,
    Option<libp2p::relay::client::Behaviour>,
) {
    // TODO: make transports configurable

    let port_reuse = true;
    let connection_timeout = Duration::from_secs(30);

    // TCP
    let tcp_config = tcp::Config::default().port_reuse(port_reuse);
    let tcp_transport = tcp::tokio::Transport::new(tcp_config.clone());

    // Websockets
    let ws_tcp = websocket::WsConfig::new(tcp::tokio::Transport::new(tcp_config));
    let tcp_ws_transport = tcp_transport.or_transport(ws_tcp);

    // Quic
    let quic_config = libp2p_quic::Config::new(keypair);
    let quic_transport = libp2p_quic::tokio::Transport::new(quic_config);

    // Noise config for TCP & Websockets
    let auth_config =
        noise::Config::new(keypair).expect("should be able to configure noise with keypair");

    // Stream muxer config for TCP & Websockets
    let muxer_config = {
        let mut mplex_config = libp2p_mplex::MplexConfig::new();
        mplex_config.set_max_buffer_size(usize::MAX);

        let mut yamux_config = yamux::Config::default();
        yamux_config.set_max_buffer_size(16 * 1024 * 1024); // TODO: configurable
        yamux_config.set_receive_window_size(16 * 1024 * 1024); // TODO: configurable
        yamux_config.set_window_update_mode(WindowUpdateMode::on_receive());
        core::upgrade::SelectUpgrade::new(yamux_config, mplex_config)
    };

    // Enable Relay if enabled
    let (tcp_ws_transport, relay_client) = if config.relay_client {
        let (relay_transport, relay_client) =
            libp2p::relay::client::new(keypair.public().to_peer_id());

        let transport = OrTransport::new(relay_transport, tcp_ws_transport);
        let transport = transport
            .upgrade(core::upgrade::Version::V1Lazy)
            .authenticate(auth_config)
            .multiplex(muxer_config)
            .timeout(connection_timeout)
            .boxed();

        (transport, Some(relay_client))
    } else {
        let tcp_transport = tcp_ws_transport
            .upgrade(core::upgrade::Version::V1Lazy)
            .authenticate(auth_config)
            .multiplex(muxer_config)
            .boxed();

        (tcp_transport, None)
    };

    // Merge in Quick
    let transport = OrTransport::new(quic_transport, tcp_ws_transport)
        .map(|o, _| match o {
            Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        })
        .boxed();

    // Setup dns resolution

    let dns_cfg = dns::ResolverConfig::cloudflare();
    let dns_opts = dns::ResolverOpts::default();
    let transport = dns::TokioDnsConfig::custom(transport, dns_cfg, dns_opts)
        .unwrap()
        .boxed();

    (transport, relay_client)
}

pub(crate) async fn build_swarm<I, M>(
    config: &Libp2pConfig,
    keypair: &Keypair,
    recons: Option<(I, M)>,
    sql_pool: SqlitePool,
) -> Result<Swarm<NodeBehaviour<I, M>>>
where
    I: Recon<Key = Interest, Hash = Sha256a>,
    M: Recon<Key = EventId, Hash = Sha256a>,
{
    let peer_id = keypair.public().to_peer_id();

    let (transport, relay_client) = build_transport(keypair, config).await;
    let behaviour = NodeBehaviour::new(keypair, config, relay_client, recons, sql_pool).await?;

    let swarm = SwarmBuilder::with_executor(transport, behaviour, peer_id, Tokio)
        .notify_handler_buffer_size(config.notify_handler_buffer_size.try_into()?)
        .per_connection_event_buffer_size(config.connection_event_buffer_size)
        .dial_concurrency_factor(config.dial_concurrency_factor.try_into().unwrap())
        .build();

    Ok(swarm)
}

struct Tokio;
impl Executor for Tokio {
    fn exec(&self, fut: std::pin::Pin<Box<dyn futures::Future<Output = ()> + Send>>) {
        tokio::task::spawn(fut);
    }
}
