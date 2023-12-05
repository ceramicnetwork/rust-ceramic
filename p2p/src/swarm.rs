use anyhow::Result;
use ceramic_core::{EventId, Interest};
use libp2p::{noise, relay, swarm::Executor, tcp, tls, yamux, Swarm, SwarmBuilder};
use libp2p_identity::Keypair;
use recon::{libp2p::Recon, Sha256a};

use crate::{behaviour::NodeBehaviour, Libp2pConfig, Metrics, SQLiteBlockStore};

pub(crate) async fn build_swarm<I, M>(
    config: &Libp2pConfig,
    keypair: Keypair,
    recons: Option<(I, M)>,
    block_store: SQLiteBlockStore,
    metrics: Metrics,
) -> Result<Swarm<NodeBehaviour<I, M>>>
where
    I: Recon<Key = Interest, Hash = Sha256a>,
    M: Recon<Key = EventId, Hash = Sha256a>,
{
    let builder = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default().port_reuse(true),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_dns()?
        .with_websocket(
            (tls::Config::new, noise::Config::new),
            yamux::Config::default,
        )
        .await?;

    let with_config = |cfg: libp2p::swarm::Config| {
        cfg.with_notify_handler_buffer_size(config.notify_handler_buffer_size)
            .with_per_connection_event_buffer_size(config.connection_event_buffer_size)
            .with_dial_concurrency_factor(config.dial_concurrency_factor)
            .with_idle_connection_timeout(config.idle_connection_timeout)
    };
    if config.relay_client {
        Ok(builder
            .with_relay_client(
                (tls::Config::new, noise::Config::new),
                yamux::Config::default,
            )?
            .with_behaviour(|keypair, relay_client| {
                new_behavior(
                    config,
                    keypair,
                    Some(relay_client),
                    recons,
                    block_store,
                    metrics.clone(),
                )
                .map_err(|err| err.into())
            })?
            .with_swarm_config(with_config)
            .build())
    } else {
        Ok(builder
            .with_behaviour(|keypair| {
                new_behavior(config, keypair, None, recons, block_store, metrics.clone())
                    .map_err(|err| err.into())
            })?
            .with_swarm_config(with_config)
            .build())
    }
}

fn new_behavior<I, M>(
    config: &Libp2pConfig,
    keypair: &Keypair,
    relay_client: Option<relay::client::Behaviour>,
    recons: Option<(I, M)>,
    block_store: SQLiteBlockStore,
    metrics: Metrics,
) -> Result<NodeBehaviour<I, M>>
where
    I: Recon<Key = Interest, Hash = Sha256a> + Send,
    M: Recon<Key = EventId, Hash = Sha256a> + Send,
{
    // TODO(WS1-1363): Remove bitswap async initialization
    let keypair = keypair.clone();
    let config = config.clone();
    let handle = tokio::runtime::Handle::current();
    std::thread::spawn(move || {
        handle.block_on(NodeBehaviour::new(
            &keypair,
            &config,
            relay_client,
            recons,
            block_store,
            metrics,
        ))
    })
    .join()
    .unwrap()
}

struct Tokio;
impl Executor for Tokio {
    fn exec(&self, fut: std::pin::Pin<Box<dyn futures::Future<Output = ()> + Send>>) {
        tokio::task::spawn(fut);
    }
}
