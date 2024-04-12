use anyhow::Result;
use ceramic_core::{EventId, Interest};
use libp2p::{noise, relay, tcp, tls, yamux, Swarm, SwarmBuilder};
use libp2p_identity::Keypair;
use recon::{libp2p::Recon, Sha256a};
use std::sync::Arc;

use crate::{behaviour::NodeBehaviour, Libp2pConfig, Metrics};

pub(crate) async fn build_swarm<I, M, S>(
    config: &Libp2pConfig,
    keypair: Keypair,
    recons: Option<(I, M)>,
    block_store: Arc<S>,
    metrics: Metrics,
) -> Result<Swarm<NodeBehaviour<I, M, S>>>
where
    I: Recon<Key = Interest, Hash = Sha256a>,
    M: Recon<Key = EventId, Hash = Sha256a>,
    S: iroh_bitswap::Store,
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

fn new_behavior<I, M, S>(
    config: &Libp2pConfig,
    keypair: &Keypair,
    relay_client: Option<relay::client::Behaviour>,
    recons: Option<(I, M)>,
    block_store: Arc<S>,
    metrics: Metrics,
) -> Result<NodeBehaviour<I, M, S>>
where
    I: Recon<Key = Interest, Hash = Sha256a> + Send,
    M: Recon<Key = EventId, Hash = Sha256a> + Send,
    S: iroh_bitswap::Store,
{
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
