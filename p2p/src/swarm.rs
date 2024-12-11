use anyhow::Result;
use ceramic_core::{EventId, PeerKey};
use libp2p::{dns, noise, relay, tcp, tls, yamux, Swarm, SwarmBuilder};
use libp2p_identity::Keypair;
use recon::{libp2p::Recon, Sha256a};
use std::sync::Arc;

use crate::{behaviour::NodeBehaviour, peers, Libp2pConfig, Metrics};

fn get_dns_config() -> (dns::ResolverConfig, dns::ResolverOpts) {
    match hickory_resolver::system_conf::read_system_conf() {
        Ok((conf, opts)) => {
            // Won't be necessary if https://github.com/hickory-dns/hickory-dns/pull/2327 is merged/incoporated into libp2p
            let conf = if conf.name_servers().is_empty() {
                tracing::warn!(
                    "No nameservers found in system DNS configuration. Using Google DNS servers."
                );
                dns::ResolverConfig::default()
            } else {
                conf
            };
            (conf, opts)
        }
        Err(e) => {
            tracing::warn!(err=%e, "Failed to load system DNS configuration. Switching to Google DNS servers. This can only be retried on server restart.");
            (dns::ResolverConfig::default(), dns::ResolverOpts::default())
        }
    }
}

pub(crate) async fn build_swarm<P, M, S>(
    config: &Libp2pConfig,
    keypair: Keypair,
    recons: Option<(P, M)>,
    block_store: Arc<S>,
    peers_tx: tokio::sync::mpsc::Sender<peers::Message>,
    metrics: Metrics,
) -> Result<Swarm<NodeBehaviour<P, M, S>>>
where
    P: Recon<Key = PeerKey, Hash = Sha256a>,
    M: Recon<Key = EventId, Hash = Sha256a>,
    S: iroh_bitswap::Store,
{
    let (dns_conf, dns_opts) = get_dns_config();
    let builder = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            // we've encountered errors when restarting the binary failing to bind with port_reuse(true)
            // so we use the default which has it disabled and use transient ports for outgoing connections
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_dns_config(dns_conf, dns_opts)
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
                    peers_tx,
                    metrics.clone(),
                )
                .map_err(|err| err.into())
            })?
            .with_swarm_config(with_config)
            .build())
    } else {
        Ok(builder
            .with_behaviour(|keypair| {
                new_behavior(
                    config,
                    keypair,
                    None,
                    recons,
                    block_store,
                    peers_tx,
                    metrics.clone(),
                )
                .map_err(|err| err.into())
            })?
            .with_swarm_config(with_config)
            .build())
    }
}

fn new_behavior<P, M, S>(
    config: &Libp2pConfig,
    keypair: &Keypair,
    relay_client: Option<relay::client::Behaviour>,
    recons: Option<(P, M)>,
    block_store: Arc<S>,
    peers_tx: tokio::sync::mpsc::Sender<peers::Message>,
    metrics: Metrics,
) -> Result<NodeBehaviour<P, M, S>>
where
    P: Recon<Key = PeerKey, Hash = Sha256a> + Send,
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
            peers_tx,
            metrics,
        ))
    })
    .join()
    .unwrap()
}
