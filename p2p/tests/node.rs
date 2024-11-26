use std::{sync::Arc, time::Duration};

use anyhow::{Context as _, Result};
use ceramic_core::NodeKey;
use ceramic_event_svc::store::SqlitePool;
use iroh_rpc_client::P2pClient;
use iroh_rpc_types::Addr;
use libp2p::{Multiaddr, PeerId};
use recon::{FullInterests, Recon, ReconInterestProvider};
use test_log::test;

use ceramic_p2p::{Config, Metrics, NetworkEvent, Node, PeerKeyInterests};
use tokio::{sync::mpsc::Receiver, task::JoinHandle};
use tracing::debug;

#[derive(Debug)]
struct TestRunnerBuilder {}

impl TestRunnerBuilder {
    fn new() -> Self {
        Self {}
    }

    async fn build(self) -> Result<TestRunner> {
        let rpc_server_addr = Addr::new_mem();
        let rpc_client_addr = rpc_server_addr.clone();
        let mut network_config = Config::default_with_rpc(rpc_client_addr.clone());

        // Bind to an open port
        network_config.libp2p.listening_multiaddrs = vec!["/ip4/0.0.0.0/tcp/0".parse().unwrap()];
        // Do not bootstrap
        network_config.libp2p.ceramic_peers = vec![];

        // Using an in memory DB for the tests for realistic benchmark disk DB is needed.
        let sql_pool = SqlitePool::connect_in_memory().await.unwrap();
        let peer_svc = Arc::new(ceramic_peer_svc::PeerService::new(sql_pool.clone()));
        let interest_svc = Arc::new(ceramic_interest_svc::InterestService::new(sql_pool.clone()));
        let event_svc =
            Arc::new(ceramic_event_svc::EventService::try_new(sql_pool, true, true, vec![]).await?);

        let mut registry = prometheus_client::registry::Registry::default();
        let metrics = Metrics::register(&mut registry);
        let recon_metrics = recon::Metrics::register(&mut registry);
        let node_key = NodeKey::random();
        let peer_id = node_key.peer_id();
        let mut p2p = Node::new(
            network_config,
            rpc_server_addr,
            node_key.clone(),
            Arc::clone(&peer_svc),
            Some((
                Recon::new(peer_svc, PeerKeyInterests, recon_metrics.clone()),
                Recon::new(
                    Arc::clone(&interest_svc),
                    FullInterests::default(),
                    recon_metrics.clone(),
                ),
                Recon::new(
                    Arc::clone(&event_svc),
                    ReconInterestProvider::new(node_key.id(), interest_svc),
                    recon_metrics.clone(),
                ),
            )),
            event_svc,
            metrics,
        )
        .await?;
        let cfg = iroh_rpc_client::Config {
            p2p_addr: Some(rpc_client_addr),
            channels: Some(1),
            ..Default::default()
        };

        let client = iroh_rpc_client::Client::new(cfg).await?;

        let network_events = p2p.network_events();
        let task = tokio::task::spawn(async move { p2p.run().await.unwrap() });

        let client = client.try_p2p()?;

        let addr = tokio::time::timeout(Duration::from_millis(500), get_addr_loop(client.clone()))
            .await
            .context("timed out before getting a listening address for the node")??;
        Ok(TestRunner {
            task,
            client,
            peer_id,
            network_events,
            addr,
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
}

impl Drop for TestRunner {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn test_local_peer_id() -> Result<()> {
    let test_runner = TestRunnerBuilder::new().build().await?;
    let got_peer_id = test_runner.client.local_peer_id().await?;
    assert_eq!(test_runner.peer_id, got_peer_id);
    Ok(())
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_two_nodes() -> Result<()> {
    let test_runner_a = TestRunnerBuilder::new().build().await?;
    let test_runner_b = TestRunnerBuilder::new().build().await?;
    let addrs_b = vec![test_runner_b.addr.clone()];
    debug!(?test_runner_a.peer_id, ?test_runner_b.peer_id, "peer ids");

    let peer_id_a = test_runner_a.client.local_peer_id().await?;
    assert_eq!(test_runner_a.peer_id, peer_id_a);
    let peer_id_b = test_runner_b.client.local_peer_id().await?;
    assert_eq!(test_runner_b.peer_id, peer_id_b);

    // connect
    test_runner_a.client.connect(peer_id_b, addrs_b).await?;

    // Make sure the peers have had time to negotiate protocols
    tokio::time::sleep(Duration::from_millis(3_000)).await;

    is_bi_connected(&test_runner_a, &test_runner_b).await?;

    Ok(())
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn test_three_nodes() -> Result<()> {
    let test_runner_a = TestRunnerBuilder::new().build().await?;
    let test_runner_b = TestRunnerBuilder::new().build().await?;
    let test_runner_c = TestRunnerBuilder::new().build().await?;

    let peer_id_a = test_runner_a.client.local_peer_id().await?;
    assert_eq!(test_runner_a.peer_id, peer_id_a);
    let peer_id_b = test_runner_b.client.local_peer_id().await?;
    assert_eq!(test_runner_b.peer_id, peer_id_b);
    let peer_id_c = test_runner_c.client.local_peer_id().await?;
    assert_eq!(test_runner_c.peer_id, peer_id_c);

    // connect a to b
    test_runner_a
        .client
        .connect(peer_id_b, vec![test_runner_b.addr.clone()])
        .await?;
    // connect b to c
    test_runner_b
        .client
        .connect(peer_id_c, vec![test_runner_c.addr.clone()])
        .await?;

    // Make sure the peers have had time to negotiate protocols
    tokio::time::sleep(Duration::from_millis(3_000)).await;

    assert!(is_bi_connected(&test_runner_a, &test_runner_b).await?);
    assert!(is_bi_connected(&test_runner_b, &test_runner_c).await?);
    // We expect that a and c find each other through b and become connected
    assert!(is_bi_connected(&test_runner_a, &test_runner_c).await?);

    Ok(())
}

// Reports if peers are connected to the other peer in both directions
async fn is_bi_connected(a: &TestRunner, b: &TestRunner) -> Result<bool> {
    Ok(is_connected(a, b).await? && is_connected(b, a).await?)
}
// Reports if a is connected to b
async fn is_connected(a: &TestRunner, b: &TestRunner) -> Result<bool> {
    let peers = a.client.get_peers().await?;
    Ok(peers.contains_key(&b.peer_id))
}

#[test(tokio::test)]
async fn test_cancel_listen_for_identify() -> Result<()> {
    let mut test_runner_a = TestRunnerBuilder::new().build().await?;
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
