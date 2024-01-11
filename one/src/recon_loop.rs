//! recon_event_loop synchronizes interests and models
//! To generate events for the loop to synchronize, use the following Ceramic Pocket Knife (cpk) command:
//! ```bash
//! model=k2t6wz4ylx0qnsxc7rroideki5ea96hy0m99gt9bhqhpjfnt7n5bydluk9l29x
//! controller=did:key:z5XyAyVKJpA2G4c1R52U4JBDSJX6qvBMHaseQVbbkLuYR
//! count=32000
//!
//! # echo "Resetting dbs..."
//! # rm peer_1/db.sqlite3
//! # rm peer_2/db.sqlite3
//!
//! cpk sql-db-generate \
//!     --sort-key model \
//!     --sort-value $model \
//!     --controller $controller \
//!     --count $count \
//!     --path peer_1/db.sqlite3
//!
//! cpk sql-db-generate \
//!     --sort-key model \
//!     --sort-value $model \
//!     --controller $controller \
//!     --count $count \
//!     --path peer_2/db.sqlite3
//!
//! echo "Starting peer 1"
//! RUST_LOG=info,ceramic_p2p=debug,ceramic_api=debug,recon::libp2p=debug \
//! CERAMIC_ONE_NETWORK=in-memory \
//! CERAMIC_ONE_BIND_ADDRESS=127.0.0.1:5001 \
//! CERAMIC_ONE_PEER_PORT=5002 \
//! CERAMIC_ONE_RECON_HTTP=true \
//! CERAMIC_ONE_STORE_DIR=peer_1 \
//! CERAMIC_ONE_METRICS_BIND_ADDRESS=127.0.0.1:9091
//! CERAMIC_ONE_SWARM_ADDRESSES /ip4/0.0.0.0/tcp/0 \
//! cargo run --release -p ceramic-one -- daemon
//!
//! echo "Starting peer 2"
//! RUST_LOG=info,ceramic_p2p=debug,ceramic_api=debug,recon::libp2p=debug \
//! CERAMIC_ONE_NETWORK=in-memory \
//! CERAMIC_ONE_BIND_ADDRESS=127.0.0.1:5002 \
//! CERAMIC_ONE_PEER_PORT=5001 \
//! CERAMIC_ONE_RECON_HTTP=true \
//! CERAMIC_ONE_STORE_DIR=peer_2 \
//! CERAMIC_ONE_METRICS_BIND_ADDRESS=127.0.0.1:9092
//! CERAMIC_ONE_SWARM_ADDRESSES /ip4/0.0.0.0/tcp/0 \
//! cargo run --release -p ceramic-one -- daemon
//!
//! echo "Subscribing to model..."
//! echo "curl -s \"http://localhost:5001/ceramic/subscribe/model/$model?controller=$controller&limit=10\""
//! curl -s "http://localhost:5001/ceramic/subscribe/model/$model?controller=$controller&limit=10"
//! curl -s "http://localhost:5002/ceramic/subscribe/model/$model?controller=$controller&limit=10"
//! ```
use anyhow::{anyhow, Result};
use ceramic_core::{EventId, Interest};
use hyper::client::HttpConnector;
use hyper::{Method, Request};
use recon::libp2p::Config;
use recon::{Client, Sha256a};
use std::time::Instant;
use std::{env, time::Duration};
use tracing::info;

type ReconInterestClient = Client<Interest, Sha256a>;
type ReconModelClient = Client<EventId, Sha256a>;
type ReconInterestMessage = recon::Message<Interest, Sha256a>;
type ReconModelMessage = recon::Message<EventId, Sha256a>;

struct Peer {
    last_sync: Option<Instant>,
    ip: String,
    port: u16,
    config: Config,
}

pub async fn recon_event_loop(
    interest_client: ReconInterestClient,
    model_client: ReconModelClient,
) -> Result<()> {
    info!("Started Recon event loop!");
    // TODO: Peer management
    let peer_port = match env::var_os("CERAMIC_ONE_PEER_PORT") {
        Some(v) => v.into_string().unwrap(),
        None => panic!("Peer address is not set"),
    };
    let mut peers = vec![Peer {
        ip: "127.0.0.1".to_owned(),
        port: peer_port.parse()?,
        last_sync: None,
        config: Config::default(),
    }];
    let client = hyper::Client::new();
    loop {
        // Check each peer and start synchronization as needed.
        for peer in &mut peers {
            let should_sync = if let Some(last_sync) = &peer.last_sync {
                last_sync.elapsed() > peer.config.per_peer_sync_timeout
            } else {
                true
            };
            if should_sync {
                match recon_sync_interests(peer, &interest_client, &client).await {
                    Ok(interest_messages) => {
                        info!(
                            "{}:{} interest messages: {:?}",
                            peer.ip, peer.port, interest_messages
                        )
                    }
                    Err(err) => info!(?err, "failed to connect to peer {}:{}", peer.ip, peer.port),
                }
                match recon_sync_models(peer, &model_client, &client).await {
                    Ok(model_messages) => info!(
                        "{}:{} model messages: {:?}",
                        peer.ip, peer.port, model_messages
                    ),
                    Err(err) => info!(?err, "failed to connect to peer {}:{}", peer.ip, peer.port),
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn recon_sync_interests(
    peer: &mut Peer,
    interest_client: &ReconInterestClient,
    http_client: &hyper::Client<HttpConnector>,
) -> Result<Vec<ReconInterestMessage>> {
    // TODO: This is an url injection vulnerability, validate the IP address first.
    let peer_addr = format!(
        "http://{}:{}/ceramic/recon?ring=interest",
        peer.ip, peer.port
    );
    let messages_to_send = interest_client.initial_messages().await?;
    let bytes_to_send: Vec<u8> = serde_cbor::to_vec(&messages_to_send)?;
    // TODO: Support TLS certs for peers
    let mut received_message = http_client
        .request(
            Request::builder()
                .method(Method::POST)
                .uri(peer_addr.as_str())
                .body(bytes_to_send.into())?,
        )
        .await?;
    let received_bytes = hyper::body::to_bytes(received_message.body_mut()).await?;
    serde_cbor::from_slice(received_bytes.as_ref())
        .map_err(|e| anyhow!("failed to deserialize interest messages: {}", e))
}

async fn recon_sync_models(
    peer: &mut Peer,
    model_client: &ReconModelClient,
    http_client: &hyper::Client<HttpConnector>,
) -> Result<Vec<ReconModelMessage>> {
    // TODO: This is an url injection vulnerability, validate the IP address first.
    let peer_addr = format!("http://{}:{}/ceramic/recon?ring=model", peer.ip, peer.port);
    let mut messages_to_send = model_client.initial_messages().await?;
    loop {
        // Serialize messages to send
        let bytes_to_send: Vec<u8> = serde_cbor::to_vec(&messages_to_send)?;
        // TODO: Support TLS certs for peers
        let mut received_message = http_client
            .request(
                Request::builder()
                    .method(Method::POST)
                    .uri(peer_addr.as_str())
                    .body(bytes_to_send.into())?,
            )
            .await?;
        let received_bytes = hyper::body::to_bytes(received_message.body_mut()).await?;
        // Deserialize received messages
        let received_messages: Vec<ReconModelMessage> =
            serde_cbor::from_slice(received_bytes.as_ref())
                .map_err(|e| anyhow!("failed to deserialize model messages: {}", e))?;
        let process_response = model_client.process_messages(received_messages).await?;
        if process_response.is_synchronized() {
            peer.last_sync = Some(Instant::now());
            break;
        }
        messages_to_send = process_response.into_messages();
    }
    Ok(vec![])
}
