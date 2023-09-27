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
    // TODO: This is an url injection vulnerability, Validate that the IP address first.
    let peer_addr = format!(
        "http://{}:{}/ceramic/recon?ring=interest",
        peer.ip, peer.port
    );
    let initial_message = interest_client.initial_messages().await?;
    // TODO: Validate that the IP address is safe
    let initial_message: Vec<u8> = serde_cbor::to_vec(&initial_message)?;
    let req = Request::builder()
        .method(Method::POST)
        .uri(peer_addr.as_str())
        .body(initial_message.into())?;
    let mut res = http_client.request(req).await?;
    let body = hyper::body::to_bytes(res.body_mut()).await?;
    serde_cbor::from_slice(body.as_ref())
        .map_err(|e| anyhow!("failed to deserialize interest messages: {}", e))
}

async fn recon_sync_models(
    peer: &mut Peer,
    model_client: &ReconModelClient,
    http_client: &hyper::Client<HttpConnector>,
) -> Result<Vec<ReconModelMessage>> {
    let peer_addr = format!("http://{}:{}/ceramic/recon?ring=model", peer.ip, peer.port);
    let initial_message = model_client.initial_messages().await?;
    // TODO: Validate that the IP address is safe
    let initial_message: Vec<u8> = serde_cbor::to_vec(&initial_message)?;
    let req = Request::builder()
        .method(Method::POST)
        .uri(peer_addr.as_str())
        .body(initial_message.into())?;
    let mut res = http_client.request(req).await?;
    let mut body = hyper::body::to_bytes(res.body_mut()).await?;
    let mut messages: Vec<ReconModelMessage> = serde_cbor::from_slice(body.as_ref())
        .map_err(|e| anyhow!("failed to deserialize model messages: {}", e))?;
    loop {
        let process_response = model_client.process_messages(messages).await?;
        if process_response.is_synchronized() {
            peer.last_sync = Some(Instant::now());
            break;
        }
        let response_body = serde_cbor::to_vec(&process_response.into_messages())?;
        let req = Request::builder()
            .method(Method::POST)
            .uri(peer_addr.as_str())
            .body(response_body.into())?;
        res = http_client.request(req).await?;
        body = hyper::body::to_bytes(res.body_mut()).await?;
        messages = serde_cbor::from_slice(body.as_ref())
            .map_err(|e| anyhow!("failed to deserialize model messages: {}", e))?;
    }
    Ok(vec![])
}
