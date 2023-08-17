use std::{convert::Infallible, net::SocketAddr};

use anyhow::Result;
use ceramic_kubo_rpc::PeerId;
use hyper::{
    http::HeaderValue,
    service::{make_service_fn, service_fn},
    Body, Request, Response,
};
use libp2p::metrics::Recorder;
use prometheus_client::registry::Registry;
use prometheus_client::{encoding::EncodeLabelSet, metrics::counter::Counter};
use prometheus_client::{encoding::EncodeLabelValue, metrics::family::Family};
use tokio::{sync::oneshot, task::JoinHandle};

use crate::pubsub;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct MsgLabels {
    msg_type: MsgType,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
enum MsgType {
    Update,
    Query,
    Response,
    Keepalive,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct PeerLabels {
    peer_id: String,
    version: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct TipLoadLabels {
    result: TipLoadResult,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum TipLoadResult {
    Success,
    Failure,
}

pub struct Metrics {
    messages: Family<MsgLabels, Counter>,
    peers: Family<PeerLabels, Counter>,
    tip_loads: Family<TipLoadLabels, Counter>,
}

impl Metrics {
    pub fn new(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry_with_prefix("ceramic");

        let messages = Family::<MsgLabels, Counter>::default();

        // Create each combination of labels so that we have explicit zeros reported
        // until the first message arrives.
        messages
            .get_or_create(&MsgLabels {
                msg_type: MsgType::Update,
            })
            .get();
        messages
            .get_or_create(&MsgLabels {
                msg_type: MsgType::Query,
            })
            .get();
        messages
            .get_or_create(&MsgLabels {
                msg_type: MsgType::Response,
            })
            .get();
        messages
            .get_or_create(&MsgLabels {
                msg_type: MsgType::Keepalive,
            })
            .get();

        sub_registry.register(
            "pubsub_messages",
            "Number of ceramic pubsub messages received",
            messages.clone(),
        );

        let peers = Family::<PeerLabels, Counter>::default();
        sub_registry.register(
            "peers",
            "Number of keepalive messages from each peer, useful for understanding network topology",
            peers.clone(),
        );

        let tip_loads = Family::<TipLoadLabels, Counter>::default();
        sub_registry.register("tip_loads", "Number tip loads", tip_loads.clone());

        Self {
            messages,
            peers,
            tip_loads,
        }
    }
}

impl Recorder<(Option<PeerId>, pubsub::Message)> for Metrics {
    fn record(&self, event: &(Option<PeerId>, pubsub::Message)) {
        let msg_type = match &event.1 {
            pubsub::Message::Update { .. } => MsgType::Update,
            pubsub::Message::Query { .. } => MsgType::Query,
            pubsub::Message::Response { .. } => MsgType::Response,
            pubsub::Message::Keepalive { ver, .. } => {
                // Record peers total
                if let Some(source) = event.0 {
                    self.peers
                        .get_or_create(&PeerLabels {
                            peer_id: source.to_string(),
                            version: ver.to_owned(),
                        })
                        .inc();
                }
                MsgType::Keepalive
            }
        };
        // Record messages total
        self.messages.get_or_create(&MsgLabels { msg_type }).inc();
    }
}

impl Recorder<TipLoadResult> for Metrics {
    fn record(&self, result: &TipLoadResult) {
        self.tip_loads
            .get_or_create(&TipLoadLabels {
                result: result.clone(),
            })
            .inc();
    }
}

async fn handle(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let data = iroh_metrics::MetricsHandle::encode();
    let mut resp = Response::new(Body::from(data));
    resp.headers_mut()
        .insert("Content-Type", HeaderValue::from_static("text/plain"));
    Ok(resp)
}

/// Start metrics server.
/// Sending on the returned channel will cause the server to shutdown gracefully.
pub fn start(addr: &SocketAddr) -> (oneshot::Sender<()>, JoinHandle<Result<(), hyper::Error>>) {
    let (tx, rx) = oneshot::channel::<()>();
    let service = make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(handle)) });

    let server = hyper::Server::bind(addr)
        .serve(service)
        .with_graceful_shutdown(async {
            rx.await.ok();
        });
    (tx, tokio::spawn(server))
}
