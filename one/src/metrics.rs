use std::{convert::Infallible, net::SocketAddr};

use anyhow::Result;
use ceramic_kubo_rpc::PeerId;
use ceramic_metrics::Recorder;
use hyper::{
    http::HeaderValue,
    service::{make_service_fn, service_fn},
    Body, Request, Response,
};
use prometheus_client::{encoding::EncodeLabelSet, metrics::counter::Counter};
use prometheus_client::{encoding::EncodeLabelValue, metrics::family::Family};
use prometheus_client::{metrics::info::Info, registry::Registry};
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct InfoLabels {
    service_name: String,
    version: String,
    build: String,
    instance_id: String,
    exe_hash: String,
}

impl From<crate::Info> for InfoLabels {
    fn from(info: crate::Info) -> Self {
        Self {
            service_name: info.service_name,
            version: info.version,
            build: info.build,
            instance_id: info.instance_id,
            exe_hash: info.exe_hash,
        }
    }
}

pub struct Metrics {
    messages: Family<MsgLabels, Counter>,
    peers: Family<PeerLabels, Counter>,
    tip_loads: Family<TipLoadLabels, Counter>,
}

impl Metrics {
    pub fn register(info: crate::Info, registry: &mut Registry) -> Self {
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

        let info: Info<InfoLabels> = Info::new(info.into());
        sub_registry.register("one", "Information about the ceramic-one process", info);

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
    let data = ceramic_metrics::MetricsHandle::encode();
    let mut resp = Response::new(Body::from(data));
    resp.headers_mut().insert(
        "Content-Type",
        // Use OpenMetrics content type so prometheus knows to parse it accordingly
        HeaderValue::from_static("application/openmetrics-text"),
    );
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
