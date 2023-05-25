use std::net;

use actix_web::{dev::Server, get, http::header::ContentType, App, HttpResponse, HttpServer};
use anyhow::Result;
use ceramic_kubo_rpc::PeerId;
use libp2p::metrics::Recorder;
use prometheus_client::registry::Registry;
use prometheus_client::{encoding::EncodeLabelSet, metrics::counter::Counter};
use prometheus_client::{encoding::EncodeLabelValue, metrics::family::Family};

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

#[get("/metrics")]
async fn metrics() -> HttpResponse {
    let data = iroh_metrics::MetricsHandle::encode();
    HttpResponse::Ok()
        .content_type(ContentType::plaintext())
        .body(data)
}

/// Create and start server for metrics endpoint.
/// NOTE: The server must be awaited.
///
/// Automatically registers shutdown listeners for interrupt and kill signals.
/// See <https://actix.rs/docs/server/#graceful-shutdown>
pub fn server<A>(addrs: A) -> Result<Server>
where
    A: net::ToSocketAddrs,
{
    Ok(HttpServer::new(move || App::new().service(metrics))
        .bind(addrs)?
        .disable_signals()
        .run())
}
