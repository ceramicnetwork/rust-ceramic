use iroh_api::PeerId;
use libp2p::metrics::Recorder;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::registry::Registry;
use prometheus_client::{encoding::text::Encode, metrics::family::Family};

use crate::pubsub;

#[derive(Clone, Debug, Hash, PartialEq, Eq, Encode)]
struct MsgLabels {
    msg_type: MsgType,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Encode)]
enum MsgType {
    Update,
    Query,
    Response,
    Keepalive,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Encode)]
struct PeerLabels {
    peer_id: String,
    version: String,
}

pub struct Metrics {
    messages: Family<MsgLabels, Counter>,
    peers: Family<PeerLabels, Counter>,
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
            Box::new(messages.clone()),
        );

        let peers = Family::<PeerLabels, Counter>::default();
        sub_registry.register(
            "peers",
            "Number of keepalive messages from each peer, useful for understanding network topology",
            Box::new(peers.clone()),
        );

        Self { messages, peers }
    }
}

impl Recorder<(PeerId, pubsub::Message)> for Metrics {
    fn record(&self, event: &(PeerId, pubsub::Message)) {
        let msg_type = match &event.1 {
            pubsub::Message::Update { .. } => MsgType::Update,
            pubsub::Message::Query { .. } => MsgType::Query,
            pubsub::Message::Response { .. } => MsgType::Response,
            pubsub::Message::Keepalive { ver, .. } => {
                self.peers
                    .get_or_create(&PeerLabels {
                        peer_id: event.0.to_string(),
                        version: ver.to_owned(),
                    })
                    .inc();
                MsgType::Keepalive
            }
        };
        self.messages.get_or_create(&MsgLabels { msg_type }).inc();
    }
}

//pub struct MetricsInfo {
//    pub config: Config,
//    pub local_peer_id: PeerId,
//}
//pub fn register_info(info: MetricsInfo) -> impl FnOnce(&mut Registry) -> () {
//    move |registry: &mut Registry| {
//        let sub_registry = registry.sub_registry_with_prefix("ceramic_one");
//        let info_metric = Info::new(vec![
//            ("service_name", info.config.service_name),
//            ("instance_id", info.config.instance_id),
//            ("build", info.config.build),
//            ("version", info.config.version),
//            ("service_env", info.config.service_env),
//            ("local_peer_id", info.local_peer_id.to_string()),
//        ]);
//        sub_registry.register(
//            "info",
//            "Information about the ceramic-one process",
//            Box::new(info_metric),
//        );
//    }
//}
