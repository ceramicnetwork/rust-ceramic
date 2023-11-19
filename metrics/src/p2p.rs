use std::fmt;

use prometheus_client::{
    metrics::{counter::Counter, gauge::Gauge},
    registry::Registry,
};
use tracing::error;

use crate::{
    core::{HistogramType, MRecorder, MetricType, MetricsRecorder},
    Collector,
};

pub(crate) type Libp2pMetrics = libp2p::metrics::Metrics;

#[derive(Default, Clone)]
pub(crate) struct Metrics {
    bad_peers: Counter,
    bad_peers_removed: Counter,
    bootstrap_peers_connected: Gauge,
    skipped_peer_bitswap: Counter,
    skipped_peer_kad: Counter,
    loops: Counter,
}

impl fmt::Debug for Metrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("P2P Metrics").finish()
    }
}

impl Metrics {
    pub(crate) fn new(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry_with_prefix("p2p");
        let bad_peers = Counter::default();
        sub_registry.register(P2PMetrics::BadPeer.name(), "", bad_peers.clone());

        let bad_peers_removed = Counter::default();
        sub_registry.register(
            P2PMetrics::BadPeerRemoved.name(),
            "",
            bad_peers_removed.clone(),
        );

        let bootstrap_peers_connected = Gauge::default();
        sub_registry.register(
            P2PMetrics::BootstrapPeersConnected.name(),
            "",
            bootstrap_peers_connected.clone(),
        );

        let skipped_peer_bitswap = Counter::default();
        sub_registry.register(
            P2PMetrics::SkippedPeerBitswap.name(),
            "",
            skipped_peer_bitswap.clone(),
        );
        let skipped_peer_kad = Counter::default();
        sub_registry.register(
            P2PMetrics::SkippedPeerKad.name(),
            "",
            skipped_peer_kad.clone(),
        );

        let loops = Counter::default();
        sub_registry.register(P2PMetrics::LoopCounter.name(), "", loops.clone());

        Self {
            bad_peers,
            bad_peers_removed,
            bootstrap_peers_connected,
            skipped_peer_bitswap,
            skipped_peer_kad,
            loops,
        }
    }
}

impl MetricsRecorder for Metrics {
    fn record<M>(&self, m: M, value: i64)
    where
        M: MetricType + std::fmt::Display,
    {
        // Counters should never record negative values
        if m.name() == P2PMetrics::BadPeer.name() {
            self.bad_peers.inc_by(value.unsigned_abs());
        } else if m.name() == P2PMetrics::BadPeerRemoved.name() {
            self.bad_peers_removed.inc_by(value.unsigned_abs());
        } else if m.name() == P2PMetrics::BootstrapPeersConnected.name() {
            // This is a gauge and can record negative values
            self.bootstrap_peers_connected.inc_by(value);
        } else if m.name() == P2PMetrics::BadPeerRemoved.name() {
            self.bad_peers_removed.inc_by(value.unsigned_abs());
        } else if m.name() == P2PMetrics::SkippedPeerBitswap.name() {
            self.skipped_peer_bitswap.inc_by(value.unsigned_abs());
        } else if m.name() == P2PMetrics::SkippedPeerKad.name() {
            self.skipped_peer_kad.inc_by(value.unsigned_abs());
        } else if m.name() == P2PMetrics::LoopCounter.name() {
            self.loops.inc_by(value.unsigned_abs());
        } else {
            error!("record (p2p): unknown metric {}", m.name());
        }
    }

    fn observe<M>(&self, m: M, _value: f64)
    where
        M: HistogramType + std::fmt::Display,
    {
        error!("observe (p2p): unknown metric {}", m.name());
    }
}

#[derive(Clone, Debug)]
pub enum P2PMetrics {
    BadPeer,
    BadPeerRemoved,
    BootstrapPeersConnected,
    SkippedPeerBitswap,
    SkippedPeerKad,
    LoopCounter,
}

impl MetricType for P2PMetrics {
    fn name(&self) -> &'static str {
        match self {
            P2PMetrics::BadPeer => "bad_peer",
            P2PMetrics::BadPeerRemoved => "bad_peer_removed",
            P2PMetrics::BootstrapPeersConnected => "bootstrap_peers_connected",
            P2PMetrics::SkippedPeerBitswap => "skipped_peer_bitswap",
            P2PMetrics::SkippedPeerKad => "skipped_peer_kad",
            P2PMetrics::LoopCounter => "loop_counter",
        }
    }
}

impl MRecorder for P2PMetrics {
    fn record(&self, value: i64) {
        crate::record(Collector::P2P, self.clone(), value);
    }
}

impl std::fmt::Display for P2PMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
