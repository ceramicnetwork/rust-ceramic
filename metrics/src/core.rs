use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
};

use prometheus_client::{encoding::text::encode, registry::Registry};

use crate::bitswap;
use crate::p2p;

lazy_static! {
    pub(crate) static ref CORE: Core = Core::default();
}

pub(crate) struct Core {
    enabled: AtomicBool,
    registry: Mutex<Registry>,
    bitswap_metrics: bitswap::Metrics,
    libp2p_metrics: p2p::Libp2pMetrics,
    p2p_metrics: p2p::Metrics,
}

impl Default for Core {
    fn default() -> Self {
        let mut reg = Registry::default();
        Core {
            enabled: AtomicBool::new(false),
            bitswap_metrics: bitswap::Metrics::new(&mut reg),
            libp2p_metrics: p2p::Libp2pMetrics::new(&mut reg),
            p2p_metrics: p2p::Metrics::new(&mut reg),
            registry: Mutex::new(reg),
        }
    }
}

impl Core {
    pub fn register<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&mut Registry) -> T,
    {
        f(&mut self
            .registry
            .lock()
            .expect("should be able to acquire lock"))
    }
    pub(crate) fn registry(&self) -> impl Deref<Target = Registry> + '_ {
        self.registry
            .lock()
            .expect("should be able to acquire lock")
    }

    pub(crate) fn bitswap_metrics(&self) -> &bitswap::Metrics {
        &self.bitswap_metrics
    }

    pub(crate) fn libp2p_metrics(&self) -> &p2p::Libp2pMetrics {
        &self.libp2p_metrics
    }

    pub(crate) fn p2p_metrics(&self) -> &p2p::Metrics {
        &self.p2p_metrics
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut buf = String::new();
        encode(&mut buf, &self.registry()).unwrap();
        buf.into()
    }

    pub(crate) fn set_enabled(&self, enabled: bool) {
        self.enabled.swap(enabled, Ordering::Relaxed);
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }
}

#[deprecated = "use ceramic_metrics::Recorder instead"]
pub trait MetricType {
    fn name(&self) -> &'static str;
}

#[deprecated = "use ceramic_metrics::Recorder instead"]
pub trait HistogramType {
    fn name(&self) -> &'static str;
}

#[deprecated = "use ceramic_metrics::Recorder instead"]
#[allow(deprecated)]
pub trait MetricsRecorder {
    fn record<M>(&self, m: M, value: u64)
    where
        M: MetricType + std::fmt::Display;
    fn observe<M>(&self, m: M, value: f64)
    where
        M: HistogramType + std::fmt::Display;
}

#[deprecated = "use ceramic_metrics::Recorder instead"]
pub trait MRecorder {
    fn record(&self, value: u64);
}

#[deprecated = "use ceramic_metrics::Recorder instead"]
pub trait MObserver {
    fn observe(&self, value: f64);
}
