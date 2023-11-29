use std::{ops::Deref, sync::Mutex};

use prometheus_client::{encoding::text::encode, registry::Registry};

use crate::bitswap;

lazy_static! {
    pub(crate) static ref CORE: Core = Core::default();
}

pub(crate) struct Core {
    registry: Mutex<Registry>,
    bitswap_metrics: bitswap::Metrics,
    libp2p_metrics: libp2p::metrics::Metrics,
}

impl Default for Core {
    fn default() -> Self {
        let mut reg = Registry::default();
        Core {
            bitswap_metrics: bitswap::Metrics::new(&mut reg),
            libp2p_metrics: libp2p::metrics::Metrics::new(&mut reg),
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

    pub(crate) fn libp2p_metrics(&self) -> &libp2p::metrics::Metrics {
        &self.libp2p_metrics
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut buf = String::new();
        encode(&mut buf, &self.registry()).unwrap();
        buf.into()
    }
}

/// This trait is inefficient as we do string comparisons to find the right metrics to update.
/// The [`Recorder`] trait in this crate is designed to allow for compile time lookup of the metric.
/// New metrics systems should use that trait.
#[deprecated = "use ceramic_metrics::Recorder instead"]
pub trait MetricType {
    fn name(&self) -> &'static str;
}

/// This trait is inefficient as we do string comparisons to find the right metrics to update.
/// The [`Recorder`] trait in this crate is designed to allow for compile time lookup of the metric.
/// New metrics systems should use that trait.
#[deprecated = "use ceramic_metrics::Recorder instead"]
pub trait HistogramType {
    fn name(&self) -> &'static str;
}

/// This trait is inefficient as we do string comparisons to find the right metrics to update.
/// The [`Recorder`] trait in this crate is designed to allow for compile time lookup of the metric.
/// New metrics systems should use that trait.
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

/// This trait is inefficient as we do string comparisons to find the right metrics to update.
/// The [`Recorder`] trait in this crate is designed to allow for compile time lookup of the metric.
/// New metrics systems should use that trait.
#[deprecated = "use ceramic_metrics::Recorder instead"]
pub trait MRecorder {
    fn record(&self, value: u64);
}

/// This trait is inefficient as we do string comparisons to find the right metrics to update.
/// The [`Recorder`] trait in this crate is designed to allow for compile time lookup of the metric.
/// New metrics systems should use that trait.
#[deprecated = "use ceramic_metrics::Recorder instead"]
pub trait MObserver {
    fn observe(&self, value: f64);
}
