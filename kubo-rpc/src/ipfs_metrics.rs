use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use ceramic_metadata::Version;
use ceramic_metrics::Recorder;
use cid::Cid;
use futures_util::Future;
use ipld_core::ipld::Ipld;
use libp2p_identity::PeerId;
use multiaddr::Multiaddr;
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{
        counter::Counter,
        family::Family,
        histogram::{exponential_buckets, Histogram},
    },
    registry::Registry,
};
use tokio::time::Instant;

use crate::{error::Error, IpfsDep, IpfsPath, PeerInfo};

/// Record metrics about calls to Ipfs
#[derive(Clone)]
pub struct IpfsMetrics {
    calls: Family<IpfsCallLabels, Counter>,
    call_durations: Family<IpfsCallLabels, Histogram>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct IpfsCallLabels {
    method: &'static str,
}
impl From<&IpfsCallEvent> for IpfsCallLabels {
    fn from(value: &IpfsCallEvent) -> Self {
        Self {
            method: value.method,
        }
    }
}

impl IpfsMetrics {
    /// Register and construct Metrics
    pub fn register(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry_with_prefix("ipfs");

        let calls = Family::<IpfsCallLabels, Counter>::default();
        sub_registry.register("calls", "Number of API calls", calls.clone());

        let call_durations = Family::<IpfsCallLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(exponential_buckets(0.005, 2.0, 20))
        });
        sub_registry.register(
            "call_durations",
            "Duration of API calls",
            call_durations.clone(),
        );

        Self {
            calls,
            call_durations,
        }
    }
}

struct IpfsCallEvent {
    method: &'static str,
    duration: Duration,
}

impl Recorder<IpfsCallEvent> for IpfsMetrics {
    fn record(&self, event: &IpfsCallEvent) {
        let labels: IpfsCallLabels = event.into();
        self.calls.get_or_create(&labels).inc();
        self.call_durations
            .get_or_create(&labels)
            .observe(event.duration.as_secs_f64());
    }
}

/// Implements IpfsDep and records metrics about each call.
#[derive(Clone)]
pub struct IpfsMetricsMiddleware<I: Clone> {
    ipfs: I,
    metrics: IpfsMetrics,
}

impl<I: Clone> IpfsMetricsMiddleware<I> {
    /// Construct a new IpfsMetricsMiddleware.
    /// The metrics should have already be registered.
    pub fn new(ipfs: I, metrics: IpfsMetrics) -> Self {
        Self { ipfs, metrics }
    }
    // Record metrics for a given API endpoint
    async fn record<T>(&self, method: &'static str, fut: impl Future<Output = T>) -> T {
        let start = Instant::now();
        let ret = fut.await;
        let duration = start.elapsed();
        let event = IpfsCallEvent { method, duration };
        self.metrics.record(&event);
        ret
    }
}

#[async_trait]
impl<I> IpfsDep for IpfsMetricsMiddleware<I>
where
    I: IpfsDep,
    I: Clone + Send + Sync,
{
    async fn lookup_local(&self) -> Result<PeerInfo, Error> {
        self.record("lookup_local", self.ipfs.lookup_local()).await
    }
    async fn lookup(&self, peer_id: PeerId) -> Result<PeerInfo, Error> {
        self.record("lookup", self.ipfs.lookup(peer_id)).await
    }
    async fn block_size(&self, cid: Cid) -> Result<u64, Error> {
        self.record("block_size", self.ipfs.block_size(cid)).await
    }
    async fn block_get(&self, cid: Cid) -> Result<Bytes, Error> {
        self.record("block_get", self.ipfs.block_get(cid)).await
    }
    async fn get(&self, ipfs_path: &IpfsPath) -> Result<(Cid, Ipld), Error> {
        self.record("get", self.ipfs.get(ipfs_path)).await
    }
    async fn resolve(&self, ipfs_path: &IpfsPath) -> Result<(Cid, String), Error> {
        self.record("resolve", self.ipfs.resolve(ipfs_path)).await
    }
    async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>, Error> {
        self.record("peers", self.ipfs.peers()).await
    }
    async fn connect(&self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<(), Error> {
        self.record("connect", self.ipfs.connect(peer_id, addrs))
            .await
    }
    async fn version(&self) -> Result<Version, Error> {
        self.record("version", self.ipfs.version()).await
    }
}
