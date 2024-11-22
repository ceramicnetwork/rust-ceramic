use std::{ops::Range, time::Duration};

use async_trait::async_trait;
use ceramic_core::{NodeId, PeerKey};
use ceramic_metrics::{register, Recorder};
use ceramic_p2p::PeerService;
use futures::Future;
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{
        counter::Counter,
        family::Family,
        histogram::{exponential_buckets, Histogram},
    },
    registry::Registry,
};
use recon::{AssociativeHash, HashCount, ReconItem, Result as ReconResult};
use tokio::time::Instant;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct StorageQuery {
    pub name: &'static str,
    pub duration: Duration,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct InsertEvent {
    pub cnt: u64,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct QueryLabels {
    name: &'static str,
}

impl From<&StorageQuery> for QueryLabels {
    fn from(value: &StorageQuery) -> Self {
        Self { name: value.name }
    }
}

#[derive(Clone, Debug)]
/// Storage system metrics
pub struct Metrics {
    key_value_insert_count: Counter,

    query_durations: Family<QueryLabels, Histogram>,
}

impl Metrics {
    /// Register and construct Metrics
    pub fn register(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry_with_prefix("store");

        register!(
            key_value_insert_count,
            "Number times a new key/value pair is inserted into the datastore",
            Counter::default(),
            sub_registry
        );

        register!(
            query_durations,
            "Durations of store queries in seconds",
            Family::<QueryLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.005, 2.0, 20))
            }),
            sub_registry
        );

        Self {
            key_value_insert_count,
            query_durations,
        }
    }
}

impl Recorder<InsertEvent> for Metrics {
    fn record(&self, event: &InsertEvent) {
        self.key_value_insert_count.inc_by(event.cnt);
    }
}

impl Recorder<StorageQuery> for Metrics {
    fn record(&self, event: &StorageQuery) {
        let labels: QueryLabels = event.into();
        self.query_durations
            .get_or_create(&labels)
            .observe(event.duration.as_secs_f64());
    }
}

/// Implement the Store and record metrics
#[derive(Debug, Clone)]
pub struct StoreMetricsMiddleware<S>
where
    S: Send + Sync,
{
    store: S,
    metrics: Metrics,
}

impl<S: Send + Sync> StoreMetricsMiddleware<S> {
    /// Construct a new StoreMetricsMiddleware.
    /// The metrics should have already be registered.
    pub fn new(store: S, metrics: Metrics) -> Self {
        Self { store, metrics }
    }
    // Record metrics for a given API endpoint
    async fn record<T>(metrics: &Metrics, name: &'static str, fut: impl Future<Output = T>) -> T {
        let start = Instant::now();
        let ret = fut.await;
        let duration = start.elapsed();
        let event = StorageQuery { name, duration };
        metrics.record(&event);
        ret
    }
}

#[async_trait]
impl<S, K, H> recon::Store for StoreMetricsMiddleware<S>
where
    S: recon::Store<Key = K, Hash = H> + Send + Sync,
    K: recon::Key,
    H: AssociativeHash,
{
    type Key = K;
    type Hash = H;

    async fn insert_many(
        &self,
        items: &[ReconItem<Self::Key>],
        informant: NodeId,
    ) -> ReconResult<recon::InsertResult<Self::Key>> {
        let res = StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "insert_many",
            self.store.insert_many(items, informant),
        )
        .await?;

        self.metrics.record(&InsertEvent {
            cnt: res.count_inserted() as u64,
        });

        Ok(res)
    }

    async fn hash_range(&self, range: Range<&Self::Key>) -> ReconResult<HashCount<Self::Hash>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "hash_range",
            self.store.hash_range(range),
        )
        .await
    }

    async fn range(
        &self,
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "range",
            self.store.range(range, offset, limit),
        )
        .await
    }
    async fn range_with_values(
        &self,
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "range_with_values",
            self.store.range_with_values(range, offset, limit),
        )
        .await
    }

    async fn full_range(
        &self,
    ) -> ReconResult<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        StoreMetricsMiddleware::<S>::record(&self.metrics, "full_range", self.store.full_range())
            .await
    }

    async fn middle(&self, range: Range<&Self::Key>) -> ReconResult<Option<Self::Key>> {
        StoreMetricsMiddleware::<S>::record(&self.metrics, "middle", self.store.middle(range)).await
    }
    async fn count(&self, range: Range<&Self::Key>) -> ReconResult<usize> {
        StoreMetricsMiddleware::<S>::record(&self.metrics, "count", self.store.count(range)).await
    }
    async fn len(&self) -> ReconResult<usize> {
        StoreMetricsMiddleware::<S>::record(&self.metrics, "len", self.store.len()).await
    }

    async fn is_empty(&self) -> ReconResult<bool> {
        StoreMetricsMiddleware::<S>::record(&self.metrics, "is_empty", self.store.is_empty()).await
    }

    async fn value_for_key(&self, key: &Self::Key) -> ReconResult<Option<Vec<u8>>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "value_for_key",
            self.store.value_for_key(key),
        )
        .await
    }
}
#[async_trait]
impl<S> ceramic_p2p::PeerService for StoreMetricsMiddleware<S>
where
    S: PeerService + Send + Sync,
{
    async fn insert(&self, peer: &PeerKey) -> anyhow::Result<()> {
        StoreMetricsMiddleware::<S>::record(&self.metrics, "insert", self.store.insert(peer)).await
    }
    async fn delete_range(&self, range: Range<&PeerKey>) -> anyhow::Result<()> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "delete_range",
            self.store.delete_range(range),
        )
        .await
    }
    async fn all_peers(&self) -> anyhow::Result<Vec<PeerKey>> {
        StoreMetricsMiddleware::<S>::record(&self.metrics, "all_peers", self.store.all_peers())
            .await
    }
}
