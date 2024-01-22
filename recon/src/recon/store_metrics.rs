use anyhow::Result;
use async_trait::async_trait;
use ceramic_metrics::Recorder;
use futures::Future;
use tokio::time::Instant;

use crate::{metrics::StoreQuery, recon::HashCount, AssociativeHash, Key, Metrics, Store};

/// Implement the Store and record metrics
#[derive(Debug)]
pub struct StoreMetricsMiddleware<S> {
    store: S,
    metrics: Metrics,
}

impl<S> StoreMetricsMiddleware<S> {
    /// Construct a new StoreMetricsMiddleware.
    /// The metrics should have already be registered.
    pub fn new(store: S, metrics: Metrics) -> Self {
        Self { store, metrics }
    }
    // Record metrics for a given API endpoint
    async fn record<T>(metrics: Metrics, name: &'static str, fut: impl Future<Output = T>) -> T {
        let start = Instant::now();
        let ret = fut.await;
        let duration = start.elapsed();
        let event = StoreQuery { name, duration };
        metrics.record(&event);
        ret
    }
}

#[async_trait]
impl<S, K, H> Store for StoreMetricsMiddleware<S>
where
    S: Store<Key = K, Hash = H> + Send,
    K: Key,
    H: AssociativeHash,
{
    type Key = K;
    type Hash = H;

    async fn insert(&mut self, key: &Self::Key) -> Result<bool> {
        StoreMetricsMiddleware::<S>::record(self.metrics.clone(), "insert", self.store.insert(key))
            .await
    }
    async fn insert_many<'a, I>(&mut self, keys: I) -> Result<bool>
    where
        I: Iterator<Item = &'a Self::Key> + Send,
    {
        StoreMetricsMiddleware::<S>::record(
            self.metrics.clone(),
            "insert_many",
            self.store.insert_many(keys),
        )
        .await
    }

    async fn hash_range(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<HashCount<Self::Hash>> {
        StoreMetricsMiddleware::<S>::record(
            self.metrics.clone(),
            "hash_range",
            self.store.hash_range(left_fencepost, right_fencepost),
        )
        .await
    }

    async fn range(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        StoreMetricsMiddleware::<S>::record(
            self.metrics.clone(),
            "range",
            self.store
                .range(left_fencepost, right_fencepost, offset, limit),
        )
        .await
    }

    async fn full_range(&mut self) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        StoreMetricsMiddleware::<S>::record(
            self.metrics.clone(),
            "full_range",
            self.store.full_range(),
        )
        .await
    }

    async fn middle(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        StoreMetricsMiddleware::<S>::record(
            self.metrics.clone(),
            "middle",
            self.store.middle(left_fencepost, right_fencepost),
        )
        .await
    }
    async fn count(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<usize> {
        StoreMetricsMiddleware::<S>::record(
            self.metrics.clone(),
            "count",
            self.store.count(left_fencepost, right_fencepost),
        )
        .await
    }
    async fn first(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        StoreMetricsMiddleware::<S>::record(
            self.metrics.clone(),
            "first",
            self.store.first(left_fencepost, right_fencepost),
        )
        .await
    }
    async fn last(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        StoreMetricsMiddleware::<S>::record(
            self.metrics.clone(),
            "last",
            self.store.last(left_fencepost, right_fencepost),
        )
        .await
    }

    async fn first_and_last(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<(Self::Key, Self::Key)>> {
        StoreMetricsMiddleware::<S>::record(
            self.metrics.clone(),
            "first_and_last",
            self.store.first_and_last(left_fencepost, right_fencepost),
        )
        .await
    }

    async fn len(&mut self) -> Result<usize> {
        StoreMetricsMiddleware::<S>::record(self.metrics.clone(), "len", self.store.len()).await
    }
    async fn is_empty(&mut self) -> Result<bool> {
        StoreMetricsMiddleware::<S>::record(self.metrics.clone(), "is_empty", self.store.is_empty())
            .await
    }
}
