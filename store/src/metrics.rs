use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use ceramic_core::RangeOpen;
use ceramic_metrics::{register, Recorder};
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
use recon::{AssociativeHash, HashCount, InsertResult, ReconItem};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct StorageQuery {
    pub name: &'static str,
    pub duration: Duration,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum InsertEventType {
    Value,
    Key,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct InsertEvent {
    pub type_: InsertEventType,
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
    key_insert_count: Counter,
    value_insert_count: Counter,

    store_query_durations: Family<QueryLabels, Histogram>,
}

impl Metrics {
    /// Register and construct Metrics
    pub fn register(registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry_with_prefix("ceramic_store");

        register!(
            key_insert_count,
            "Number times a new key is inserted into the datastore",
            Counter::default(),
            sub_registry
        );
        register!(
            value_insert_count,
            "Number times a new value is inserted into the datastore",
            Counter::default(),
            sub_registry
        );

        register!(
            store_query_durations,
            "Durations of store queries in seconds",
            Family::<QueryLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.005, 2.0, 20))
            }),
            sub_registry
        );

        Self {
            key_insert_count,
            value_insert_count,
            store_query_durations,
        }
    }
}

impl Recorder<InsertEvent> for Metrics {
    fn record(&self, event: &InsertEvent) {
        match event.type_ {
            InsertEventType::Value => {
                self.value_insert_count.inc_by(event.cnt);
            }
            InsertEventType::Key => {
                self.key_insert_count.inc_by(event.cnt);
            }
        }
    }
}

impl Recorder<StorageQuery> for Metrics {
    fn record(&self, event: &StorageQuery) {
        let labels: QueryLabels = event.into();
        self.store_query_durations
            .get_or_create(&labels)
            .observe(event.duration.as_secs_f64());
    }
}

/// Implement the Store and record metrics
#[derive(Debug, Clone)]
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
    async fn record<T>(metrics: &Metrics, name: &'static str, fut: impl Future<Output = T>) -> T {
        let start = Instant::now();
        let ret = fut.await;
        let duration = start.elapsed();
        let event = StorageQuery { name, duration };
        metrics.record(&event);
        ret
    }

    fn record_key_insert(&self, new_key: bool, new_val: bool) {
        if new_key {
            self.metrics.record(&InsertEvent {
                type_: InsertEventType::Key,
                cnt: 1,
            });
        }
        if new_val {
            self.metrics.record(&InsertEvent {
                type_: InsertEventType::Value,
                cnt: 1,
            });
        }
    }
}

#[async_trait]
impl<S, K, H> ceramic_api::AccessInterestStore for StoreMetricsMiddleware<S>
where
    S: ceramic_api::AccessInterestStore<Key = K, Hash = H>,
    K: recon::Key,
    H: AssociativeHash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>,
{
    type Key = K;
    type Hash = H;

    async fn insert(&self, key: Self::Key) -> Result<bool> {
        let new = StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "interest_insert",
            self.store.insert(key),
        )
        .await?;
        self.record_key_insert(new, false);
        Ok(new)
    }
    async fn range(
        &self,
        start: Self::Key,
        end: Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Self::Key>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "interest_range",
            self.store.range(start, end, offset, limit),
        )
        .await
    }
}

#[async_trait]
impl<S, K, H> ceramic_api::AccessModelStore for StoreMetricsMiddleware<S>
where
    S: ceramic_api::AccessModelStore<Key = K, Hash = H>,
    K: recon::Key,
    H: AssociativeHash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>,
{
    type Key = K;
    type Hash = H;

    async fn insert(&self, key: Self::Key, value: Option<Vec<u8>>) -> Result<(bool, bool)> {
        let (new_key, new_val) = StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "model_insert",
            self.store.insert(key, value),
        )
        .await?;

        self.record_key_insert(new_key, new_val);
        Ok((new_key, new_val))
    }
    async fn range_with_values(
        &self,
        start: Self::Key,
        end: Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<(Self::Key, Vec<u8>)>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "model_range_with_values",
            self.store.range_with_values(start, end, offset, limit),
        )
        .await
    }

    async fn value_for_key(&self, key: Self::Key) -> Result<Option<Vec<u8>>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "model_value_for_key",
            self.store.value_for_key(key),
        )
        .await
    }

    async fn keys_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
    ) -> anyhow::Result<(i64, Vec<Self::Key>)> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "model_keys_since_highwater_mark",
            self.store.keys_since_highwater_mark(highwater, limit),
        )
        .await
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

    async fn insert(&self, item: ReconItem<'_, Self::Key>) -> Result<bool> {
        let new_val = item.value.is_some();
        let new =
            StoreMetricsMiddleware::<S>::record(&self.metrics, "insert", self.store.insert(item))
                .await?;
        self.record_key_insert(new, new_val);
        Ok(new)
    }

    async fn insert_many<'a, I>(&self, items: I) -> Result<InsertResult>
    where
        I: ExactSizeIterator<Item = ReconItem<'a, K>> + Send + Sync,
    {
        let res = StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "insert_many",
            self.store.insert_many(items),
        )
        .await?;

        let key_cnt = res.keys.iter().filter(|k| **k).count();

        self.metrics.record(&InsertEvent {
            type_: InsertEventType::Key,
            cnt: key_cnt as u64,
        });
        self.metrics.record(&InsertEvent {
            type_: InsertEventType::Value,
            cnt: res.value_count as u64,
        });

        Ok(res)
    }

    async fn hash_range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<HashCount<Self::Hash>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "hash_range",
            self.store.hash_range(left_fencepost, right_fencepost),
        )
        .await
    }

    async fn range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "range",
            self.store
                .range(left_fencepost, right_fencepost, offset, limit),
        )
        .await
    }
    async fn range_with_values(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "range_with_values",
            self.store
                .range_with_values(left_fencepost, right_fencepost, offset, limit),
        )
        .await
    }

    async fn full_range(&self) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        StoreMetricsMiddleware::<S>::record(&self.metrics, "full_range", self.store.full_range())
            .await
    }

    async fn middle(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "middle",
            self.store.middle(left_fencepost, right_fencepost),
        )
        .await
    }
    async fn count(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<usize> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "count",
            self.store.count(left_fencepost, right_fencepost),
        )
        .await
    }
    async fn first(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "first",
            self.store.first(left_fencepost, right_fencepost),
        )
        .await
    }
    async fn last(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "last",
            self.store.last(left_fencepost, right_fencepost),
        )
        .await
    }

    async fn first_and_last(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<(Self::Key, Self::Key)>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "first_and_last",
            self.store.first_and_last(left_fencepost, right_fencepost),
        )
        .await
    }

    async fn len(&self) -> Result<usize> {
        StoreMetricsMiddleware::<S>::record(&self.metrics, "len", self.store.len()).await
    }

    async fn is_empty(&self) -> Result<bool> {
        StoreMetricsMiddleware::<S>::record(&self.metrics, "is_empty", self.store.is_empty()).await
    }

    async fn value_for_key(&self, key: &Self::Key) -> Result<Option<Vec<u8>>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "value_for_key",
            self.store.value_for_key(key),
        )
        .await
    }
    async fn keys_with_missing_values(
        &self,
        range: RangeOpen<Self::Key>,
    ) -> Result<Vec<Self::Key>> {
        StoreMetricsMiddleware::<S>::record(
            &self.metrics,
            "keys_with_missing_values",
            self.store.keys_with_missing_values(range),
        )
        .await
    }
}
