use crate::test_utils::BTreeStore;
use crate::{Key, Metrics, Range, Result, Sha256a, Store, SyncState};
use ceramic_core::RangeOpen;
use prometheus_client::registry::Registry;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub enum MockOrRealStore<K, S>
where
    K: Key + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>,
    S: Store<Key = K, Hash = Sha256a> + Send + Sync,
{
    Mock(Arc<Mutex<S>>),
    Real(BTreeStore<K, Sha256a>),
}

#[async_trait::async_trait]
impl<K, R> crate::protocol::Recon for Arc<Mutex<R>>
where
    K: Key + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>,
    R: crate::protocol::Recon<Key = K, Hash = Sha256a>,
{
    type Key = K;
    type Hash = Sha256a;

    async fn insert(&self, key: Self::Key, value: Option<Vec<u8>>) -> Result<()> {
        self.lock().await.insert(key, value).await
    }

    async fn range(
        &self,
        left_fencepost: Self::Key,
        right_fencepost: Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Self::Key>> {
        self.lock()
            .await
            .range(left_fencepost, right_fencepost, offset, limit)
            .await
    }

    async fn len(&self) -> Result<usize> {
        self.lock().await.len().await
    }

    async fn value_for_key(&self, key: Self::Key) -> Result<Option<Vec<u8>>> {
        self.lock().await.value_for_key(key).await
    }

    async fn keys_with_missing_values(
        &self,
        range: RangeOpen<Self::Key>,
    ) -> Result<Vec<Self::Key>> {
        self.lock().await.keys_with_missing_values(range).await
    }

    async fn interests(&self) -> Result<Vec<RangeOpen<Self::Key>>> {
        self.lock().await.interests().await
    }

    async fn process_interests(
        &self,
        interests: Vec<RangeOpen<Self::Key>>,
    ) -> Result<Vec<RangeOpen<Self::Key>>> {
        self.lock().await.process_interests(interests).await
    }

    async fn initial_range(
        &self,
        interest: RangeOpen<Self::Key>,
    ) -> Result<Range<Self::Key, Sha256a>> {
        self.lock().await.initial_range(interest).await
    }

    async fn process_range(
        &self,
        range: Range<Self::Key, Sha256a>,
    ) -> Result<(SyncState<Self::Key, Sha256a>, Vec<Self::Key>)> {
        self.lock().await.process_range(range).await
    }

    fn metrics(&self) -> Metrics {
        Metrics::register(&mut Registry::default())
    }
}

#[async_trait::async_trait]
impl<K, S> Store for Arc<Mutex<S>>
where
    K: Key + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>,
    S: Store<Key = K, Hash = Sha256a> + Send + Sync,
{
    type Key = K;
    type Hash = Sha256a;

    async fn insert<'a>(&self, item: &crate::ReconItem<'a, Self::Key>) -> Result<bool> {
        self.lock().await.insert(item).await
    }

    async fn insert_many<'a>(
        &self,
        items: &[crate::ReconItem<'a, Self::Key>],
    ) -> Result<crate::InsertResult> {
        self.lock().await.insert_many(items).await
    }

    async fn hash_range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<crate::HashCount<Self::Hash>> {
        self.lock()
            .await
            .hash_range(left_fencepost, right_fencepost)
            .await
    }

    async fn range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        self.lock()
            .await
            .range(left_fencepost, right_fencepost, offset, limit)
            .await
    }

    async fn range_with_values(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        self.lock()
            .await
            .range_with_values(left_fencepost, right_fencepost, offset, limit)
            .await
    }

    async fn value_for_key(&self, key: &Self::Key) -> Result<Option<Vec<u8>>> {
        self.lock().await.value_for_key(key).await
    }

    /// Report all keys in the range that are missing a value.
    async fn keys_with_missing_values(
        &self,
        range: RangeOpen<Self::Key>,
    ) -> Result<Vec<Self::Key>> {
        self.lock().await.keys_with_missing_values(range).await
    }
}

impl<K, S> MockOrRealStore<K, S>
where
    K: Key + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>,
    S: Store<Key = K, Hash = Sha256a> + Send + Sync,
{
    pub fn as_mock(&self) -> Arc<Mutex<S>> {
        match self {
            Self::Mock(store) => store.clone(),
            Self::Real(_) => panic!("Not a mocked store"),
        }
    }
}

#[async_trait::async_trait]
impl<K, S> Store for MockOrRealStore<K, S>
where
    K: Key + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>,
    S: Store<Key = K, Hash = Sha256a> + Send + Sync,
{
    type Key = K;
    type Hash = Sha256a;

    async fn insert<'a>(&self, item: &crate::ReconItem<'a, Self::Key>) -> Result<bool> {
        match self {
            Self::Mock(store) => store.lock().await.insert(item).await,
            Self::Real(store) => store.insert(item).await,
        }
    }

    async fn insert_many<'a>(
        &self,
        items: &[crate::ReconItem<'a, Self::Key>],
    ) -> Result<crate::InsertResult> {
        match self {
            Self::Mock(store) => store.lock().await.insert_many(items).await,
            Self::Real(store) => store.insert_many(items).await,
        }
    }

    async fn hash_range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<crate::HashCount<Self::Hash>> {
        match self {
            Self::Mock(store) => {
                store
                    .lock()
                    .await
                    .hash_range(left_fencepost, right_fencepost)
                    .await
            }
            Self::Real(store) => store.hash_range(left_fencepost, right_fencepost).await,
        }
    }

    async fn range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        match self {
            Self::Mock(store) => {
                store
                    .lock()
                    .await
                    .range(left_fencepost, right_fencepost, offset, limit)
                    .await
            }
            Self::Real(store) => {
                store
                    .range(left_fencepost, right_fencepost, offset, limit)
                    .await
            }
        }
    }

    async fn range_with_values(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        match self {
            Self::Mock(store) => {
                store
                    .lock()
                    .await
                    .range_with_values(left_fencepost, right_fencepost, offset, limit)
                    .await
            }
            Self::Real(store) => {
                store
                    .range_with_values(left_fencepost, right_fencepost, offset, limit)
                    .await
            }
        }
    }

    async fn value_for_key(&self, key: &Self::Key) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Mock(store) => store.lock().await.value_for_key(key).await,
            Self::Real(store) => store.value_for_key(key).await,
        }
    }

    /// Report all keys in the range that are missing a value.
    async fn keys_with_missing_values(
        &self,
        range: RangeOpen<Self::Key>,
    ) -> Result<Vec<Self::Key>> {
        match self {
            Self::Mock(store) => store.lock().await.keys_with_missing_values(range).await,
            Self::Real(store) => store.keys_with_missing_values(range).await,
        }
    }
}
