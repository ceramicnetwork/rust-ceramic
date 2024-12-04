use async_trait::async_trait;
use ceramic_core::NodeId;
use std::{collections::BTreeMap, ops::Range, sync::Arc};
use tokio::sync::Mutex;
use tracing::instrument;

use crate::{
    recon::{AssociativeHash, Key, MaybeHashedKey, ReconItem, Store},
    HashCount, InsertResult, Result,
};

#[derive(Clone, Debug)]
struct BTreeStoreInner<K, H> {
    keys: BTreeMap<K, H>,
    values: BTreeMap<K, Vec<u8>>,
}

/// An implementation of a Store that stores keys in an in-memory BTree
#[derive(Clone, Debug)]
pub struct BTreeStore<K, H> {
    inner: Arc<Mutex<BTreeStoreInner<K, H>>>,
}

impl<K, H> Default for BTreeStore<K, H> {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BTreeStoreInner {
                keys: BTreeMap::new(),
                values: BTreeMap::new(),
            })),
        }
    }
}

impl<K, H> BTreeStore<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    /// make a new recon from a set of keys and values
    pub async fn from_set(s: BTreeMap<K, Vec<u8>>) -> Self {
        let r: Self = Default::default();
        {
            let mut inner = r.inner.lock().await;
            for (key, value) in s {
                let hash = H::digest(&key);
                inner.keys.insert(key.clone(), hash);
                inner.values.insert(key, value);
            }
        }
        r
    }

    async fn hash_range(&self, range: Range<&K>) -> Result<HashCount<H>> {
        if range.start >= range.end {
            return Ok(HashCount {
                hash: H::identity(),
                count: 0,
            });
        }
        let inner = self.inner.lock().await;
        let hash: H = H::identity().digest_many(
            inner
                .keys
                .range(range.clone())
                .map(|(key, hash)| MaybeHashedKey::new(key, Some(hash))),
        );
        let count: usize = inner.keys.range(range).count();
        Ok(HashCount {
            hash,
            count: count as u64,
        })
    }

    async fn range(
        &self,
        range: Range<&K>,
    ) -> Result<Box<dyn Iterator<Item = K> + Send + 'static>> {
        let keys: Vec<K> = self
            .inner
            .lock()
            .await
            .keys
            .range(range)
            .map(|(key, _hash)| key)
            .cloned()
            .collect();
        Ok(Box::new(keys.into_iter()))
    }
    async fn first(&self, range: Range<&K>) -> Result<Option<K>> {
        Ok(self
            .inner
            .lock()
            .await
            .keys
            .range(range)
            .next()
            .map(|(k, _)| k.clone()))
    }

    async fn middle(&self, range: Range<&K>) -> Result<Option<K>> {
        let inner = self.inner.lock().await;
        let count = inner.keys.range(range.clone()).count();
        Ok(inner
            .keys
            .range(range)
            .nth(count / 2)
            .map(|(k, _)| k.clone()))
    }

    async fn insert(&self, item: &ReconItem<K>, _informant: NodeId) -> Result<bool> {
        let mut inner = self.inner.lock().await;
        let new = inner
            .keys
            .insert(item.key.clone(), H::digest(&item.key))
            .is_none();

        inner.values.insert(item.key.clone(), item.value.to_vec());
        Ok(new)
    }
}

#[async_trait]
impl<K, H> Store for BTreeStore<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    type Key = K;
    type Hash = H;

    #[instrument(skip(self))]
    async fn insert_many(
        &self,
        items: &[ReconItem<Self::Key>],
        informant: NodeId,
    ) -> Result<InsertResult<Self::Key>> {
        tracing::trace!("inserting items: {}", items.len());
        let mut new = 0;
        for item in items.iter() {
            self.insert(item, informant).await?.then(|| new += 1);
        }
        Ok(InsertResult::new(new))
    }

    async fn hash_range(&self, range: Range<&Self::Key>) -> Result<HashCount<Self::Hash>> {
        BTreeStore::hash_range(self, range).await
    }

    async fn range(
        &self,
        range: Range<&Self::Key>,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        BTreeStore::range(self, range).await
    }
    async fn first(&self, range: Range<&Self::Key>) -> Result<Option<Self::Key>> {
        BTreeStore::first(self, range).await
    }

    async fn middle(&self, range: Range<&Self::Key>) -> Result<Option<Self::Key>> {
        BTreeStore::middle(self, range).await
    }

    async fn value_for_key(&self, key: &Self::Key) -> Result<Option<Vec<u8>>> {
        Ok(self.inner.lock().await.values.get(key).cloned())
    }
}
