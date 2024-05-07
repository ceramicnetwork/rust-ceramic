use async_trait::async_trait;
use std::{collections::BTreeMap, ops::Range, sync::Arc};
use tokio::sync::Mutex;

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
    pub async fn from_set(s: BTreeMap<K, Option<Vec<u8>>>) -> Self {
        let r: Self = Default::default();
        {
            let mut inner = r.inner.lock().await;
            for (key, value) in s {
                let hash = H::digest(&key);
                inner.keys.insert(key.clone(), hash);
                if let Some(value) = value {
                    inner.values.insert(key, value);
                }
            }
        }
        r
    }

    /// Return the hash of all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    pub async fn hash_range(&self, range: Range<&K>) -> Result<HashCount<H>> {
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

    /// Return all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    pub async fn range(
        &self,
        range: Range<&K>,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = K> + Send + 'static>> {
        let keys: Vec<K> = self
            .inner
            .lock()
            .await
            .keys
            .range(range)
            .skip(offset)
            .take(limit)
            .map(|(key, _hash)| key)
            .cloned()
            .collect();
        Ok(Box::new(keys.into_iter()))
    }
    /// Return all keys and values in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    pub async fn range_with_values(
        &self,
        range: Range<&K>,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (K, Vec<u8>)> + Send + 'static>> {
        let inner = self.inner.lock().await;

        let keys: Vec<(K, Vec<u8>)> = inner
            .keys
            .range(range)
            .skip(offset)
            .take(limit)
            .filter_map(|(key, _hash)| {
                inner
                    .values
                    .get(key)
                    .map(|value| (key.clone(), value.clone()))
            })
            .collect();
        Ok(Box::new(keys.into_iter()))
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

    async fn insert(&self, item: &ReconItem<'_, Self::Key>) -> Result<bool> {
        let mut inner = self.inner.lock().await;
        let new = inner
            .keys
            .insert(item.key.clone(), H::digest(item.key))
            .is_none();

        inner.values.insert(item.key.clone(), item.value.to_vec());
        Ok(new)
    }

    async fn insert_many(&self, items: &[ReconItem<'_, K>]) -> Result<InsertResult> {
        let mut new = vec![false; items.len()];
        for (idx, item) in items.iter().enumerate() {
            new[idx] = self.insert(item).await?;
        }
        Ok(InsertResult::new(new))
    }

    async fn hash_range(&self, range: Range<&Self::Key>) -> Result<HashCount<Self::Hash>> {
        // Self does not need async to implement hash_range, so it exposes a pub non async hash_range function
        // and we delegate to its implementation here.
        BTreeStore::hash_range(self, range).await
    }

    async fn range(
        &self,
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        // Self does not need async to implement range, so it exposes a pub non async range function
        // and we delegate to its implementation here.
        BTreeStore::range(self, range, offset, limit).await
    }
    async fn range_with_values(
        &self,
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        BTreeStore::range_with_values(self, range, offset, limit).await
    }

    async fn last(&self, range: Range<&Self::Key>) -> Result<Option<Self::Key>> {
        Ok(self
            .inner
            .lock()
            .await
            .keys
            .range(range)
            .next_back()
            .map(|(k, _)| k.to_owned()))
    }

    async fn first_and_last(
        &self,

        range: Range<&Self::Key>,
    ) -> Result<Option<(Self::Key, Self::Key)>> {
        let inner = self.inner.lock().await;
        let mut range = inner.keys.range(range);
        let first = range.next().map(|(k, _)| k);
        if let Some(first) = first {
            if let Some(last) = range.next_back().map(|(k, _)| k) {
                Ok(Some((first.to_owned(), last.to_owned())))
            } else {
                Ok(Some((first.to_owned(), first.to_owned())))
            }
        } else {
            Ok(None)
        }
    }

    /// value_for_key returns an Error is retrieving failed and None if the key is not stored.
    async fn value_for_key(&self, key: &Self::Key) -> Result<Option<Vec<u8>>> {
        Ok(self.inner.lock().await.values.get(key).cloned())
    }
}
