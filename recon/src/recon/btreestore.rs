use crate::recon::{AssociativeHash, Key, MaybeHashedKey, Store};
use anyhow::Result;
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
};

/// An implementation of a Store that stores keys in an in-memory BTree
#[derive(Clone, Debug)]
pub struct BTreeStore<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    /// The set of keys and their Sha256a hashes
    keys: BTreeMap<K, H>, // this will be a b#tree at some point in the future
    values: BTreeMap<K, Vec<u8>>, // Map from keys to values.
}

impl<K, H> Default for BTreeStore<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    fn default() -> Self {
        Self {
            keys: Default::default(),
            values: Default::default(),
        }
    }
}

impl<K, H> BTreeStore<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    /// make a new recon form a set of Strings
    pub fn from_set(s: BTreeSet<K>) -> Self {
        let mut r = Self {
            keys: Default::default(),
            values: Default::default(),
        };
        for key in s {
            let hash = H::digest(&key);
            r.keys.insert(key, hash);
        }
        r
    }

    /// Return the hash of all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    pub fn hash_range(&self, left_fencepost: &K, right_fencepost: &K) -> Result<H> {
        if left_fencepost >= right_fencepost {
            return Ok(H::identity());
        }
        let range = (
            Bound::Excluded(left_fencepost),
            Bound::Excluded(right_fencepost),
        );
        Ok(H::identity().digest_many(
            self.keys
                .range(range)
                .map(|(key, hash)| MaybeHashedKey::new(key, Some(hash))),
        ))
    }

    /// Return all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    pub fn range(
        &self,
        left_fencepost: &K,
        right_fencepost: &K,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = K> + Send + 'static>> {
        let range = (
            Bound::Excluded(left_fencepost),
            Bound::Excluded(right_fencepost),
        );
        let keys: Vec<K> = self
            .keys
            .range(range)
            .skip(offset)
            .take(limit)
            .map(|(key, _hash)| key)
            .cloned()
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

    async fn insert(&mut self, key: &Self::Key) -> Result<bool> {
        Ok(self.keys.insert(key.to_owned(), H::digest(key)).is_none())
    }

    async fn hash_range(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Self::Hash> {
        // Self does not need async to implement hash_range, so it exposes a pub non async hash_range function
        // and we delegate to its implementation here.
        BTreeStore::hash_range(self, left_fencepost, right_fencepost)
    }

    async fn range(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        // Self does not need async to implement range, so it exposes a pub non async range function
        // and we delegate to its implementation here.
        BTreeStore::range(self, left_fencepost, right_fencepost, offset, limit)
    }

    async fn last(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        let range = (
            Bound::Excluded(left_fencepost),
            Bound::Excluded(right_fencepost),
        );
        Ok(self
            .keys
            .range(range)
            .next_back()
            .map(|(k, _)| k.to_owned()))
    }

    async fn first_and_last(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<(Self::Key, Self::Key)>> {
        let range = (
            Bound::Excluded(left_fencepost),
            Bound::Excluded(right_fencepost),
        );
        let mut range = self.keys.range(range);
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

    /// store_value_for_key returns Some(true) is inserting, Some(false) if present, and Err if store failed.
    async fn store_value_for_key(&mut self, key: &Self::Key, value: &[u8]) -> Result<bool> {
        Ok(self.values.insert(key.clone(), value.to_vec()).is_none())
    }
    /// value_for_key returns an Error is retrieving failed and None if the key is not stored.
    async fn value_for_key(&mut self, key: &Self::Key) -> Result<Option<Vec<u8>>> {
        Ok(self.values.get(key).map(|value| value.clone()))
    }
}
