use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
};

use async_trait::async_trait;

use crate::recon::{AssociativeHash, Key, MaybeHashedKey, Store};

/// An implementation of a Store that stores keys in an in-memory BTree
#[derive(Clone, Debug)]
pub struct BTreeStore<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    /// The set of keys and their Sha256a hashes
    keys: BTreeMap<K, H>, // this will be a b#tree at some point in the future
}

impl<K, H> Default for BTreeStore<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    fn default() -> Self {
        Self {
            keys: Default::default(),
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
            keys: BTreeMap::default(),
        };
        for key in s {
            let hash = H::digest(&key);
            r.keys.insert(key, hash);
        }
        r
    }

    /// Return the hash of all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    pub fn hash_range(&self, left_fencepost: &K, right_fencepost: &K) -> anyhow::Result<(H, u64)> {
        if left_fencepost >= right_fencepost {
            return Ok((H::identity(), 0));
        }
        let range = (
            Bound::Excluded(left_fencepost),
            Bound::Excluded(right_fencepost),
        );
        let hash: H = H::identity().digest_many(
            self.keys
                .range(range)
                .map(|(key, hash)| MaybeHashedKey::new(key, Some(hash))),
        );
        let count: usize = self.keys.range(range).count();
        Ok((hash, count as u64))
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
    ) -> anyhow::Result<Box<dyn Iterator<Item = K> + Send + 'static>> {
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

    async fn insert(&mut self, key: &Self::Key) -> anyhow::Result<bool> {
        Ok(self.keys.insert(key.to_owned(), H::digest(key)).is_none())
    }

    async fn hash_range(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> anyhow::Result<(Self::Hash, u64)> {
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
    ) -> anyhow::Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        // Self does not need async to implement range, so it exposes a pub non async range function
        // and we delegate to its implementation here.
        BTreeStore::range(self, left_fencepost, right_fencepost, offset, limit)
    }

    async fn last(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> anyhow::Result<Option<Self::Key>> {
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
    ) -> anyhow::Result<Option<(Self::Key, Self::Key)>> {
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
}
