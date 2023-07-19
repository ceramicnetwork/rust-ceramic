use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
};

use crate::recon::{AssociativeHash, Key, MaybeHashedKey, Store};

/// An implemetation of a Store that stores keys in an in-memory BTree
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
}

impl<K, H> Store for BTreeStore<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    type Key = K;
    type Hash = H;

    fn insert(&mut self, key: &Self::Key) -> anyhow::Result<bool> {
        Ok(self.keys.insert(key.to_owned(), H::digest(key)).is_none())
    }

    fn hash_range(&self, left_fencepost: &Self::Key, right_fencepost: &Self::Key) -> Self::Hash {
        if left_fencepost >= right_fencepost {
            return H::identity();
        }
        let range = (
            Bound::Excluded(left_fencepost),
            Bound::Excluded(right_fencepost),
        );
        H::identity().digest_many(
            self.keys
                .range(range)
                .map(|(key, hash)| MaybeHashedKey::new(key, Some(hash))),
        )
    }

    fn range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Box<dyn Iterator<Item = &Self::Key> + '_> {
        let range = (
            Bound::Excluded(left_fencepost),
            Bound::Excluded(right_fencepost),
        );
        Box::new(
            self.keys
                .range(range)
                .skip(offset)
                .take(limit)
                .map(|(key, _hash)| key),
        )
    }

    fn last(&self, left_fencepost: &Self::Key, right_fencepost: &Self::Key) -> Option<&Self::Key> {
        let range = (
            Bound::Excluded(left_fencepost),
            Bound::Excluded(right_fencepost),
        );
        self.keys.range(range).next_back().map(|(k, _)| k)
    }

    fn first_and_last(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Option<(&Self::Key, &Self::Key)> {
        let range = (
            Bound::Excluded(left_fencepost),
            Bound::Excluded(right_fencepost),
        );
        let mut range = self.keys.range(range);
        let first = range.next().map(|(k, _)| k);
        if let Some(first) = first {
            if let Some(last) = range.next_back().map(|(k, _)| k) {
                Some((first, last))
            } else {
                Some((first, first))
            }
        } else {
            None
        }
    }
}
