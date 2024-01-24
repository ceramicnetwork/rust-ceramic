pub mod btreestore;
pub mod sqlitestore;
pub mod store_metrics;
#[cfg(test)]
pub mod tests;

use std::{fmt::Display, marker::PhantomData};

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use ceramic_core::{EventId, Interest, PeerId, RangeOpen};
use ceramic_metrics::Recorder;
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{
    metrics::{KeyInsertEvent, ValueInsertEvent},
    Client, Metrics, Sha256a,
};

/// Recon is a protocol for set reconciliation via a message passing paradigm.
/// An initial message can be created and then messages are exchanged between two Recon instances
/// until their sets are reconciled.
///
/// Recon is
/// short for reconnaissance and
/// short for set reconciliation based synchronization.
///
///
/// Recon is generic over its Key, Hash and Store implementations.
/// This type provides the core protocol implementation.
#[derive(Debug)]
pub struct Recon<K, H, S, I>
where
    K: Key,
    H: AssociativeHash,
    S: Store<Key = K, Hash = H> + Send,
    I: InterestProvider<Key = K>,
{
    interests: I,
    store: S,
    // allow metrics to be easily cloned
    pub(crate) metrics: Metrics,
}

impl<K, H, S, I> Recon<K, H, S, I>
where
    K: Key,
    H: AssociativeHash,
    S: Store<Key = K, Hash = H> + Send,
    I: InterestProvider<Key = K>,
{
    /// Construct a new Recon instance.
    pub fn new(store: S, interests: I, metrics: Metrics) -> Self {
        Self {
            store,
            interests,
            metrics,
        }
    }

    /// Compute the intersection of the remote interests with the local interests.
    pub async fn process_interests(
        &mut self,
        remote_interests: &[RangeOpen<K>],
    ) -> Result<Vec<RangeOpen<K>>> {
        // Find the intersection of interests.
        // Then reply with a message per intersection.
        //
        // TODO: This is O(n^2) over the number of interests.
        // We should make this more efficient in the future.
        // Potentially we could use a variant of https://en.wikipedia.org/wiki/Bounding_volume_hierarchy
        // to quickly find intersections.
        let mut intersections = Vec::with_capacity(remote_interests.len() * 2);
        for local_range in self.interests().await? {
            for remote_range in remote_interests {
                if let Some(intersection) = local_range.intersect(remote_range) {
                    intersections.push(intersection)
                }
            }
        }
        Ok(intersections)
    }
    /// Compute the hash of the keys within the range.
    pub async fn initial_range(&mut self, interest: RangeOpen<K>) -> Result<Range<K, H>> {
        let hash = self
            .store
            .hash_range(&interest.start, &interest.end)
            .await?;
        Ok(Range {
            first: interest.start,
            hash,
            last: interest.end,
        })
    }
    /// Processes a range from a remote.
    ///
    /// Reports any new keys and what the range indicates about how the local and remote node are
    /// synchronized.
    pub async fn process_range(&mut self, range: Range<K, H>) -> Result<(SyncState<K, H>, Vec<K>)> {
        let mut should_add = Vec::with_capacity(2);
        let mut new_keys = Vec::with_capacity(2);

        if !range.first.is_fencepost() {
            should_add.push(range.first.clone());
        }

        if !range.last.is_fencepost() {
            should_add.push(range.last.clone());
        }

        if !should_add.is_empty() {
            let new = self
                .insert_many(should_add.iter().map(|key| ReconItem::new_key(key)))
                .await?;
            debug_assert_eq!(
                new.len(),
                should_add.len(),
                "new and should_add must be same length"
            );
            for (idx, key) in should_add.into_iter().enumerate() {
                if new[idx] {
                    new_keys.push(key);
                }
            }
        }

        let calculated_hash = self.store.hash_range(&range.first, &range.last).await?;

        if calculated_hash == range.hash {
            Ok((SyncState::Synchronized, new_keys))
        } else if calculated_hash.hash.is_zero() {
            Ok((
                SyncState::Unsynchronized {
                    ranges: vec![Range {
                        first: range.first,
                        hash: H::identity().into(),
                        last: range.last,
                    }],
                },
                new_keys,
            ))
        } else if range.hash.hash.is_zero() {
            Ok((
                SyncState::RemoteMissing {
                    range: Range {
                        first: range.first,
                        hash: calculated_hash,
                        last: range.last,
                    },
                },
                new_keys,
            ))
        } else {
            // We disagree on the hash for range.
            // Split the range.
            trace!(
                ?range.first,
                ?range.last,
                ?range.hash,
                ?calculated_hash,
                "splitting",
            );
            Ok((
                SyncState::Unsynchronized {
                    ranges: self.compute_splits(range, calculated_hash.count).await?,
                },
                new_keys,
            ))
        }
    }

    async fn compute_splits(&mut self, range: Range<K, H>, count: u64) -> Result<Vec<Range<K, H>>> {
        // If the number of keys in a range is <= SPLIT_THRESHOLD then directly send all the keys.
        const SPLIT_THRESHOLD: u64 = 4;

        if count <= SPLIT_THRESHOLD {
            trace!(count, "small split sending all keys");
            // We have only a few keys in the range. Let's short circuit the roundtrips and
            // send the keys directly.
            let keys: Vec<K> = self
                .store
                .range(&range.first, &range.last, 0, usize::MAX)
                .await?
                .collect();

            let mut ranges = Vec::with_capacity(keys.len() + 1);
            let mut prev = None;
            for key in keys {
                if let Some(prev) = prev {
                    // Push range for each intermediate key.
                    ranges.push(Range {
                        first: prev,
                        hash: H::identity().into(),
                        last: key.clone(),
                    });
                } else {
                    // Push first key in range.
                    ranges.push(Range {
                        first: range.first.clone(),
                        hash: H::identity().into(),
                        last: key.clone(),
                    });
                }
                prev = Some(key);
            }
            if let Some(prev) = prev {
                // Push last key in range.
                ranges.push(Range {
                    first: prev,
                    hash: H::identity().into(),
                    last: range.last,
                });
            }
            Ok(ranges)
        } else {
            // Split the range in two
            let mid_key = self.store.middle(&range.first, &range.last).await?;
            trace!(?mid_key, "splitting on key");
            if let Some(mid_key) = mid_key {
                let first_half = self.store.hash_range(&range.first, &mid_key).await?;
                let last_half = self.store.hash_range(&mid_key, &range.last).await?;
                Ok(vec![
                    Range {
                        first: range.first,
                        hash: first_half,
                        last: mid_key.clone(),
                    },
                    Range {
                        first: mid_key,
                        hash: last_half,
                        last: range.last,
                    },
                ])
            } else {
                bail!("unable to find a split key")
            }
        }
    }

    /// Retrieve a value associated with a recon key
    pub async fn value_for_key(&mut self, key: K) -> Result<Option<Vec<u8>>> {
        self.store.value_for_key(&key).await
    }

    /// Insert key into the key space. Includes an optional value.
    /// Returns a boolean (true) indicating if the key was new.
    pub async fn insert(&mut self, item: ReconItem<'_, K>) -> Result<bool> {
        let new_val = item.value.is_some();
        let new = self.store.insert(item).await?;

        if new {
            self.metrics.record(&KeyInsertEvent { cnt: 1 });
        }
        if new_val {
            self.metrics.record(&ValueInsertEvent { cnt: 1 });
        }

        Ok(new)
    }

    /// Insert many keys into the key space. Includes an optional value for each key.
    /// Returns an array with a boolean for each key indicating if the key was new.
    /// The order is the same as the order of the keys. True means new, false means not new.
    pub async fn insert_many<'a, IT>(&mut self, items: IT) -> Result<Vec<bool>>
    where
        IT: ExactSizeIterator<Item = ReconItem<'a, K>> + Send + Sync,
    {
        let result = self.store.insert_many(items).await?;
        let key_cnt = result.keys.iter().filter(|k| **k).count();

        self.metrics.record(&KeyInsertEvent {
            cnt: key_cnt as u64,
        });
        self.metrics.record(&ValueInsertEvent {
            cnt: result.value_count as u64,
        });

        Ok(result.keys)
    }

    /// Reports total number of keys
    pub async fn len(&mut self) -> Result<usize> {
        self.store.len().await
    }

    /// Reports if the set is empty
    pub async fn is_empty(&mut self) -> Result<bool> {
        self.store.is_empty().await
    }

    /// Return all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    pub async fn range(
        &mut self,
        left_fencepost: &K,
        right_fencepost: &K,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = K> + Send + 'static>> {
        self.store
            .range(left_fencepost, right_fencepost, offset, limit)
            .await
    }

    /// Return all keys.
    pub async fn full_range(&mut self) -> Result<Box<dyn Iterator<Item = K> + Send + 'static>> {
        self.store.full_range().await
    }

    /// Return the interests
    pub async fn interests(&self) -> Result<Vec<RangeOpen<K>>> {
        self.interests.interests().await
    }
}

/// A hash with a count of how many values produced the hash.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct HashCount<H> {
    hash: H,
    count: u64,
}

impl<H> std::fmt::Debug for HashCount<H>
where
    H: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("HashCount")
                .field("hash", &self.hash)
                .field("count", &self.count)
                .finish()
        } else {
            write!(f, "{:?}#{}", self.hash, self.count)
        }
    }
}

impl<H> std::fmt::Display for HashCount<H>
where
    H: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.hash, self.count)
    }
}

impl<H> From<H> for HashCount<H> {
    fn from(value: H) -> Self {
        Self {
            hash: value,
            count: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReconItem<'a, K>
where
    K: Key,
{
    pub key: &'a K,
    pub value: Option<&'a [u8]>,
}

impl<'a, K> ReconItem<'a, K>
where
    K: Key,
{
    pub fn new(key: &'a K, value: Option<&'a [u8]>) -> Self {
        Self { key, value }
    }

    pub fn new_key(key: &'a K) -> Self {
        Self { key, value: None }
    }

    pub fn new_with_value(key: &'a K, value: &'a [u8]) -> Self {
        Self {
            key,
            value: Some(value),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InsertResult {
    /// A true/false list indicating whether or not the key was new.
    /// It is in the same order as the input list of keys.
    pub keys: Vec<bool>,
    pub value_count: usize,
}

impl InsertResult {
    pub fn new(new_keys: Vec<bool>, value_count: usize) -> Self {
        Self {
            keys: new_keys,
            value_count,
        }
    }

    /// true if any key is new, false otherwise
    pub fn included_new_key(&self) -> bool {
        self.keys.iter().any(|new| *new)
    }
}

/// Store defines the API needed to store the Recon set.
#[async_trait]
pub trait Store: std::fmt::Debug {
    /// Type of the Key being stored.
    type Key: Key;
    /// Type of the AssociativeHash to compute over keys.
    type Hash: AssociativeHash;

    /// Insert a new key into the key space. Returns true if the key did not exist.
    /// The value will be updated if included
    async fn insert(&mut self, item: ReconItem<'_, Self::Key>) -> Result<bool>;

    /// Insert new keys into the key space.
    /// Returns true for each key if it did not previously exist, in the
    /// same order as the input iterator.
    async fn insert_many<'a, I>(&mut self, items: I) -> Result<InsertResult>
    where
        I: ExactSizeIterator<Item = ReconItem<'a, Self::Key>> + Send + Sync;

    /// Return the hash of all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    /// Returns Result<(Hash, count), Err>
    async fn hash_range(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<HashCount<Self::Hash>>;

    /// Return all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    async fn range(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>>;

    /// Return all keys.
    async fn full_range(&mut self) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        self.range(
            &Self::Key::min_value(),
            &Self::Key::max_value(),
            0,
            usize::MAX,
        )
        .await
    }

    /// Return a key that is approximately in the middle of the range.
    /// An exact middle is not necessary but performance will be better with a better approximation.
    ///
    /// The default implementation will count all elements and then find the middle.
    async fn middle(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        let count = self.count(left_fencepost, right_fencepost).await?;
        if count == 0 {
            Ok(None)
        } else {
            Ok(self
                .range(left_fencepost, right_fencepost, (count - 1) / 2, 1)
                .await?
                .next())
        }
    }
    /// Return the number of keys within the range.
    async fn count(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<usize> {
        Ok(self
            .range(left_fencepost, right_fencepost, 0, usize::MAX)
            .await?
            .count())
    }
    /// Return the first key within the range.
    async fn first(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        Ok(self
            .range(left_fencepost, right_fencepost, 0, 1)
            .await?
            .next())
    }
    /// Return the last key within the range.
    async fn last(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<Self::Key>> {
        Ok(self
            .range(left_fencepost, right_fencepost, 0, usize::MAX)
            .await?
            .last())
    }

    /// Return the first and last keys within the range.
    /// If the range contains only a single key it will be returned as both first and last.
    async fn first_and_last(
        &mut self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Result<Option<(Self::Key, Self::Key)>> {
        let mut range = self
            .range(left_fencepost, right_fencepost, 0, usize::MAX)
            .await?;
        let first = range.next();
        if let Some(first) = first {
            if let Some(last) = range.last() {
                Ok(Some((first, last)))
            } else {
                Ok(Some((first.clone(), first)))
            }
        } else {
            Ok(None)
        }
    }

    /// Reports total number of keys
    async fn len(&mut self) -> Result<usize> {
        self.count(&Self::Key::min_value(), &Self::Key::max_value())
            .await
    }
    /// Reports of there are no keys stored.
    async fn is_empty(&mut self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// value_for_key returns
    /// Ok(Some(value)) if stored,
    /// Ok(None) if not stored, and
    /// Err(e) if retrieving failed.
    async fn value_for_key(&mut self, key: &Self::Key) -> Result<Option<Vec<u8>>>;
}

/// Represents a key that can be reconciled via Recon.
pub trait Key:
    From<Vec<u8>> + Ord + Clone + Display + std::fmt::Debug + Send + Sync + 'static
{
    /// Produce the key that is less than all other keys.
    fn min_value() -> Self;

    /// Produce the key that is greater than all other keys.
    fn max_value() -> Self;

    /// Produce a byte representation of the key.
    fn as_bytes(&self) -> &[u8];

    /// Construct a hex encoded representation of the key.
    fn to_hex(&self) -> String {
        hex::encode_upper(self.as_bytes())
    }
    /// Report if this key is a fencepost and not an actual key
    fn is_fencepost(&self) -> bool;
}

/// Associative hash function that is 32 bytes long.
///
/// Associativity means the order in which items are hashed is unimportant and that hashes can be
/// accumulated by summing.
pub trait AssociativeHash:
    std::ops::Add<Output = Self>
    + Clone
    + Default
    + PartialEq
    + std::fmt::Debug
    + Display
    + From<[u32; 8]>
    + Send
    + Sync
    + 'static
{
    /// The value that when added to the Hash it does not change
    fn identity() -> Self {
        Self::default()
    }

    /// Create new hash from a key.
    fn digest<K: Key>(key: &K) -> Self;

    /// Tests if the hash is the identity
    fn is_zero(&self) -> bool {
        self == &Self::identity()
    }

    /// Sum a iterator of keys
    fn digest_many<'a, I, K>(self, keys: I) -> Self
    where
        I: Iterator<Item = MaybeHashedKey<'a, K, Self>>,
        Self: 'a,
        K: Key + 'a,
    {
        keys.fold(self, |accumulator, key| accumulator + key.hash())
    }

    /// Return the current bytes of the hash
    fn as_bytes(&self) -> [u8; 32];

    /// Return the current ints of the hash
    fn as_u32s(&self) -> &[u32; 8];

    /// Return the bytes of the hash as a hex encoded string
    fn to_hex(&self) -> String {
        hex::encode_upper(self.as_bytes())
    }
}

// Represents a key where we may already know its hash.
#[derive(Debug)]
pub struct MaybeHashedKey<'a, K, H> {
    key: &'a K,
    hash: Option<&'a H>,
}

impl<'a, K, H> MaybeHashedKey<'a, K, H>
where
    K: Key,
    H: AssociativeHash,
{
    pub fn new(key: &'a K, hash: Option<&'a H>) -> Self {
        Self { key, hash }
    }
    /// Report the hash of the key.
    /// If its not already known it will be computed.
    pub fn hash(&self) -> H {
        if let Some(hash) = self.hash {
            hash.clone()
        } else {
            H::digest(self.key)
        }
    }
}

/// InterestProvider describes a set of interests
#[async_trait]
pub trait InterestProvider {
    /// The type of Key over which we are interested.
    type Key: Key;
    /// Report a set of interests.
    async fn interests(&self) -> Result<Vec<RangeOpen<Self::Key>>>;
}

/// InterestProvider that is interested in everything.
#[derive(Debug)]
pub struct FullInterests<K>(PhantomData<K>);

impl<K> Default for FullInterests<K> {
    fn default() -> Self {
        Self(Default::default())
    }
}

#[async_trait]
impl<K: Key> InterestProvider for FullInterests<K> {
    type Key = K;

    async fn interests(&self) -> Result<Vec<RangeOpen<K>>> {
        Ok(vec![(K::min_value(), K::max_value()).into()])
    }
}

/// An implementation of [`InterestProvider`] backed by a Recon instance.
#[derive(Debug)]
pub struct ReconInterestProvider<H = Sha256a>
where
    H: AssociativeHash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>,
{
    start: Interest,
    end: Interest,
    recon: Client<Interest, H>,
}

impl<H> ReconInterestProvider<H>
where
    H: AssociativeHash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>,
{
    /// Construct an [`InterestProvider`] from a Recon [`Client`] and a [`PeerId`].
    pub fn new(peer_id: PeerId, recon: Client<Interest, H>) -> Self {
        let sort_key = "model";
        let start = Interest::builder()
            .with_sort_key(sort_key)
            .with_peer_id(&peer_id)
            .with_min_range()
            .build_fencepost();
        let end = Interest::builder()
            .with_sort_key(sort_key)
            .with_peer_id(&peer_id)
            .with_max_range()
            .build_fencepost();

        Self { start, end, recon }
    }
}

// Implement InterestProvider for a Recon of interests.
#[async_trait]
impl<H> InterestProvider for ReconInterestProvider<H>
where
    H: AssociativeHash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de>,
{
    type Key = EventId;

    async fn interests(&self) -> Result<Vec<RangeOpen<EventId>>> {
        self.recon
            .range(self.start.clone(), self.end.clone(), 0, usize::MAX)
            .await?
            .map(|interest| {
                if let Some(RangeOpen { start, end }) = interest.range()? {
                    let range = (EventId::from(start), EventId::from(end)).into();
                    Ok(range)
                } else {
                    Err(anyhow!("stored interest does not contain a range"))
                }
            })
            .collect::<Result<Vec<RangeOpen<EventId>>>>()
    }
}

/// Represents a synchronization unit, a pair of keys and the hash of values between the keys
/// (exclusive of the keys).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Range<K, H> {
    /// First key in the range
    /// This key may be a fencepost, meaning its not an actual key but simply a boundary.
    pub first: K,
    /// Hash of all keys in between first and last exclusive.
    /// Hash is zero if there are no keys within the range.
    pub hash: HashCount<H>,
    /// Last key in the range,
    /// This key may be a fencepost, meaning its not an actual key but simply a boundary.
    pub last: K,
}

/// Enumerates the possible synchronization states between local and remote peers.
#[derive(Debug)]
pub enum SyncState<K, H> {
    /// The local is synchronized with the remote.
    Synchronized,
    /// The remote range is missing all data in the range.
    RemoteMissing {
        /// The range and hash of the local data the remote is missing.
        range: Range<K, H>,
    },
    /// The local is out of sync with the remote.
    Unsynchronized {
        /// New set of ranges to deliver to the remote.
        /// Often these are a split of the previous range or a zero if no local data was found.
        ranges: Vec<Range<K, H>>,
    },
}

impl Key for EventId {
    fn min_value() -> Self {
        Vec::new().into()
    }

    fn max_value() -> Self {
        // No EventId starts with an 0xFF byte
        vec![0xFF].into()
    }

    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }

    fn is_fencepost(&self) -> bool {
        //An event is complete if it contains all values up to the event CID.
        self.cid().is_none()
    }
}

impl Key for Interest {
    fn min_value() -> Self {
        Vec::new().into()
    }

    fn max_value() -> Self {
        // No Interest starts with an 0xFF byte
        vec![0xFF].into()
    }

    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }

    fn is_fencepost(&self) -> bool {
        // An interest is only complete if it contains all values up to the not_after value.
        self.not_after().is_err()
    }
}
