pub mod btreestore;
#[cfg(test)]
pub mod tests;

use std::{
    fmt::Display,
    marker::PhantomData,
    ops::{Add, Range},
};

use anyhow::anyhow;
use async_trait::async_trait;
use ceramic_core::{EventId, Interest, PeerId, RangeOpen};
use serde::{Deserialize, Serialize};
use tracing::{instrument, trace, Level};

use crate::{Client, Error, Metrics, Result, Sha256a};

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
    S: Store<Key = K, Hash = H> + Send + Sync,
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
    S: Store<Key = K, Hash = H> + Send + Sync,
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
        &self,
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
    pub async fn initial_range(&mut self, interest: RangeOpen<K>) -> Result<RangeHash<K, H>> {
        let hash = self
            .store
            .hash_range(&interest.start..&interest.end)
            .await?;
        Ok(RangeHash {
            first: interest.start,
            hash,
            last: interest.end,
        })
    }
    /// Processes a range from a remote.
    ///
    /// Reports any new keys and what the range indicates about how the local and remote node are
    /// synchronized.
    #[instrument(skip(self), ret(level = Level::DEBUG))]
    pub async fn process_range(&mut self, range: RangeHash<K, H>) -> Result<SyncState<K, H>> {
        let calculated_hash = self.store.hash_range(&range.first..&range.last).await?;
        if calculated_hash == range.hash {
            Ok(SyncState::Synchronized { range })
        } else if calculated_hash.hash.is_zero() {
            Ok(SyncState::Unsynchronized {
                ranges: vec![RangeHash {
                    first: range.first,
                    hash: HashCount::default(),
                    last: range.last,
                }],
            })
        } else if range.hash.hash.is_zero()
            || (!range.first.is_fencepost() && range.hash.count == 1)
        {
            // Remote does not have any data (except for possibly the first key) in the range.

            // Its also possible that locally we do not have the first key in the range.
            // As an optmization return two ranges if the first bound in the range is not a
            // fencepost and is not a key we have. The ranges are:
            //
            //      1. A range containing only the first key, will be zero since we do not have the
            //         key.
            //      2. A range containing the rest of the keys
            //
            // The first range will trigger the remote to send the key and its value. The second
            // range will ensure we are synchronized.
            if range.first.is_fencepost() || calculated_hash.count == 0 {
                Ok(SyncState::RemoteMissing {
                    ranges: vec![RangeHash {
                        first: range.first,
                        hash: calculated_hash,
                        last: range.last,
                    }],
                })
            } else {
                // Get the first key in the range, should always exist since we checked the count
                // in the range already.
                let split_key = self
                    .store
                    .range(&range.first..&range.last, 0, 1)
                    .await?
                    .next()
                    .ok_or_else(|| {
                        Error::new_fatal(anyhow!(
                            "unreachable, at least one key should exist in range given the conditional guard above"
                        ))
                    })?;
                if split_key == range.first {
                    // We already have the bounding key no need to split
                    Ok(SyncState::RemoteMissing {
                        ranges: vec![RangeHash {
                            first: range.first,
                            hash: calculated_hash,
                            last: range.last,
                        }],
                    })
                } else {
                    // We do not have the bounding key...
                    Ok(SyncState::RemoteMissing {
                        ranges: vec![
                            // Send range indicating we are missing the bounding key
                            RangeHash {
                                first: range.first,
                                hash: HashCount::default(),
                                last: split_key.clone(),
                            },
                            // Send range of everything else past the bounding key
                            RangeHash {
                                first: split_key.clone(),
                                hash: self.store.hash_range(&split_key..&range.last).await?,
                                last: range.last,
                            },
                        ],
                    })
                }
            }
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

            Ok(SyncState::Unsynchronized {
                ranges: self.compute_splits(range, calculated_hash.count).await?,
            })
        }
    }

    #[instrument(skip(self), fields(range, count), ret(level = Level::DEBUG))]
    async fn compute_splits(
        &mut self,
        range: RangeHash<K, H>,
        count: u64,
    ) -> Result<Vec<RangeHash<K, H>>> {
        // If the number of keys in a range is <= SPLIT_THRESHOLD then directly send all the keys.
        // TODO: Remove threshold and just use a large N-ary split for all cases
        const SPLIT_THRESHOLD: u64 = 4;

        if count <= SPLIT_THRESHOLD {
            trace!(count, ?range, "small split sending all keys");
            // We have only a few keys in the range. Let's short circuit the roundtrips and
            // send the keys directly.
            let keys: Vec<K> = self
                .store
                .range(&range.first..&range.last, 0, usize::MAX)
                .await?
                .collect();

            let mut ranges = Vec::with_capacity(keys.len() + 1);
            let mut prev: Option<K> = None;
            for key in keys {
                if let Some(prev) = prev {
                    // Push range for each intermediate key.
                    let hash = if !prev.is_fencepost() {
                        HashCount::new(H::digest(&prev), 1)
                    } else {
                        HashCount::default()
                    };
                    ranges.push(RangeHash {
                        first: prev,
                        hash,
                        last: key.clone(),
                    });
                } else if range.first != key {
                    // respond with initial fencepost to first key range is empty
                    ranges.push(RangeHash {
                        first: range.first.clone(),
                        hash: HashCount::default(),
                        last: key.clone(),
                    })
                }
                prev = Some(key);
            }
            if let Some(prev) = prev {
                // Push last key in range.
                let hash = if !prev.is_fencepost() {
                    HashCount::new(H::digest(&prev), 1)
                } else {
                    HashCount::default()
                };
                ranges.push(RangeHash {
                    first: prev,
                    hash,
                    last: range.last,
                });
            }
            Ok(ranges)
        } else {
            // Split the range in two
            let mid_key = self.store.middle(&range.first..&range.last).await?;
            trace!(?mid_key, "splitting on key");
            if let Some(mid_key) = mid_key {
                let first_half = self.store.hash_range(&range.first..&mid_key).await?;
                let last_half = self.store.hash_range(&mid_key..&range.last).await?;
                Ok(vec![
                    RangeHash {
                        first: range.first,
                        hash: first_half,
                        last: mid_key.clone(),
                    },
                    RangeHash {
                        first: mid_key,
                        hash: last_half,
                        last: range.last,
                    },
                ])
            } else {
                Err(Error::new_app(anyhow!("unable to find a split key")))
            }
        }
    }

    /// Retrieve a value associated with a recon key
    pub async fn value_for_key(&self, key: K) -> Result<Option<Vec<u8>>> {
        self.store.value_for_key(&key).await
    }

    /// Insert key into the key space. Includes an optional value.
    /// Returns a boolean (true) indicating if the key was new.
    pub async fn insert(&self, item: &ReconItem<'_, K>) -> Result<bool> {
        self.store.insert(item).await
    }

    /// Insert many keys into the key space. Includes an optional value for each key.
    /// Returns an array with a boolean for each key indicating if the key was new.
    /// The order is the same as the order of the keys. True means new, false means not new.
    pub async fn insert_many(&self, items: &[ReconItem<'_, K>]) -> Result<Vec<bool>> {
        let result = self.store.insert_many(items).await?;

        Ok(result.keys)
    }

    /// Reports total number of keys
    pub async fn len(&self) -> Result<usize> {
        self.store.len().await
    }

    /// Reports if the set is empty
    pub async fn is_empty(&self) -> Result<bool> {
        self.store.is_empty().await
    }

    /// Return all keys in the range between left_fencepost and right_fencepost.
    /// The upper range bound is exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    pub async fn range(
        &self,
        left_fencepost: &K,
        right_fencepost: &K,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = K> + Send + 'static>> {
        self.store
            .range(left_fencepost..right_fencepost, offset, limit)
            .await
    }

    /// Return all keys and values in the range between left_fencepost and right_fencepost.
    /// The upper range bound is exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    pub async fn range_with_values(
        &self,
        left_fencepost: &K,
        right_fencepost: &K,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (K, Vec<u8>)> + Send + 'static>> {
        self.store
            .range_with_values(left_fencepost..right_fencepost, offset, limit)
            .await
    }

    /// Return all keys.
    pub async fn full_range(&self) -> Result<Box<dyn Iterator<Item = K> + Send + 'static>> {
        self.store.full_range().await
    }

    /// Return the interests
    pub async fn interests(&self) -> Result<Vec<RangeOpen<K>>> {
        self.interests.interests().await
    }
}

/// A hash with a count of how many values produced the hash.
#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HashCount<H> {
    hash: H,
    count: u64,
}

impl<H> HashCount<H> {
    /// Construct a new HashCount
    pub fn new(hash: H, count: u64) -> Self {
        Self { hash, count }
    }

    /// The hash of the values.
    pub fn hash(&self) -> &H {
        &self.hash
    }

    /// The number of values that produced the hash.
    pub fn count(&self) -> u64 {
        self.count
    }
}

impl<H> Add<Self> for HashCount<H>
where
    H: AssociativeHash,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self::Output {
            hash: self.hash + rhs.hash,
            count: self.count + rhs.count,
        }
    }
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

impl<H> Display for HashCount<H>
where
    H: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.hash, self.count)
    }
}

/// A key value pair to store
#[derive(Clone, Debug)]
pub struct ReconItem<'a, K>
where
    K: Key,
{
    /// The key.
    pub key: &'a K,
    /// The value, if not set no value is stored.
    pub value: &'a [u8],
}

impl<'a, K> ReconItem<'a, K>
where
    K: Key,
{
    /// Construct a new item with a key and optional value
    pub fn new(key: &'a K, value: &'a [u8]) -> Self {
        Self { key, value }
    }
}

/// The result of an insert operation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct InsertResult {
    /// A true/false list indicating whether or not the key was new.
    /// It is in the same order as the input list of keys.
    pub keys: Vec<bool>,
}

impl InsertResult {
    /// Construct an insert result
    pub fn new(new_keys: Vec<bool>) -> Self {
        Self { keys: new_keys }
    }

    /// true if any key is new, false otherwise
    pub fn included_new_key(&self) -> bool {
        self.keys.iter().any(|new| *new)
    }
}

/// Store defines the API needed to store the Recon set.
#[async_trait]
pub trait Store {
    /// Type of the Key being stored.
    type Key: Key;
    /// Type of the AssociativeHash to compute over keys.
    type Hash: AssociativeHash;

    /// Insert a new key into the key space. Returns true if the key did not exist.
    /// The value will be updated if included
    async fn insert(&self, item: &ReconItem<'_, Self::Key>) -> Result<bool>;

    /// Insert new keys into the key space.
    /// Returns true for each key if it did not previously exist, in the
    /// same order as the input iterator.
    async fn insert_many(&self, items: &[ReconItem<'_, Self::Key>]) -> Result<InsertResult>;

    /// Return the hash of all keys in the range between left_fencepost and right_fencepost.
    /// The upper range bound is exclusive.
    /// Returns Result<(Hash, count), Err>
    async fn hash_range(&self, range: Range<&Self::Key>) -> Result<HashCount<Self::Hash>>;

    /// Return all keys in the range.
    /// The upper range bound is exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    async fn range(
        &self,
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>>;

    /// Return all keys and values in the range between left_fencepost and right_fencepost.
    /// The upper range bound is exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    async fn range_with_values(
        &self,
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>>;

    /// Return all keys.
    async fn full_range(&self) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        self.range(
            &Self::Key::min_value()..&Self::Key::max_value(),
            0,
            usize::MAX,
        )
        .await
    }

    /// Return a key that is approximately in the middle of the range.
    /// An exact middle is not necessary but performance will be better with a better approximation.
    ///
    /// The default implementation will count all elements and then find the middle.
    async fn middle(&self, range: Range<&Self::Key>) -> Result<Option<Self::Key>> {
        let count = self.count(range.clone()).await?;
        if count == 0 {
            Ok(None)
        } else {
            Ok(self.range(range, count / 2, 1).await?.next())
        }
    }

    /// Return any number of splits of the range.
    /// An exact split is not necessary but performance will be better with a better approximation.
    ///
    /// The input range bounds are not part of the returned split as they are the
    /// implicit outermost bounds of the split.
    ///
    /// The default implementation uses middle to split the range approximately in two.
    async fn split(&self, range: Range<&Self::Key>) -> Result<Split<Self::Key, Self::Hash>> {
        if let Some(middle) = self.middle(range.clone()).await? {
            let left = self.hash_range(range.start..&middle).await?;
            let right = self.hash_range(&middle..range.end).await?;
            Ok(Split {
                keys: vec![middle],
                hashes: vec![left, right],
            })
        } else {
            // No keys in range return empty split
            Ok(Split {
                keys: vec![],
                hashes: vec![HashCount {
                    hash: Self::Hash::identity(),
                    count: 0,
                }],
            })
        }
    }

    /// Return the number of keys within the range.
    async fn count(&self, range: Range<&Self::Key>) -> Result<usize> {
        Ok(self.range(range, 0, usize::MAX).await?.count())
    }

    /// Reports total number of keys
    async fn len(&self) -> Result<usize> {
        self.count(&Self::Key::min_value()..&Self::Key::max_value())
            .await
    }
    /// Reports of there are no keys stored.
    async fn is_empty(&self) -> Result<bool> {
        Ok(self.len().await? == 0)
    }

    /// Reports the value for the key
    async fn value_for_key(&self, key: &Self::Key) -> Result<Option<Vec<u8>>>;
}

#[async_trait::async_trait]
impl<K, H, S> Store for std::sync::Arc<S>
where
    K: Key,
    H: AssociativeHash,
    S: Store<Key = K, Hash = H> + Send + Sync,
{
    type Key = K;
    type Hash = H;

    async fn insert(&self, item: &ReconItem<'_, Self::Key>) -> Result<bool> {
        self.as_ref().insert(item).await
    }

    async fn insert_many(&self, items: &[ReconItem<'_, Self::Key>]) -> Result<InsertResult> {
        self.as_ref().insert_many(items).await
    }

    async fn hash_range(&self, range: Range<&Self::Key>) -> Result<HashCount<Self::Hash>> {
        self.as_ref().hash_range(range).await
    }

    async fn range(
        &self,
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        self.as_ref().range(range, offset, limit).await
    }

    async fn range_with_values(
        &self,
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> Result<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        self.as_ref().range_with_values(range, offset, limit).await
    }

    async fn value_for_key(&self, key: &Self::Key) -> Result<Option<Vec<u8>>> {
        self.as_ref().value_for_key(key).await
    }
}

/// Store for Interests
#[async_trait::async_trait]
pub trait InterestStore: Store<Key = Interest, Hash = Sha256a> {}

/// Store for EventId
#[async_trait::async_trait]
pub trait EventIdStore: Store<Key = EventId, Hash = Sha256a> {}

/// Represents a key that can be reconciled via Recon.
pub trait Key:
    TryFrom<Vec<u8>> + Ord + Clone + Display + std::fmt::Debug + Send + Sync + 'static
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
            .with_sep_key(sort_key)
            .with_peer_id(&peer_id)
            .with_min_range()
            .build_fencepost();
        let end = Interest::builder()
            .with_sep_key(sort_key)
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
                if let Some(RangeOpen { start, end }) = interest.range() {
                    let start = EventId::try_from(start).map_err(|e| Error::new_app(anyhow!(e)))?;
                    let end = EventId::try_from(end).map_err(|e| Error::new_app(anyhow!(e)))?;
                    let range = (start, end).into();
                    Ok(range)
                } else {
                    Err(Error::new_app(anyhow!(
                        "stored interest does not contain a range"
                    )))
                }
            })
            .collect::<Result<Vec<RangeOpen<EventId>>>>()
    }
}

/// Represents a synchronization unit, a pair of keys and the hash of values between the keys
/// (exclusive of the keys).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RangeHash<K, H> {
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

impl<K, H> From<RangeHash<K, H>> for RangeOpen<K> {
    fn from(value: RangeHash<K, H>) -> Self {
        Self {
            start: value.first,
            end: value.last,
        }
    }
}

impl<K, H> From<RangeHash<K, H>> for Range<K> {
    fn from(value: RangeHash<K, H>) -> Self {
        Self {
            start: value.first,
            end: value.last,
        }
    }
}

/// Represent a sequence of ranges where the end key is the start key of the subsequent range.
#[derive(Debug)]
pub struct Split<K, H> {
    /// They keys split the hashses, as such there are always one less key than hash.
    pub keys: Vec<K>,
    ///  The hashes of the split ranges,where the outer hashes are the implicitly bounded by the
    ///  bounds used to compute the split.
    pub hashes: Vec<HashCount<H>>,
}

/// Enumerates the possible synchronization states between local and remote peers.
///
/// Recon the algorithm is a recursive algorithm between two peers.
/// The SyncState represents the base cases and intermediate states of the recursive process.
///
/// In short the Recon algorithm descends the key space tree until we discover that the range is
/// synchronized, this is the base case.
///
/// Along the way if we discover any ranges where the remote is missing keys we send over those keys.
///
/// In the limit we descend the tree until only a single key is in each range, send the missing key
/// and then determine that range is synchronized.
///
/// Otherwise the range is unsynchronized, we split the key space and recurse.
#[derive(Debug)]
pub enum SyncState<K, H> {
    /// The local is synchronized with the remote.
    Synchronized {
        /// The range and hash of the synchronized range
        range: RangeHash<K, H>,
    },
    /// The remote range is missing all data in the range.
    RemoteMissing {
        /// The ranges of the local data.
        /// Often, as an optmization, this is split into two ranges one including only the first
        /// key in the range and then another for the remaining keys.
        ranges: Vec<RangeHash<K, H>>,
    },
    /// The local is out of sync with the remote.
    Unsynchronized {
        /// New set of ranges to deliver to the remote.
        /// Often these are a split of the previous range or a zero if no local data was found.
        ranges: Vec<RangeHash<K, H>>,
    },
}

impl Key for EventId {
    fn min_value() -> Self {
        EventId::builder().build_min_fencepost()
    }

    fn max_value() -> Self {
        EventId::builder().build_max_fencepost()
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
        Interest::builder().build_min_fencepost()
    }

    fn max_value() -> Self {
        Interest::builder().build_max_fencepost()
    }

    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }

    fn is_fencepost(&self) -> bool {
        // An interest is only complete if it contains all values up to the not_after value.
        self.not_after().is_none()
    }
}
