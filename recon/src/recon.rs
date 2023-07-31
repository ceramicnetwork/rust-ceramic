pub mod btree;
pub mod durability;
#[cfg(test)]
pub mod tests;

use std::{
    fmt::Display,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use anyhow::{bail, Result};
use ceramic_core::{EventId, Interest, RangeOpen};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument, trace};

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
    S: Store<Key = K, Hash = H>,
    I: InterestProvider<Key = K>,
{
    interests: I,
    store: S,
}

impl<K, H, S, I> Recon<K, H, S, I>
where
    K: Key,
    H: AssociativeHash,
    S: Store<Key = K, Hash = H>,
    I: InterestProvider<Key = K>,
{
    /// Construct a new Recon instance.
    pub fn new(store: S, interests: I) -> Self {
        Self { store, interests }
    }

    /// Construct a message to send as the first message.
    pub fn initial_messages(&self) -> Result<Vec<Message<K, H>>> {
        let interests = self.interests()?;
        let mut messages = Vec::with_capacity(interests.len());
        for range in interests {
            let mut response = Message::new(&range.start, &range.end);
            if let Some((first, last)) = self.store.first_and_last(&range.start, &range.end) {
                // Cannot use store.len, we need the count between first and last
                let len = self.store.len();
                match len {
                    0 => {
                        // this should be unreachable, but we explicitly add this case to be clear.
                    }
                    1 => {
                        // -> (only_key)
                        response.keys.push(first.to_owned());
                    }
                    2 => {
                        // -> (first 0 last)
                        response.keys.push(first.to_owned());
                        response.hashes.push(H::identity());
                        response.keys.push(last.to_owned());
                    }
                    _ => {
                        // -> (first h(middle) last)
                        response.keys.push(first.to_owned());
                        response.hashes.push(self.store.hash_range(&first, &last));
                        response.keys.push(last.to_owned());
                    }
                }
            }
            messages.push(response)
        }
        Ok(messages)
    }

    /// Process an incoming message and respond with a message reply.
    #[instrument(skip_all, ret)]
    pub fn process_messages(&mut self, received: &[Message<K, H>]) -> Result<Response<K, H>> {
        debug!("process_messages");
        // First we must find the intersection of interests.
        // Then reply with a message per intersection.
        //
        // TODO: This is O(n^2) over the number of interests.
        // We should make this more efficient in the future.
        // Potentially we could use a variant of https://en.wikipedia.org/wiki/Bounding_volume_hierarchy
        // to quickly find intersections.
        let mut intersections: Vec<(RangeOpen<K>, BoundedMessage<K, H>)> = Vec::new();
        for range in self.interests.interests()? {
            for msg in received {
                if let Some(intersection) = range.intersect(&msg.range()) {
                    let bounded = msg.bound(&intersection);
                    intersections.push((intersection, bounded))
                }
            }
        }
        debug!(?intersections, "intersections");

        let mut response = Response {
            is_synchronized: true,
            ..Default::default()
        };
        for (range, received) in intersections {
            trace!(?range, ?received, "processing range");
            let mut response_message = Message::new(&range.start, &range.end);
            for key in &received.keys {
                if self.store.insert(key)? {
                    response.is_synchronized = false;
                }
            }

            if let Some(mut left_fencepost) = self.store.first(&range.start, &range.end) {
                let mut received_hashs = received.hashes.iter();
                let mut received_keys = received.keys.iter();
                let mut right_fencepost: K = match received_keys.next() {
                    Some(k) => k.to_owned(),
                    None => self
                        .store
                        .last(&range.start, &range.end)
                        .expect("should be at least one key"),
                };

                let mut received_hash = &H::identity();
                let zero = &H::identity();

                response_message.keys.push(left_fencepost.clone());
                while !received.keys.is_empty() && left_fencepost < *received.keys.last().unwrap() {
                    response.is_synchronized &= response_message.process_range(
                        &left_fencepost,
                        &right_fencepost,
                        received_hash,
                        &self.store,
                    )?;
                    left_fencepost = right_fencepost;
                    right_fencepost = match received_keys.next() {
                        Some(k) => k.to_owned(),
                        None => self
                            .store
                            .last(&range.start, &range.end)
                            .expect("should be at least one key"),
                    };
                    received_hash = received_hashs.next().unwrap_or(zero);
                }
                if !received.keys.is_empty() {
                    response.is_synchronized &= response_message.process_range(
                        received.keys.last().unwrap(),
                        &self
                            .store
                            .last(&range.start, &range.end)
                            .expect("should be at least one key"),
                        zero,
                        &self.store,
                    )?;
                }
                response_message.end_streak(
                    &self
                        .store
                        .last(&range.start, &range.end)
                        .expect("should be at least one key"),
                    &self.store,
                );
            };
            response.messages.push(response_message);
        }
        Ok(response)
    }

    /// Insert a new key into the key space.
    pub fn insert(&mut self, key: &K) -> Result<()> {
        self.store.insert(key)?;
        Ok(())
    }

    /// Reports total number of keys
    pub fn len(&self) -> usize {
        self.store.len()
    }

    /// Reports if the set is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
    ) -> Box<dyn Iterator<Item = K> + '_> {
        self.store
            .range(left_fencepost, right_fencepost, offset, limit)
    }

    /// Return all keys.
    pub fn full_range(&self) -> Box<dyn Iterator<Item = K> + '_> {
        self.store.full_range()
    }

    fn interests(&self) -> Result<Vec<RangeOpen<K>>> {
        self.interests.interests()
    }
}

/// Store defines the API needed to store the Recon set.
pub trait Store: std::fmt::Debug {
    /// Type of the Key being stored.
    type Key: Key;
    /// Type of the AssociativeHash to compute over keys.
    type Hash: AssociativeHash;

    /// Insert a new key into the key space.
    /// Returns true if the key did not previously exist.
    fn insert(&mut self, key: &Self::Key) -> Result<bool>;

    /// Return the hash of all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    fn hash_range(&self, left_fencepost: &Self::Key, right_fencepost: &Self::Key) -> Self::Hash;

    /// Return all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    fn range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> Box<dyn Iterator<Item = Self::Key> + '_>;

    /// Return all keys.
    fn full_range(&self) -> Box<dyn Iterator<Item = Self::Key> + '_> {
        self.range(
            &Self::Key::min_value(),
            &Self::Key::max_value(),
            0,
            usize::MAX,
        )
    }

    /// Return a key that is approximately in the middle of the range.
    /// An exact middle is not necessary but performance will be better with a better approximation.
    ///
    /// The default implementation will count all elements and then find the middle.
    #[instrument(skip(self), ret)]
    fn middle(&self, left_fencepost: &Self::Key, right_fencepost: &Self::Key) -> Option<Self::Key> {
        let count = self.count(left_fencepost, right_fencepost);
        self.range(left_fencepost, right_fencepost, (count - 1) / 2, 1)
            .next()
    }
    /// Return the number of keys within the range.
    #[instrument(skip(self), ret)]
    fn count(&self, left_fencepost: &Self::Key, right_fencepost: &Self::Key) -> usize {
        self.range(left_fencepost, right_fencepost, 0, usize::MAX)
            .count()
    }
    /// Return the first key within the range.
    #[instrument(skip(self), ret)]
    fn first(&self, left_fencepost: &Self::Key, right_fencepost: &Self::Key) -> Option<Self::Key> {
        self.range(left_fencepost, right_fencepost, 0, 1).next()
    }
    /// Return the last key within the range.
    #[instrument(skip(self), ret)]
    fn last(&self, left_fencepost: &Self::Key, right_fencepost: &Self::Key) -> Option<Self::Key> {
        self.range(left_fencepost, right_fencepost, 0, usize::MAX)
            .last()
    }

    /// Return the first and last keys within the range.
    /// If the range contains only a single key it will be returned as both first and last.
    fn first_and_last(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> Option<(Self::Key, Self::Key)> {
        let mut range = self.range(left_fencepost, right_fencepost, 0, usize::MAX);
        let first = range.next();
        if let Some(first) = first {
            if let Some(last) = range.last() {
                Some((first, last))
            } else {
                Some((first.clone(), first))
            }
        } else {
            None
        }
    }

    /// Reports total number of keys
    fn len(&self) -> usize {
        self.count(&Self::Key::min_value(), &Self::Key::max_value())
    }
    /// Reports of there are no keys stored.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Represents a key that can be reconciled via Recon.
pub trait Key: From<Vec<u8>> + Ord + Clone + Display + std::fmt::Debug {
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
}

/// Associative hash function that is 32 bytes long.
///
/// Associativity means the order in which items are hashed is unimportant and that hashes can be
/// accumulated by summing.
pub trait AssociativeHash:
    std::ops::Add<Output = Self> + Clone + Default + PartialEq + std::fmt::Debug + From<[u32; 8]>
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
pub trait InterestProvider {
    /// The type of Key over which we are interested.
    type Key: Key;
    /// Report a set of interests.
    fn interests(&self) -> Result<Vec<RangeOpen<Self::Key>>>;
}

/// InterestProvider that is interested in everything.
#[derive(Debug)]
pub struct FullInterests<K>(PhantomData<K>);

impl<K> Default for FullInterests<K> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<K: Key> InterestProvider for FullInterests<K> {
    type Key = K;

    fn interests(&self) -> Result<Vec<RangeOpen<K>>> {
        Ok(vec![(K::min_value(), K::max_value()).into()])
    }
}

// Implement InterestProvider for a Recon of interests.
impl<H, S, I> InterestProvider for Arc<Mutex<Recon<Interest, H, S, I>>>
where
    H: AssociativeHash + std::fmt::Debug + Serialize + for<'de> Deserialize<'de> + Send + 'static,
    S: Store<Key = Interest, Hash = H> + Send + 'static,
    I: InterestProvider<Key = Interest>,
{
    type Key = EventId;

    fn interests(&self) -> Result<Vec<RangeOpen<EventId>>> {
        self.lock()
            .expect("should be able to acquire lock")
            .range(
                &Interest::min_value(),
                &Interest::max_value(),
                0,
                usize::MAX,
            )
            .map(|interest| {
                let RangeOpen { start, end } = interest.range()?;
                Ok((EventId::from(start), EventId::from(end)).into())
            })
            .collect::<Result<Vec<RangeOpen<EventId>>>>()
    }
}

/// Messages are alternating keys and hashes.
/// The hashes are of all keys in range from left to right.
///
/// -> (first, hash(...), middle, hash(...), last)
///
/// The key to the left of the hash is less then all keys in the hash.
/// The key to the right of the hash is greater the all keys in the hash.
///
/// Represented as a struct of arrays.
///   keys[]
///   hashes[]
/// with one more key then hashes.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Message<K, H> {
    // Exclusive start bound, a value of None implies K::min_value
    start: Option<K>,
    // Exclusive end bound, a value of None implies K::max_value
    end: Option<K>,
    // keys must be 1 longer then hashs unless both are empty
    keys: Vec<K>,

    // hashes must be 1 shorter then keys
    hashes: Vec<H>,
}

// Explicitly implement default so that K and H do not have an unnecessary Default constraint.
impl<K: Key, H> Default for Message<K, H> {
    fn default() -> Self {
        Self {
            start: Default::default(),
            end: Default::default(),
            keys: Default::default(),
            hashes: Default::default(),
        }
    }
}

impl<K, H> Display for Message<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(")?;
        if let Some(start) = &self.start {
            write!(f, "<{start}")?;
        }
        for (k, h) in self.keys.iter().zip(self.hashes.iter()) {
            let hash_hex = if h.is_zero() {
                "0".to_string()
            } else {
                h.to_hex()[0..6].to_string()
            };
            write!(f, "{}, {}, ", k, hash_hex)?;
        }
        if let Some(end) = &self.end {
            write!(f, ">{end}")?;
        }

        write!(
            f,
            "{})",
            self.keys
                .last()
                .map_or(String::default(), |key| key.to_string(),)
        )
    }
}

impl<K, H> Message<K, H>
where
    K: Key,
    H: AssociativeHash,
{
    fn new(start: &K, end: &K) -> Self {
        Message {
            start: if start == &K::min_value() {
                None
            } else {
                Some(start.clone())
            },
            end: if end == &K::max_value() {
                None
            } else {
                Some(end.clone())
            },
            ..Default::default()
        }
    }
    // When a new key is added to the list of keys
    // we add the accumulator to hashes and clear it
    // if it is the first key there is no range so we don't push the accumulator
    // keys must be pushed in lexical order

    fn end_streak<S>(&mut self, left_fencepost: &K, local_store: &S)
    where
        S: Store<Key = K, Hash = H>,
    {
        let h = local_store.hash_range(self.keys.last().unwrap(), left_fencepost);
        // If the left fencepost has not been sent send it now
        if self.keys.last().unwrap() != left_fencepost {
            // Add the left_fencepost to end the match streak.
            self.keys.push(left_fencepost.to_owned());
            self.hashes.push(h);
        }
    }

    // Process keys within a specific range. Returns true if the ranges were already in sync.
    fn process_range<S>(
        &mut self,
        left_fencepost: &K,
        right_fencepost: &K,
        received_hash: &H,
        local_store: &S,
    ) -> Result<bool>
    where
        S: Store<Key = K, Hash = H>,
    {
        if left_fencepost == right_fencepost {
            return Ok(true);
        }

        let calculated_hash = local_store.hash_range(left_fencepost, right_fencepost);

        if &calculated_hash == received_hash {
            return Ok(true);
        }

        self.end_streak(left_fencepost, local_store);

        if calculated_hash.is_zero() {
            // we are missing all keys in range
            // send a 0
            self.keys.push(right_fencepost.to_owned());
            self.hashes.push(H::identity());
        } else if received_hash.is_zero() {
            // they are missing all keys in range
            // send all the keys
            debug!(
                left_fencepost = left_fencepost.to_hex(),
                right_fencepost = right_fencepost.to_hex(),
                "sending all keys in range"
            );
            for key in local_store.range(left_fencepost, right_fencepost, 0, usize::MAX) {
                self.keys.push(key.to_owned());
                self.hashes.push(H::identity());
            }
        } else {
            // We disagree on the hash for range.
            // Split the range.
            trace!(
                "split ({},{}) {}!={}",
                left_fencepost,
                right_fencepost,
                received_hash.to_hex(),
                calculated_hash.to_hex()
            );
            self.send_split(left_fencepost, right_fencepost, local_store)?;
        }
        Ok(false)
    }

    #[instrument(skip(self, local_store))]
    fn send_split<S>(
        &mut self,
        left_fencepost: &K,
        right_fencepost: &K,
        local_store: &S,
    ) -> Result<()>
    where
        S: Store<Key = K, Hash = H>,
    {
        // If less than SPLIT_THRESHOLD exist just send them, do not split.
        const SPLIT_THRESHOLD: usize = 4;
        let mut range = local_store.range(left_fencepost, right_fencepost, 0, usize::MAX);
        let head: Vec<K> = range.by_ref().take(SPLIT_THRESHOLD).collect();

        if head.len() < SPLIT_THRESHOLD {
            trace!("sending all keys");
            for key in head {
                self.keys.push(key.to_owned());
                self.hashes.push(H::identity());
            }
        } else {
            let mid_key = local_store.middle(left_fencepost, right_fencepost);
            trace!("splitting on {:?}", mid_key);
            if let Some(mid_key) = mid_key {
                self.keys.push(mid_key.to_owned());
                self.hashes
                    .push(local_store.hash_range(left_fencepost, &mid_key));

                self.keys.push(right_fencepost.to_owned());
                self.hashes
                    .push(local_store.hash_range(&mid_key, right_fencepost));
            } else {
                bail!("unable to find a split key")
            };
        }
        Ok(())
    }

    fn bound(&self, range: &RangeOpen<K>) -> BoundedMessage<K, H> {
        // TODO: Can we do this without allocating?
        // Some challenges that make this hard currently:
        //  1. The process_messages method iterates over the keys twice
        //  2. We have to keep keys and hashes properly aligned

        debug!(?self.keys, "bound keys");
        let mut hashes = self.hashes.iter();
        let keys: Vec<K> = self
            .keys
            .iter()
            .skip_while(|key| {
                if *key <= &range.start {
                    // Advance hashes
                    hashes.next();
                    true
                } else {
                    false
                }
            })
            .take_while(|key| *key < &range.end)
            .cloned()
            // Collect the keys to ensure side effects of iteration have been applied.
            .collect();

        // Collect the hashes up to the last key.
        let hashes = if keys.len() <= 1 {
            Vec::new()
        } else {
            hashes.take(keys.len() - 1).cloned().collect()
        };

        BoundedMessage { keys, hashes }
    }

    fn range(&self) -> RangeOpen<K> {
        RangeOpen {
            start: self.start.to_owned().unwrap_or_else(|| K::min_value()),
            end: self.end.to_owned().unwrap_or_else(|| K::max_value()),
        }
    }
}

// A derivative message that has had its keys and hashes bounded to a specific start and end range.
#[derive(Debug)]
struct BoundedMessage<K, H> {
    keys: Vec<K>,
    hashes: Vec<H>,
}

/// Response from processing a message
#[derive(Debug)]
pub struct Response<K, H> {
    messages: Vec<Message<K, H>>,
    is_synchronized: bool,
}

impl<K: Key, H> Default for Response<K, H> {
    fn default() -> Self {
        Self {
            messages: Default::default(),
            is_synchronized: Default::default(),
        }
    }
}

impl<K, H> Response<K, H> {
    /// Consume the response and produce a message
    pub fn into_messages(self) -> Vec<Message<K, H>> {
        self.messages
    }
    /// Report if the response indicates that synchronization has completed
    pub fn is_synchronized(&self) -> bool {
        self.is_synchronized
    }
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
}
