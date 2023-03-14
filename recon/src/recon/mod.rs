use crate::AHash;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::ops::Bound::Excluded;

#[cfg(test)]
pub mod tests;

/// Recon
/// short for reconnaissance
/// short for set reconciliation based synchronization
///
/// A recon should hold the list of keys and generate responses
/// when processing messages
#[derive(Debug, Default)]
pub struct Recon {
    /// The set of keys and their AHashes
    keys: BTreeMap<String, AHash>, // this will be a b#tree at some point in the future
}

impl Recon {
    /// make a new recon form a set of Strings
    pub fn from_set(s: BTreeSet<String>) -> Recon {
        let mut r = Recon {
            keys: BTreeMap::default(),
        };
        for key in s {
            r.insert(&key);
        }
        r
    }

    /// insert a new string into a recon
    pub fn insert(&mut self, key: &str) {
        self.keys.insert(key.to_string(), AHash::digest(key));
    }

    fn hash_range<H: Hash>(&self, left_fencepost: &str, right_fencepost: &str) -> H {
        let l = Excluded(left_fencepost.to_string());
        let r = Excluded(right_fencepost.to_string());
        if l == r {
            return H::identity();
        }
        H::digest_many(self.keys.range((l, r)))
    }

    /// generate a first message for the synchronization back and forth.
    pub fn first_message<H: Hash>(&self) -> Message<H> {
        let mut response = Message::<H>::default();
        if self.keys.len() == 1 {
            // -> (only_key)
            response
                .keys
                .push(self.keys.first_key_value().unwrap().0.to_string());
        } else if self.keys.len() == 2 {
            // -> (first 0 last)
            response
                .keys
                .push(self.keys.first_key_value().unwrap().0.to_string());
            response.ahashs.push(H::identity());
            response
                .keys
                .push(self.keys.last_key_value().unwrap().0.to_string());
        } else if self.keys.len() > 2 {
            // -> (first h(middle) last)
            response
                .keys
                .push(self.keys.first_key_value().unwrap().0.to_string());
            response.ahashs.push(self.hash_range(
                self.keys.first_key_value().unwrap().0,
                self.keys.last_key_value().unwrap().0,
            ));
            response
                .keys
                .push(self.keys.last_key_value().unwrap().0.to_string());
        }
        response
    }

    /// Generate a response message for a incoming message
    pub fn process_message<H: Hash>(&mut self, received: &Message<H>) -> Message<H> {
        // self.keys.extend(received.keys.clone()); // add received keys
        for key in &received.keys {
            let h = AHash::digest(key);
            self.keys.insert(key.to_string(), h);
        }
        if self.keys.is_empty() {
            return Message::<H>::default();
        }
        let mut received_keys = received.keys.iter();
        let mut received_hashs = received.ahashs.iter();

        let mut response = Message::<H>::default(); // init the response empty

        let mut left_fencepost = self.keys.first_key_value().unwrap().0;
        let mut right_fencepost = received_keys
            .next()
            .unwrap_or(self.keys.last_key_value().unwrap().0);
        let mut received_hash = &H::identity();
        let zero = &H::identity();

        response.keys.push(left_fencepost.to_string());
        while !received.keys.is_empty() && left_fencepost < received.keys.last().unwrap() {
            response.process_range(left_fencepost, right_fencepost, received_hash, self);
            left_fencepost = right_fencepost;
            right_fencepost = received_keys
                .next()
                .unwrap_or(self.keys.last_key_value().unwrap().0);
            received_hash = received_hashs.next().unwrap_or(zero);
        }
        if !received.keys.is_empty() {
            response.process_range(
                received.keys.last().unwrap(),
                self.keys.last_key_value().unwrap().0,
                zero,
                self,
            );
        }
        response.end_streak(self.keys.last_key_value().unwrap().0, self);
        response
    }
}

/// Messages
/// messages are alternating keys and hashs
/// the hashs are of all keys in range from left to right
///
/// -> (first, hash(...), middle, hash(...), last)
/// the key to the left of the hash is less then all keys in the hash
/// the key to the right of the hash is grater the all keys in the hash
/// it is like a b-tree node storing a sorted set
///
/// Represented as a struct of arrays.
/// keys[]
/// hashs[]
/// with one more key then hashes
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Message<H: Hash> {
    /// keys must be 1 longer then ahashs unless both are empty
    #[serde(rename = "k")]
    pub keys: Vec<String>,

    /// ahashs must be 1 shorter then keys
    #[serde(rename = "h")]
    pub ahashs: Vec<H>,
}

impl<H: Hash> Display for Message<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(")?;
        for (k, h) in self.keys.iter().zip(self.ahashs.iter()) {
            let hash_hex = if h.is_zero() {
                "0".to_string()
            } else {
                h.to_hex()[0..6].to_string()
            };
            write!(f, "{}, {}, ", k, hash_hex)?;
        }

        write!(f, "{})", self.keys.last().unwrap_or(&"".to_string()))
    }
}

/// The generic trait for the associative hash function
pub trait Hash: std::ops::Add<Output = Self> + Clone + Default + PartialEq {
    /// The value that when added to the Hash it dose not change
    fn identity() -> Self {
        Self::default()
    }

    /// Tests if the hash is the identity
    fn is_zero(&self) -> bool;

    /// Set the value to the identity. This should not allocate.
    fn clear(&mut self);

    /// Add a string to the accumulated hash
    fn push(&mut self, key: (&String, &AHash));

    /// convert the hash to bytes
    ///
    /// for the identity this should be the 0 byte vector
    /// for all other states it should be 32 bytes
    fn to_bytes(&self) -> Vec<u8>;

    /// export the bytes as a hex string
    fn to_hex(&self) -> String;

    /// set the accumulated value to the bytes
    fn from_bytes(bytes: [u8; 32]) -> Self;

    /// sum a iterator of strings and AHashes
    fn digest_many<'a, I>(keys: I) -> Self
    where
        I: Iterator<Item = (&'a String, &'a AHash)>;
}

impl<H: Hash> Message<H> {
    // When a new key is added to the list of keys
    // we add the accumulator to ahashs and clear it
    // if it is the first key there is no range so we don't push the accumulator
    // keys must be pushed in lexical order

    fn end_streak(&mut self, left_fencepost: &str, local_keys: &Recon) {
        let h = local_keys.hash_range::<H>(self.keys.last().unwrap(), left_fencepost);
        // If the left fencepost has not been sent send it now
        if self.keys.last().unwrap() != left_fencepost {
            // Add the left_fencepost to end the match streak.
            self.keys.push(left_fencepost.to_string());
            self.ahashs.push(h);
        }
    }

    fn process_range(
        &mut self,
        left_fencepost: &str,
        right_fencepost: &str,
        received_hash: &H,
        local_keys: &Recon,
    ) {
        if left_fencepost == right_fencepost {
            return; // no keys in range nothing to do
        }

        let l = Excluded(left_fencepost.to_string());
        let r = Excluded(right_fencepost.to_string());

        let calculated_hash = local_keys.hash_range::<H>(left_fencepost, right_fencepost);

        if &calculated_hash == received_hash {
            return; // keys in range match nothing to do
        }

        self.end_streak(left_fencepost, local_keys);

        if calculated_hash.is_zero() {
            // we are missing all keys in range
            // send a 0
            self.keys.push(right_fencepost.to_string());
            self.ahashs.push(H::identity());
        } else if received_hash.is_zero() {
            // they are missing all keys in range
            // send all the keys
            for key in local_keys.keys.range::<String, _>((l, r)) {
                self.keys.push(key.0.to_string());
                self.ahashs.push(H::identity());
            }
        } else {
            // We disagree on the hash for range.
            // Split the range.
            // println!("split ({},{}) {}!={}", left_fencepost, right_fencepost, received_hash.to_hex(), calculated_hash.to_hex());
            self.send_split(left_fencepost, right_fencepost, local_keys);
        }
    }

    fn send_split(&mut self, left_fencepost: &str, right_fencepost: &str, local_keys: &Recon) {
        let range = local_keys.keys.range::<String, _>((
            Excluded(left_fencepost.to_string()),
            Excluded(right_fencepost.to_string()),
        ));
        let count = range.clone().count();

        if count <= 3 {
            for key in range {
                self.keys.push(key.0.clone());
                self.ahashs.push(H::identity());
            }
        } else {
            let mid_key = range.clone().nth((count - 1) / 2).unwrap().0;
            self.keys.push(mid_key.to_string());
            self.ahashs
                .push(local_keys.hash_range(left_fencepost, mid_key));

            self.keys.push(right_fencepost.to_string());
            self.ahashs
                .push(local_keys.hash_range(mid_key, right_fencepost));
        }
    }
}
