use ::serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use multihash::{Hasher, Sha2_256};
use serde::de::Visitor;
use std::fmt::{self, Debug, Display, Formatter};

use crate::Hash;

/// AHash an associative hash function for use in set reconciliation
#[derive(Default, PartialEq, Clone, Copy)]
pub struct AHash([u32; 8]);

impl std::ops::Add for AHash {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        AHash([
            self.0[0].wrapping_add(rhs.0[0]),
            self.0[1].wrapping_add(rhs.0[1]),
            self.0[2].wrapping_add(rhs.0[2]),
            self.0[3].wrapping_add(rhs.0[3]),
            self.0[4].wrapping_add(rhs.0[4]),
            self.0[5].wrapping_add(rhs.0[5]),
            self.0[6].wrapping_add(rhs.0[6]),
            self.0[7].wrapping_add(rhs.0[7]),
        ])
    }
}

impl Serialize for AHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.to_bytes())
    }
}

// de::Visitor that can handle bytes or a sequence of byte values.
struct ByteVisitor;

impl ByteVisitor {
    fn length_error<E: de::Error>(len: usize) -> E {
        E::invalid_length(len, &"hash must have a length of 32 bytes")
    }
}
impl<'de> Visitor<'de> for ByteVisitor {
    // Construct AHash directly to avoid unneccessary copies of the data.
    type Value = AHash;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("bytes")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(AHash::from_bytes(
            v.try_into()
                .map_err(|_| ByteVisitor::length_error(v.len()))?,
        ))
    }
    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(AHash::from_bytes(
            v.as_slice()
                .try_into()
                .map_err(|_| ByteVisitor::length_error(v.len()))?,
        ))
    }
    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(AHash::from_bytes(
            v.try_into()
                .map_err(|_| ByteVisitor::length_error(v.len()))?,
        ))
    }
    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut v = Vec::with_capacity(32);
        while let Some(byte) = seq.next_element()? {
            v.push(byte);
            if v.len() > 32 {
                return Err(ByteVisitor::length_error(seq.size_hint().unwrap_or(1) + 32));
            }
        }
        Ok(AHash::from_bytes(
            v.as_slice()
                .try_into()
                .map_err(|_| ByteVisitor::length_error(v.len()))?,
        ))
    }
}
impl<'de> Deserialize<'de> for AHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(ByteVisitor)
    }
}

impl crate::recon::Hash for AHash {
    fn is_zero(&self) -> bool {
        self == &Self::default()
    }

    fn clear(&mut self) {
        self.0[0] = 0;
        self.0[1] = 0;
        self.0[2] = 0;
        self.0[3] = 0;
        self.0[4] = 0;
        self.0[5] = 0;
        self.0[6] = 0;
        self.0[7] = 0;
    }
    fn push(&mut self, key: (&String, &AHash)) {
        // Add a key to the accumulated associative hash
        self.0[0] = self.0[0].wrapping_add(key.1 .0[0]);
        self.0[1] = self.0[1].wrapping_add(key.1 .0[1]);
        self.0[2] = self.0[2].wrapping_add(key.1 .0[2]);
        self.0[3] = self.0[3].wrapping_add(key.1 .0[3]);
        self.0[4] = self.0[4].wrapping_add(key.1 .0[4]);
        self.0[5] = self.0[5].wrapping_add(key.1 .0[5]);
        self.0[6] = self.0[6].wrapping_add(key.1 .0[6]);
        self.0[7] = self.0[7].wrapping_add(key.1 .0[7]);
    }

    fn digest_many<'a, I>(keys: I) -> AHash
    where
        I: Iterator<Item = (&'a String, &'a AHash)>,
    {
        let mut total = AHash::identity();
        for key in keys {
            total.push(key)
        }
        total
    }

    fn to_bytes(&self) -> Vec<u8> {
        if self.is_zero() {
            vec![]
        } else {
            [
                u32::to_le_bytes(self.0[0]),
                u32::to_le_bytes(self.0[1]),
                u32::to_le_bytes(self.0[2]),
                u32::to_le_bytes(self.0[3]),
                u32::to_le_bytes(self.0[4]),
                u32::to_le_bytes(self.0[5]),
                u32::to_le_bytes(self.0[6]),
                u32::to_le_bytes(self.0[7]),
            ]
            .concat()
        }
    }

    fn from_bytes(bytes: &[u8; 32]) -> AHash {
        AHash([
            // 4 byte slices safe to unwrap to [u8; 4]
            u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
            u32::from_le_bytes(bytes[24..28].try_into().unwrap()),
            u32::from_le_bytes(bytes[28..32].try_into().unwrap()),
        ])
    }

    fn to_hex(&self) -> String {
        hex::encode(self.to_bytes()).to_uppercase()
    }
}

impl AHash {
    fn sha256_digest(s: &str) -> [u8; 32] {
        let mut hasher = Sha2_256::default();
        hasher.update(s.as_bytes());
        // sha256 is 32 bytes safe to unwrap to [u8; 32]
        hasher.finalize().try_into().unwrap()
    }

    /// turn a string into a AHash
    pub fn digest(key: &str) -> AHash {
        AHash::from_bytes(&AHash::sha256_digest(key))
    }

    /// allocate a new hash with the state from the hex string
    pub fn from_hex(hex_data: &String) -> Result<AHash, hex::FromHexError> {
        let mut bytes: [u8; 32] = [0u8; 32];
        hex::decode_to_slice(hex_data, &mut bytes)?;
        Ok(AHash::from_bytes(&bytes))
    }
}

impl Debug for AHash {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        f.debug_struct("Ahash")
            .field("hex", &self.to_hex())
            .field("u32_8", &self.0)
            .finish()
    }
}

impl Display for AHash {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use crate::recon::Hash;

    use super::*;
    use expect_test::expect;

    #[test]
    fn hello() {
        assert_eq!(
            AHash::digest("hello").to_hex(),
            "2CF24DBA5FB0A30E26E83B2AC5B9E29E1B161E5C1FA7425E73043362938B9824",
        )
    }

    #[test]
    fn other() {
        let other_hash = AHash::digest("other");
        expect![[r#"
            Ahash {
                hex: "D9298A10D1B0735837DC4BD85DAC641B0F3CEF27A47E5D53A54F2F3F5B2FCFFA",
                u32_8: [
                    277490137,
                    1483976913,
                    3628850231,
                    459582557,
                    669989903,
                    1398636196,
                    1060065189,
                    4207882075,
                ],
            }
        "#]]
        .assert_debug_eq(&other_hash)
    }

    #[test]
    fn push_add() {
        let plus = AHash::digest("hello") + AHash::digest("world");
        let mut push = AHash::identity();
        push.push((&"hello".to_string(), &AHash::digest("hello")));
        push.push((&"world".to_string(), &AHash::digest("world")));
        assert_eq!(plus.to_hex(), push.to_hex())
    }

    #[test]
    fn serde_json() {
        // JSON doesn't have a first class bytes value so its serializes values as a sequence of
        // integers.
        // Validate we can roundtrip this kind of serialization.
        let hello = AHash::digest("hello");
        let data = serde_json::to_vec(&hello).unwrap();
        let new_hello: AHash = serde_json::from_slice(&data).unwrap();
        assert_eq!(hello, new_hello);
    }
    #[test]
    fn serde_cbor() {
        let hello = AHash::digest("hello");
        let data = serde_cbor::to_vec(&hello).unwrap();
        let new_hello: AHash = serde_cbor::from_slice(&data).unwrap();
        assert_eq!(hello, new_hello);
    }
}
