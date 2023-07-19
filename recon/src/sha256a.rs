use ::serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use multihash::{Hasher, Sha2_256};
use serde::de::Visitor;
use std::convert::From;
use std::fmt::{self, Debug, Display, Formatter};

use crate::{recon::Key, AssociativeHash};

/// Sha256a an associative hash function for use in set reconciliation
#[derive(Default, PartialEq, Clone, Copy)]
pub struct Sha256a([u32; 8]);

impl std::ops::Add for Sha256a {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        Sha256a([
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

impl Serialize for Sha256a {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.is_zero() {
            serializer.serialize_bytes(&[])
        } else {
            serializer.serialize_bytes(&self.as_bytes())
        }
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
    // Construct Sha256a directly to avoid unnecessary copies of the data.
    type Value = Sha256a;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("bytes")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v.is_empty() {
            Ok(Sha256a::default())
        } else {
            let bytes: [u8; 32] = v
                .try_into()
                .map_err(|_| ByteVisitor::length_error(v.len()))?;
            Ok(Sha256a::from(&bytes))
        }
    }
    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v.is_empty() {
            Ok(Sha256a::default())
        } else {
            let bytes: [u8; 32] = v
                .as_slice()
                .try_into()
                .map_err(|_| ByteVisitor::length_error(v.len()))?;
            Ok(Sha256a::from(&bytes))
        }
    }
    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v.is_empty() {
            Ok(Sha256a::default())
        } else {
            let bytes: [u8; 32] = v
                .try_into()
                .map_err(|_| ByteVisitor::length_error(v.len()))?;
            Ok(Sha256a::from(&bytes))
        }
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
        if v.is_empty() {
            Ok(Sha256a::default())
        } else {
            let bytes: [u8; 32] = v
                .as_slice()
                .try_into()
                .map_err(|_| ByteVisitor::length_error(v.len()))?;
            Ok(Sha256a::from(&bytes))
        }
    }
}
impl<'de> Deserialize<'de> for Sha256a {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(ByteVisitor)
    }
}

impl AssociativeHash for Sha256a {
    fn digest<K: Key>(key: &K) -> Self {
        let mut hasher = Sha2_256::default();
        hasher.update(key.as_bytes());
        // sha256 is 32 bytes safe to unwrap to [u8; 32]
        let bytes: &[u8; 32] = hasher.finalize().try_into().unwrap();
        bytes.into()
    }

    fn as_bytes(&self) -> [u8; 32] {
        let mut res = [0; 32];
        for i in 0..8 {
            res[4 * i..(4 * i + 4)].copy_from_slice(&self.0[i].to_le_bytes());
        }
        res
    }
}

impl From<&[u8; 32]> for Sha256a {
    fn from(bytes: &[u8; 32]) -> Self {
        Sha256a([
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
}

impl Debug for Sha256a {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        f.debug_struct("Sha256a")
            .field("hex", &self.to_hex())
            .field("u32_8", &self.0)
            .finish()
    }
}

impl Display for Sha256a {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use crate::recon::AssociativeHash;

    use super::*;
    use ceramic_core::Bytes;
    use expect_test::expect;

    #[test]
    fn hello() {
        assert_eq!(
            Sha256a::digest(&Bytes::from("hello")).to_hex(),
            "2CF24DBA5FB0A30E26E83B2AC5B9E29E1B161E5C1FA7425E73043362938B9824",
        )
    }

    #[test]
    fn other() {
        let other_hash = Sha256a::digest(&Bytes::from("other"));
        expect![[r#"
            Sha256a {
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
    fn serde_json() {
        // JSON doesn't have a first class bytes value so its serializes values as a sequence of
        // integers.
        // Validate we can roundtrip this kind of serialization.
        let hello = Sha256a::digest(&Bytes::from("hello"));
        let data = serde_json::to_vec(&hello).unwrap();
        let new_hello: Sha256a = serde_json::from_slice(data.as_slice()).unwrap();
        assert_eq!(hello, new_hello);
    }
    #[test]
    fn serde_cbor() {
        let hello = Sha256a::digest(&Bytes::from("hello"));
        let data = serde_cbor::to_vec(&hello).unwrap();
        let new_hello: Sha256a = serde_cbor::from_slice(data.as_slice()).unwrap();
        assert_eq!(hello, new_hello);
    }
}
