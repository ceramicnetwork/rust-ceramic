use std::fmt::{Debug, Display};

use serde::de::Error;
use serde::{
    de::{self, Visitor},
    Deserialize, Serialize,
};

/// Sequence of byte values.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Bytes(Vec<u8>);

impl Bytes {
    /// Returns the inner slice held by this `Bytes`.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for Bytes {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}
impl From<&str> for Bytes {
    fn from(value: &str) -> Self {
        Self(value.as_bytes().to_vec())
    }
}
impl From<&String> for Bytes {
    fn from(value: &String) -> Self {
        Self(value.clone().into_bytes())
    }
}
impl From<String> for Bytes {
    fn from(value: String) -> Self {
        Self(value.into_bytes())
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Serialize for Bytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_byte_buf(BytesVisitor)
    }
}

struct BytesVisitor;

impl<'de> Visitor<'de> for BytesVisitor {
    type Value = Bytes;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a sequence of bytes")
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Bytes(v))
    }

    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Bytes(v.to_vec()))
    }
}

impl Display for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Display bytes as utf-8 if possible otherwise as hex
        write!(
            f,
            "{}",
            String::from_utf8(self.0.clone())
                .unwrap_or_else(|_| format!("0x{}", hex::encode_upper(&self.0)))
        )
    }
}

impl Debug for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Format bytes field as utf-8 if possible otherwise as hex
        f.debug_tuple("Bytes").field(&format!("{}", &self)).finish()
    }
}
