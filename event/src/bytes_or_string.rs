use serde::{de::Visitor, Deserialize, Serialize};

use crate::bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BytesOrString {
    Bytes(Bytes),
    String(String),
}

impl BytesOrString {
    /// Return the contents as a slice of bytes.
    pub fn as_slice(&self) -> &[u8] {
        match self {
            BytesOrString::Bytes(bytes) => bytes.as_slice(),
            BytesOrString::String(string) => string.as_bytes(),
        }
    }
}

impl From<String> for BytesOrString {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<Vec<u8>> for BytesOrString {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value.into())
    }
}
impl From<Bytes> for BytesOrString {
    fn from(value: Bytes) -> Self {
        Self::Bytes(value)
    }
}

impl Serialize for BytesOrString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            BytesOrString::Bytes(bytes) => bytes.serialize(serializer),
            BytesOrString::String(string) => string.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for BytesOrString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(BytesOrStringVisitor)
    }
}

struct BytesOrStringVisitor;

impl<'de> Visitor<'de> for BytesOrStringVisitor {
    type Value = BytesOrString;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a sequence of bytes or a string")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v.to_vec().into())
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v.into())
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v.to_string().into())
    }
    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v.into())
    }
}
