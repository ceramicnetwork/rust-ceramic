use serde::de::Error;
use serde::{
    de::{self, Visitor},
    Deserialize, Serialize,
};

use crate::Key;

/// Sequence of byte values including only ASCII alpha numeric values.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct AlphaNumBytes(Vec<u8>);

impl AlphaNumBytes {
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl From<&[u8]> for AlphaNumBytes {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

impl From<Vec<u8>> for AlphaNumBytes {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}
impl From<&str> for AlphaNumBytes {
    fn from(value: &str) -> Self {
        Self(value.as_bytes().to_vec())
    }
}
impl From<&String> for AlphaNumBytes {
    fn from(value: &String) -> Self {
        Self(value.clone().into_bytes())
    }
}
impl From<String> for AlphaNumBytes {
    fn from(value: String) -> Self {
        Self(value.into_bytes())
    }
}

impl AsRef<[u8]> for AlphaNumBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for AlphaNumBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Display bytes as utf-8 if possible otherwise as hex
        write!(
            f,
            "{}",
            String::from_utf8(self.0.as_slice().to_vec())
                .unwrap_or_else(|_| format!("0x{}", hex::encode_upper(self.0.as_slice())))
        )
    }
}

impl std::fmt::Debug for AlphaNumBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Format bytes field as utf-8 if possible otherwise as hex
        if f.alternate() {
            f.debug_tuple("Bytes").field(&format!("{}", &self)).finish()
        } else {
            write!(f, "{self}")
        }
    }
}

impl Serialize for AlphaNumBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for AlphaNumBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_byte_buf(BytesVisitor)
    }
}

struct BytesVisitor;

impl<'de> Visitor<'de> for BytesVisitor {
    type Value = AlphaNumBytes;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a sequence of bytes")
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(AlphaNumBytes(v))
    }

    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(AlphaNumBytes(v.to_vec()))
    }
}

impl Key for AlphaNumBytes {
    fn min_value() -> Self {
        Vec::new().into()
    }

    fn max_value() -> Self {
        // We need a value that sorts greater than any ascii value
        Self(b"\xff".as_slice().into())
    }

    fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    fn is_fencepost(&self) -> bool {
        // We assume that the only valid fenceposts are the min and max values
        self == &Self::min_value() || self == &Self::max_value()
    }
}
