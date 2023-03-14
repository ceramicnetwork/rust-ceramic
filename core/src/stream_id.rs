use crate::Bytes;
use cid::multibase::{self, Base};
pub use cid::Cid;
use std::fmt::Formatter;
use std::io::Write;

use int_enum::IntEnum;
use serde::{Deserialize, Deserializer, Serialize};
use unsigned_varint::{decode, encode};

/// Types of possible stream id's
#[repr(u64)]
#[derive(Copy, Clone, Debug, Eq, IntEnum, PartialEq)]
pub enum StreamIdType {
    /// A model stream id
    Model = 2,
    /// A document stream id
    Document = 3,
    /// Unloadable. This is also used for the parent stream id of all models
    Unloadable = 4,
}

impl Serialize for StreamIdType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(self.int_value())
    }
}

/// A stream id, which is a cid with a type
#[derive(Debug, Eq, PartialEq)]
pub struct StreamId {
    /// The type of the stream
    pub r#type: StreamIdType,
    /// Cid of the stream
    pub cid: Cid,
    /// Optional commit for this stream
    pub commit: Option<Cid>,
}

const U64_LEN: usize = 10;
const STREAMID_CODEC: u64 = 206;

impl StreamId {
    /// Create a new stream id for a model type
    pub fn model(id: Cid) -> Self {
        Self {
            r#type: StreamIdType::Model,
            cid: id,
            commit: None,
        }
    }

    /// Whether this stream id is a model type
    pub fn is_model(&self) -> bool {
        self.r#type == StreamIdType::Model
    }

    /// Create a new stream for a document type
    pub fn document(id: Cid) -> Self {
        Self {
            r#type: StreamIdType::Document,
            cid: id,
            commit: None,
        }
    }

    /// Whether this stream id is a document type
    pub fn is_document(&self) -> bool {
        self.r#type == StreamIdType::Document
    }

    fn len(&self) -> usize {
        U64_LEN
            + U64_LEN
            + self.cid.encoded_len()
            + self.commit.as_ref().map(|v| v.encoded_len()).unwrap_or(0)
    }

    /// Write the stream id to a writer
    pub fn write<W: Write>(&self, mut writer: W) -> anyhow::Result<usize> {
        let mut written = 0;
        let mut buf = encode::u64_buffer();
        let v = encode::u64(STREAMID_CODEC, &mut buf);
        written += v.len();
        writer.write_all(v)?;
        let v = encode::u64(self.r#type.int_value(), &mut buf);
        written += v.len();
        writer.write_all(v)?;
        written += self.cid.write_bytes(&mut writer)?;
        if let Some(cmt) = &self.commit {
            written += cmt.write_bytes(writer)?;
        }
        Ok(written)
    }

    /// Convert the stream id to a vector
    pub fn as_vec(&self) -> anyhow::Result<Vec<u8>> {
        let buf = Vec::with_capacity(self.len());
        let mut writer = std::io::BufWriter::new(buf);
        let written = self.write(&mut writer)?;
        let mut inner = writer.into_inner()?;
        if inner.len() != written {
            inner.resize(written, 0);
        }
        Ok(inner)
    }
}

impl TryInto<Bytes> for StreamId {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        Ok((&self).try_into()?)
    }
}

impl TryInto<Bytes> for &StreamId {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from(self.as_vec()?))
    }
}

impl std::str::FromStr for StreamId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (_, id) = multibase::decode(s)?;
        Self::try_from(id.as_slice())
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Ok(b) = self.as_vec() {
            let s = multibase::encode(Base::Base36Lower, b);
            write!(f, "{}", s)
        } else {
            Err(std::fmt::Error::default())
        }
    }
}

impl TryFrom<&[u8]> for StreamId {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let (value, rest) = decode::u64(value)?;
        if value == STREAMID_CODEC {
            let (tpe, rest) = decode::u64(rest)?;
            let tpe = StreamIdType::from_int(tpe)?;
            let cid = Cid::read_bytes(std::io::BufReader::new(rest))?;
            let rest = &rest[cid.encoded_len()..];
            let commit = if rest.is_empty() {
                None
            } else {
                let cid = Cid::read_bytes(std::io::BufReader::new(rest))?;
                Some(cid)
            };
            Ok(StreamId {
                r#type: tpe,
                cid,
                commit,
            })
        } else {
            anyhow::bail!("Invalid StreamId, does not include StreamId Codec");
        }
    }
}

impl Serialize for StreamId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for StreamId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(StreamIdVisitor)
    }
}

struct StreamIdVisitor;

impl<'de> serde::de::Visitor<'de> for StreamIdVisitor {
    type Value = StreamId;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a multi base string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match multibase::decode(v) {
            Ok((_, v)) => StreamId::try_from(v.as_slice())
                .map_err(|e| serde::de::Error::custom(format!("{:?}", e))),
            Err(e) => Err(serde::de::Error::custom(format!("{:?}", e))),
        }
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(&v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn can_serialize_and_deserialize_correctly() {
        let orig = "kjzl6kcym7w8y7nzgytqayf6aro12zt0mm01n6ydjomyvvklcspx9kr6gpbwd09";
        let stream = StreamId::from_str(orig).unwrap();
        assert_eq!(stream.r#type, 3);
        assert!(stream.commit.is_none());
        let s = stream.to_string();
        assert_eq!(&s, orig);
    }
}
