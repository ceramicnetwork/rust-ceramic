use crate::Bytes;
use cid::multibase::{self, Base};
pub use cid::Cid;
use multihash_codetable::Multihash;
use std::fmt::Formatter;
use std::io::Write;

use int_enum::IntEnum;
use serde::{Deserialize, Deserializer, Serialize};
use unsigned_varint::{decode, encode};

/// Types of possible stream id's
/// Defined here:
/// https://cips.ceramic.network/tables/streamtypes.csv
#[repr(u64)]
#[derive(Copy, Clone, Debug, Eq, IntEnum, PartialEq)]
pub enum StreamIdType {
    /// A stream type representing a json document
    /// https://cips.ceramic.network/CIPs/cip-8
    Tile = 0,
    /// Link blockchain accounts to DIDs
    /// https://cips.ceramic.network/CIPs/cip-7
    Caip10Link = 1,
    /// Defines a schema shared by group of documents in ComposeDB
    /// https://github.com/ceramicnetwork/js-ceramic/tree/main/packages/stream-model
    Model = 2,
    /// Represents a json document in ComposeDB
    /// https://github.com/ceramicnetwork/js-ceramic/tree/main/packages/stream-model-instance
    ModelInstanceDocument = 3,
    /// A stream that is not meant to be loaded
    /// https://github.com/ceramicnetwork/js-ceramic/blob/main/packages/stream-model/src/model.ts#L163-L165
    Unloadable = 4,
    /// An event id encoded as a cip-124 EventID
    /// https://cips.ceramic.network/CIPs/cip-124
    EventId = 5,
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamId {
    /// The type of the stream
    pub r#type: StreamIdType,
    /// Cid of the stream
    pub cid: Cid,
}

const STREAMID_CODEC: u64 = 206;

/// Known [`StreamId`] to which all model streams belong.
pub const METAMODEL_STREAM_ID: StreamId = {
    // Original typescript code that generates the stream id:
    //
    // ```js
    // import { CID } from 'multiformats/cid'
    // import { create } from 'multiformats/hashes/digest'
    // import { code, encode } from '@ipld/dag-cbor'
    // import { identity } from 'multiformats/hashes/identity'
    //
    //  static readonly MODEL: StreamID = (function () {
    //    const data = encode('model-v1')
    //    const multihash = identity.digest(data)
    //    const digest = create(code, multihash.bytes)
    //    const cid = CID.createV1(code, digest)
    //    return new StreamID('UNLOADABLE', cid)
    //  })()
    // ```
    //
    // Hash is the identity hash of the dab-cbor encoding of the string `model-v1`.
    let hash = [
        0x00, 0x09, 0x68, // cbor string header
        'm' as u8, 'o' as u8, 'd' as u8, 'e' as u8, 'l' as u8, '-' as u8, 'v' as u8, '1' as u8,
    ];
    // The above typescript code has a bug that we must reproduce here.
    // The hash is the identity hash, which should use the code 0x00, however the code was set to
    // 0x71 (a.k.a dag-cbor).
    // Therefore we use the dag-cbor code for both the multihash and the cid.
    const DAG_CBOR_CODEC: u64 = 0x71;
    let multihash = const_unwrap(Multihash::wrap(DAG_CBOR_CODEC, &hash));
    StreamId {
        r#type: StreamIdType::Unloadable,
        cid: cid::Cid::new_v1(DAG_CBOR_CODEC, multihash),
    }
};

const fn const_unwrap<T: Copy, E>(r: Result<T, E>) -> T {
    let v = match r {
        Ok(r) => r,
        Err(_) => panic!(),
    };
    std::mem::forget(r);
    v
}

impl StreamId {
    /// Create a new stream id for a model type
    pub fn model(id: Cid) -> Self {
        Self {
            r#type: StreamIdType::Model,
            cid: id,
        }
    }

    /// Whether this stream id is a model type
    pub fn is_model(&self) -> bool {
        self.r#type == StreamIdType::Model
    }

    /// Create a new stream for a document type
    pub fn document(id: Cid) -> Self {
        Self {
            r#type: StreamIdType::ModelInstanceDocument,
            cid: id,
        }
    }

    /// Whether this stream id is a document type
    pub fn is_document(&self) -> bool {
        self.r#type == StreamIdType::ModelInstanceDocument
    }

    // TODO re-add this logic once we can use cid@0.10
    //fn len(&self) -> usize {
    //    U64_LEN
    //        + U64_LEN
    //        + self.cid.encoded_len()
    //        + self.commit.as_ref().map(|v| v.encoded_len()).unwrap_or(0)
    //}

    /// Write the stream id to a writer
    pub fn write<W: Write>(&self, mut writer: W) -> anyhow::Result<()> {
        let mut buf = encode::u64_buffer();
        let v = encode::u64(STREAMID_CODEC, &mut buf);
        writer.write_all(v)?;
        let v = encode::u64(self.r#type.int_value(), &mut buf);
        writer.write_all(v)?;
        self.cid.write_bytes(&mut writer)?;
        Ok(())
    }

    /// Convert the stream id to a vector
    pub fn to_vec(&self) -> Vec<u8> {
        // Use self.len() here when we have cid@0.10
        let buf = Vec::new();
        let mut writer = std::io::BufWriter::new(buf);
        // safe to unwrap because self.write should never fail.
        self.write(&mut writer).unwrap();
        writer.into_inner().unwrap()
    }
}

impl TryInto<Bytes> for StreamId {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        (&self).try_into()
    }
}

impl TryInto<Bytes> for &StreamId {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        Ok(Bytes::from(self.to_vec()))
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
        let b = self.to_vec();
        let s = multibase::encode(Base::Base36Lower, b);
        write!(f, "{}", s)
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
            Ok(StreamId { r#type: tpe, cid })
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
        assert_eq!(stream.r#type, StreamIdType::ModelInstanceDocument);
        let s = stream.to_string();
        assert_eq!(&s, orig);
    }
    #[test]
    fn metamodel_stream_id_to_string() {
        assert_eq!(
            "kh4q0ozorrgaq2mezktnrmdwleo1d",
            &METAMODEL_STREAM_ID.to_string()
        );
    }
}
