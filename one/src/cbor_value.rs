use anyhow::{anyhow, Error, Result};
use cid::Cid;
use enum_as_inner::EnumAsInner;
use minicbor::{data::Tag, data::Type, Decoder};
use ordered_float::OrderedFloat;
use std::{collections::BTreeMap, ops::Index};
use tracing::debug;

/// CborValue
/// a enum for working with dynamic values from decoding CBOR values.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Debug, EnumAsInner)]
pub enum CborValue {
    // not clear if Indef should be treated distinctly from the finite versions
    /// Simple Values (20 false | 21 true)
    /// https://datatracker.ietf.org/doc/html/rfc8949#fpnoconttbl2
    Bool(bool),
    /// Simple Values (22 null)
    /// https://datatracker.ietf.org/doc/html/rfc8949#fpnoconttbl2
    Null,
    /// Simple Values (23 undefined)
    /// https://datatracker.ietf.org/doc/html/rfc8949#fpnoconttbl2
    Undefined,
    /// Major type 0: An unsigned integer in the range 0..(2**64)-1 inclusive.
    /// https://datatracker.ietf.org/doc/html/rfc8949#section-3.1
    U64(u64),
    /// Major type 1: A negative integer in the range -2**64..-1 inclusive.
    /// https://datatracker.ietf.org/doc/html/rfc8949#section-3.1
    /// note: we store Major type 1: A negative integer less then -2**63 in a i128
    I64(i64),
    /// Major type 1: A negative integer in the range -2**64..-1 inclusive.
    /// https://datatracker.ietf.org/doc/html/rfc8949#section-3.1
    /// note: we store Major type 1: A negative integer grater then -2**63 in a i64
    Int(i128), // Cbor ints range from [ -(2^64) , 2^64-1 ] to fit this i65 we use i128
    /// Major type 7:
    /// Floating-point numbers and simple values, as well as the "break" stop code.
    /// https://datatracker.ietf.org/doc/html/rfc8949#fpnocont
    /// note: we store Simple values and breaks separately
    F64(OrderedFloat<f64>), // this makes it possible to put CborValue in BTreeMap
    /// Simple Values (20 false | 21 true)
    /// https://datatracker.ietf.org/doc/html/rfc8949#fpnoconttbl2
    /// 24..31 (reserved)
    /// 32..255 (unassigned)
    Simple(u8),
    /// Major type 2: A byte string.
    /// https://datatracker.ietf.org/doc/html/rfc8949#section-3.1
    Bytes(Vec<u8>),
    /// Major type 3: A text string
    /// https://datatracker.ietf.org/doc/html/rfc8949#section-3.1
    String(String),
    /// Major type 4: An array of data items.
    /// https://datatracker.ietf.org/doc/html/rfc8949#section-3.1
    Array(Vec<CborValue>),
    /// Major type 5: A map of pairs of data items.
    /// https://datatracker.ietf.org/doc/html/rfc8949#section-3.1
    Map(BTreeMap<CborValue, CborValue>),
    /// Major type 6:
    /// A tagged data item ("tag") whose tag number, an integer in the range 0..(2**64)-1 inclusive
    /// https://datatracker.ietf.org/doc/html/rfc8949#tags
    /// https://www.iana.org/assignments/cbor-tags/cbor-tags.xhtml
    /// tag(42) = Cid
    Tag((Tag, Box<CborValue>)),
    /// Major type 7: 0xff Break signal
    /// https://datatracker.ietf.org/doc/html/rfc8949#break
    Break, // 0xff
    /// Used to represent a byte that was not valid for its location.
    /// You should not see this from a well formed CBOR value.
    Unknown(u8),
}

impl CborValue {
    /// Parse bytes to a single CborValue
    /// This will error on CBOR seq.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let mut decoder = Decoder::new(bytes);
        let c = CborValue::next(&mut decoder);
        if decoder.position() != bytes.len() {
            debug!("decoder not at end {}/{}", decoder.position(), bytes.len());
            return Err(anyhow!("CborValue decode error not at end"));
        }
        c
    }
    fn next(decoder: &mut Decoder) -> Result<CborValue> {
        match decoder.datatype() {
            Ok(Type::Bool) => Ok(CborValue::Bool(decoder.bool()?)),
            Ok(Type::Null) => {
                decoder.null()?;
                Ok(CborValue::Null)
            }
            Ok(Type::Undefined) => {
                decoder.undefined()?;
                Ok(CborValue::Undefined)
            }
            Ok(Type::U8) => Ok(CborValue::U64(decoder.u8()?.into())),
            Ok(Type::U16) => Ok(CborValue::U64(decoder.u16()?.into())),
            Ok(Type::U32) => Ok(CborValue::U64(decoder.u32()?.into())),
            Ok(Type::U64) => Ok(CborValue::U64(decoder.u64()?)),
            Ok(Type::I8) => Ok(CborValue::I64(decoder.i8()?.into())),
            Ok(Type::I16) => Ok(CborValue::I64(decoder.i16()?.into())),
            Ok(Type::I32) => Ok(CborValue::I64(decoder.i32()?.into())),
            Ok(Type::I64) => Ok(CborValue::I64(decoder.i64()?)),
            Ok(Type::Int) => Ok(CborValue::Int(decoder.int()?.into())),
            Ok(Type::F16) => Ok(CborValue::F64(OrderedFloat(decoder.f16()?.into()))),
            Ok(Type::F32) => Ok(CborValue::F64(OrderedFloat(decoder.f32()?.into()))),
            Ok(Type::F64) => Ok(CborValue::F64(OrderedFloat(decoder.f64()?))),
            Ok(Type::Simple) => Ok(CborValue::Simple(decoder.simple()?)),
            Ok(Type::Bytes) => Ok(CborValue::Bytes(decoder.bytes()?.to_vec())),
            Ok(Type::BytesIndef) => Ok(CborValue::Undefined), // TODO: support Type::BytesIndef
            Ok(Type::String) => Ok(CborValue::String(decoder.str()?.to_string())),
            Ok(Type::StringIndef) => Ok(CborValue::Undefined), // TODO: support Type::StringIndef
            Ok(Type::Array) => {
                let mut array = Vec::new();
                for _ in 0..decoder.array().unwrap().unwrap() {
                    let value = CborValue::next(decoder)?;
                    array.push(value);
                }
                Ok(CborValue::Array(array))
            }
            Ok(Type::ArrayIndef) => {
                let mut array = Vec::new();
                loop {
                    let value = CborValue::next(decoder)?;
                    if let CborValue::Break = value {
                        break;
                    }
                    array.push(value)
                }
                Ok(CborValue::Array(array))
            }
            Ok(Type::Map) => {
                let mut map = BTreeMap::new();
                for _ in 0..decoder.map().unwrap().unwrap() {
                    let key = CborValue::next(decoder)?;
                    let value = CborValue::next(decoder)?;
                    map.insert(key, value);
                }
                Ok(CborValue::Map(map))
            }
            Ok(Type::MapIndef) => Ok(CborValue::Undefined), // TODO: support Type::MapIndef
            Ok(Type::Tag) => {
                let tag: Tag = decoder.tag()?;
                let value = CborValue::next(decoder)?;
                Ok(CborValue::Tag((tag, Box::new(value))))
            }
            Ok(Type::Break) => Ok(CborValue::Break),
            Ok(Type::Unknown(additional)) => Ok(CborValue::Unknown(additional)),
            Err(e) => Err(e.into()),
        }
    }

    /// Accesses a deeply nested CborValue when the path keys are all strings.
    pub fn path(&self, parts: &[&str]) -> CborValue {
        match parts.split_first() {
            None => self.clone(), // if there are no parts this is thing at the path.
            Some((first, rest)) => {
                let CborValue::Map(map) = &self else {
                    return CborValue::Undefined; // if self is not a map there is no thing at the path.
                };
                let Some(next) = map.get(&first.to_string().into()) else {
                    return CborValue::Undefined; // if the next part is not in the map there is no thing at the path.
                };
                next.path(rest)
            }
        }
    }

    /// Accesses the underlying CborValue for a specified tag type.
    pub fn untag(&self, tag: u64) -> Option<&CborValue> {
        if let CborValue::Tag((Tag::Unassigned(inner_tag), boxed_value)) = &self {
            if inner_tag == &tag {
                return Some(boxed_value);
            }
        }
        None
    }

    /// get a printable string of the type_name
    pub fn type_name(&self) -> &str {
        match self {
            CborValue::Bool(_) => "bool",
            CborValue::Null => "null",
            CborValue::Undefined => "undefined",
            CborValue::U64(_) => "u64",
            CborValue::I64(_) => "i64",
            CborValue::Int(_) => "int",
            CborValue::F64(_) => "f64",
            CborValue::Simple(_) => "simple",
            CborValue::Bytes(_) => "bytes",
            CborValue::String(_) => "string",
            CborValue::Array(_) => "array",
            CborValue::Map(_) => "map",
            CborValue::Tag(_) => "tag",
            CborValue::Break => "break",
            CborValue::Unknown(_) => "unknown",
        }
    }

    /// If the CborValue is a CborValue::Array get the CborValue at index.
    /// If it is not a CborValue::Array or index is out of bounds returns None.
    pub fn get_index(&self, index: usize) -> Option<&CborValue> {
        self.as_array()?.get(index)
    }

    /// If the CborValue is a CborValue::Map get the CborValue at index.
    /// If it is not a CborValue::Map or key is missing returns None.
    pub fn get_key(&self, key: &str) -> Option<&CborValue> {
        self.as_map()?.get(&CborValue::String(key.to_owned()))
    }
}

impl From<&[u8]> for CborValue {
    fn from(bytes: &[u8]) -> Self {
        CborValue::Bytes(bytes.to_vec())
    }
}

impl TryInto<Vec<u8>> for CborValue {
    type Error = Error;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        match self {
            CborValue::Bytes(bytes) => Ok(bytes.to_vec()),
            CborValue::String(string) => Ok(string.as_bytes().to_vec()),
            _ => Err(anyhow!("{} not Bytes or String", self.type_name())),
        }
    }
}

impl From<&str> for CborValue {
    fn from(string: &str) -> Self {
        CborValue::String(string.to_string())
    }
}

impl From<String> for CborValue {
    fn from(string: String) -> Self {
        CborValue::String(string)
    }
}

impl TryInto<Cid> for CborValue {
    type Error = Error;
    fn try_into(self) -> Result<Cid, Self::Error> {
        if let Some(CborValue::Bytes(cid_bytes)) = self.untag(42) {
            Cid::try_from(&cid_bytes[1..]).map_err(|e| e.into())
        } else {
            Err(anyhow!("{} not a CID", self.type_name()))
        }
    }
}

impl TryInto<String> for CborValue {
    type Error = Error;
    fn try_into(self) -> Result<String, Self::Error> {
        if let CborValue::String(s) = self {
            Ok(s)
        } else {
            Err(anyhow!("{} not a String", self.type_name()))
        }
    }
}

impl Index<usize> for CborValue {
    type Output = Self;

    /// Returns a reference to the value corresponding to the supplied key.
    ///
    /// # Panics
    ///
    /// Panics if the key is not present in the `Vec`.
    fn index(&self, index: usize) -> &Self {
        &self.as_array().unwrap()[index]
    }
}

impl Index<&CborValue> for CborValue {
    type Output = Self;

    /// Returns a reference to the value corresponding to the supplied key.
    ///
    /// # Panics
    ///
    /// Panics if the key is not present in the `BTreeMap`.
    fn index(&self, key: &CborValue) -> &Self {
        &self.as_map().unwrap()[key]
    }
}
