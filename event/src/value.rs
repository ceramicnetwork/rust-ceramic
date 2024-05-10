use crate::EventBytes;
use serde::de::{Error, Visitor};
use serde::{de, Deserialize, Serialize};

/// Allowed values in additional headers
pub enum Value {
    /// Boolean value
    Bool(bool),
    /// Number value
    Number(i64),
    /// String value
    String(String),
    /// Bytes value
    Bytes(EventBytes),
}

impl Value {
    /// Get the value as a bool
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(v) => Some(*v),
            _ => None,
        }
    }

    /// Get the value as a number
    pub fn as_number(&self) -> Option<i64> {
        match self {
            Self::Number(v) => Some(*v),
            _ => None,
        }
    }

    /// Get the value as a string
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(v) => Some(v.as_ref()),
            _ => None,
        }
    }

    /// Get the value as bytes
    pub fn as_bytes(&self) -> Option<&EventBytes> {
        match self {
            Self::Bytes(v) => Some(v),
            _ => None,
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Number(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<EventBytes> for Value {
    fn from(value: EventBytes) -> Self {
        Self::Bytes(value)
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Bool(v) => serializer.serialize_bool(*v),
            Self::Number(v) => serializer.serialize_i64(*v),
            Self::String(v) => serializer.serialize_str(v),
            Self::Bytes(v) => v.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_byte_buf(ValueVisitor)
    }
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a value")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let bytes: EventBytes = v.to_vec().into();
        Ok(Value::Bytes(bytes))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let bytes: EventBytes = v.into();
        Ok(Value::Bytes(bytes))
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::Bool(v))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::String(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::String(v.to_string()))
    }

    fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::Number(v as i64))
    }

    fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::Number(v as i64))
    }

    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::Number(v as i64))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::Number(v))
    }

    fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::Number(v as i64))
    }

    fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::Number(v as i64))
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Value::Number(v as i64))
    }
}
