use serde::{Deserialize, Serialize};

/// Allowed values in additional headers
#[derive(Deserialize, Serialize)]
#[serde(untagged)]
pub enum Value {
    /// Boolean value
    Bool(bool),
    /// Number value
    Number(i64),
    /// String value
    String(String),
    /// Bytes value
    Bytes(Vec<u8>),
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
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(v) => Some(v.as_slice()),
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

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

/// A map of values
pub type ValueMap = std::collections::HashMap<String, Value>;
