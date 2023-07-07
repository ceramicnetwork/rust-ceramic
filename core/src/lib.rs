//! # Ceramic Core
//! Core functionality for ceramic, including the StreamId, Cid, and Jws types.
#![warn(missing_docs)]
mod bytes;
mod event_id;
mod jwk;
mod jws;
mod network;
mod stream_id;

pub use bytes::Bytes;
pub use event_id::EventId;
pub use jwk::Jwk;
pub use jws::Jws;
pub use network::Network;
pub use stream_id::{StreamId, StreamIdType};

pub use cid::Cid;
pub use ssi;
pub use ssi::did::Document as DidDocument;

use base64::Engine;
use multibase::Base;
use serde::{Deserialize, Serialize};
use std::fmt::Formatter;

macro_rules! impl_multi_base {
    ($typname:ident, $base:expr) => {
        /// A string that is encoded with a multibase prefix
        #[derive(Clone, Debug, Deserialize, Serialize)]
        #[serde(transparent)]
        pub struct $typname(String);

        impl std::convert::TryFrom<&Cid> for $typname {
            type Error = anyhow::Error;

            fn try_from(v: &Cid) -> Result<Self, Self::Error> {
                let s = v.to_string_of_base($base)?;
                Ok(Self(s))
            }
        }

        impl std::convert::TryFrom<&StreamId> for $typname {
            type Error = anyhow::Error;

            fn try_from(v: &StreamId) -> Result<Self, Self::Error> {
                let v = v.to_vec()?;
                Ok(Self::from(v))
            }
        }

        impl AsRef<str> for $typname {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl From<&[u8]> for $typname {
            fn from(value: &[u8]) -> Self {
                Self(multibase::encode($base, value))
            }
        }

        impl From<Vec<u8>> for $typname {
            fn from(value: Vec<u8>) -> Self {
                Self::from(value.as_slice())
            }
        }
    };
}

impl_multi_base!(MultiBase32String, multibase::Base::Base32Lower);
impl_multi_base!(MultiBase36String, multibase::Base::Base36Lower);
impl_multi_base!(MultiBase58BtcString, multibase::Base::Base58Btc);
impl_multi_base!(MultiBase64String, multibase::Base::Base64);
impl_multi_base!(MultiBase64UrlString, multibase::Base::Base64Url);

/// Newtype to encapsulate a value that is DagCbor encoded
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(transparent)]
pub struct DagCborEncoded(Vec<u8>);

impl AsRef<[u8]> for DagCborEncoded {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl DagCborEncoded {
    /// Create a new DagCborEncoded from a value that can be serialized to DagCbor
    pub fn new<T: Serialize>(value: &T) -> anyhow::Result<Self> {
        let res = serde_ipld_dagcbor::to_vec(value)?;
        Ok(Self(res))
    }
}

impl std::fmt::Display for DagCborEncoded {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = multibase::encode(Base::Base64, &self.0);
        write!(f, "{}", s)
    }
}

/// A string that is encoded with base64
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(transparent)]
pub struct Base64String(String);

impl Base64String {
    /// Create a new Base64String from a cid
    pub fn from_cid(cid: &Cid) -> Self {
        Self::from(cid.to_bytes().as_slice())
    }
    /// Convert the Base64String to a Vec<u8>
    pub fn to_vec(&self) -> anyhow::Result<Vec<u8>> {
        let v = base64::engine::general_purpose::STANDARD_NO_PAD.decode(&self.0)?;
        Ok(v)
    }
}

impl AsRef<str> for Base64String {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<Vec<u8>> for Base64String {
    fn from(value: Vec<u8>) -> Self {
        Self::from(value.as_slice())
    }
}

impl From<&[u8]> for Base64String {
    fn from(value: &[u8]) -> Self {
        let s = base64::engine::general_purpose::STANDARD_NO_PAD.encode(value);
        Self(s)
    }
}

impl From<String> for Base64String {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// A string that is encoded with base64url
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(transparent)]
pub struct Base64UrlString(String);

impl Base64UrlString {
    /// Create a new Base64UrlString from a cid
    pub fn from_cid(cid: &Cid) -> Self {
        Self::from(cid.to_bytes().as_slice())
    }
    /// Convert the Base64UrlString to a Vec<u8>
    pub fn to_vec(&self) -> anyhow::Result<Vec<u8>> {
        let v = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&self.0)?;
        Ok(v)
    }
}

impl AsRef<str> for Base64UrlString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<Vec<u8>> for Base64UrlString {
    fn from(value: Vec<u8>) -> Self {
        Self::from(value.as_slice())
    }
}

impl From<&[u8]> for Base64UrlString {
    fn from(value: &[u8]) -> Self {
        let s = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value);
        Self(s)
    }
}

impl From<String> for Base64UrlString {
    fn from(value: String) -> Self {
        Self(value)
    }
}

// Version 0.10 of the cid crate has an encoded_len method.
// However we are on version 0.9 because of iroh dependencies.
//
// To make upgrading easier this trait implements the behavior
// however inefficiently.
//
// Once we upgrade to 0.10 of cid we can simply delete this code.
trait EncodedLen {
    fn encoded_len(&self) -> usize;
}

impl EncodedLen for Cid {
    fn encoded_len(&self) -> usize {
        self.to_bytes().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_roundtrip_base64string() {
        let data = vec![0, 1, 2, 3];
        let s = Base64String::from(data.as_slice());
        let other_data = s.to_vec().unwrap();
        assert_eq!(other_data, data);
    }

    #[test]
    fn should_roundtrip_base64urlstring() {
        let data = vec![0, 1, 2, 3];
        let s = Base64UrlString::from(data.as_slice());
        let other_data = s.to_vec().unwrap();
        assert_eq!(other_data, data);
    }
}
