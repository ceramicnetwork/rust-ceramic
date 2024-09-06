//! # Ceramic Core
//! Core functionality for ceramic, including the StreamId, Cid, and Jws types.
#![warn(missing_docs)]
mod bytes;
pub mod event_id;
pub mod interest;
mod jwk;
mod key;
mod network;
mod range;
mod serialize_ext;
mod stream_id;

pub use bytes::Bytes;
pub use event_id::EventId;
pub use interest::{Interest, PeerId};
pub use jwk::Jwk;
pub use key::{
    cid_from_ed25519_key_pair, did_from_peer_id, did_key_from_ed25519_key_pair,
    ed25519_key_pair_from_secret, read_ed25519_key_from_dir,
};
pub use network::Network;
pub use range::RangeOpen;
pub use serialize_ext::SerializeExt;
pub use stream_id::{StreamId, StreamIdType};

pub use cid::Cid;
pub use ssi;
pub use ssi::did::Document as DidDocument;

use base64::Engine;
use serde::{Deserialize, Serialize};

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
                let v = v.to_vec();
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

impl std::fmt::Display for Base64String {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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

    /// Deserialize the Base64UrlString to a value
    pub fn to_value<T: serde::de::DeserializeOwned>(&self) -> anyhow::Result<T> {
        let v = self.to_vec()?;
        let res = serde_json::from_slice(&v)?;
        Ok(res)
    }
}

impl std::fmt::Display for Base64UrlString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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
