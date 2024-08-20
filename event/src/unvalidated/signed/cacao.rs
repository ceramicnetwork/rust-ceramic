//! Structures for encoding and decoding CACAO capability objects.

use serde::{ser::SerializeMap as _, Deserialize, Serialize};
use ssi::jwk::Algorithm;
use std::collections::HashMap;

/// Capability object, see https://github.com/ChainAgnostic/CAIPs/blob/main/CAIPs/caip-74.md
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Capability {
    /// Header for capability
    #[serde(rename = "h")]
    pub header: Header,
    /// Payload for capability
    #[serde(rename = "p")]
    pub payload: Payload,
    /// Signature for capability
    #[serde(rename = "s")]
    pub signature: Signature,
}

/// Type of Capability Header
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum HeaderType {
    /// EIP-4361 Capability
    #[serde(rename = "eip4361")]
    EIP4361,
    /// CAIP-122 Capability
    #[serde(rename = "caip122")]
    CAIP122,
}

/// Header for a Capability
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Header {
    /// Type of the Capability Header
    #[serde(rename = "t")]
    pub r#type: HeaderType,
}

/// Time format for capability
pub type CapabilityTime = chrono::DateTime<chrono::Utc>;

/// Payload for a CACAO
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Payload {
    /// Audience for payload
    #[serde(rename = "aud")]
    pub audience: String,

    /// Domain for payload
    pub domain: String,

    /// Expiration time
    #[serde(rename = "exp", skip_serializing_if = "Option::is_none")]
    pub expiration: Option<CapabilityTime>,

    /// Issued at time
    #[serde(rename = "iat")]
    pub issued_at: CapabilityTime,

    /// Issuer for payload. For capability will be DID in URI format
    #[serde(rename = "iss")]
    pub issuer: String,

    /// Not before time
    #[serde(rename = "nbf", skip_serializing_if = "Option::is_none")]
    pub not_before: Option<CapabilityTime>,

    /// Nonce of payload
    pub nonce: String,

    /// Request ID
    #[serde(rename = "requestId", skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,

    /// Resources
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<Vec<String>>,

    /// Subject of payload
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statement: Option<String>,

    /// Version of payload
    pub version: String,
}

/// Type of Signature
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum SignatureType {
    /// EIP-191 Signature
    #[serde(rename = "eip191")]
    EIP191,
    /// EIP-1271 Signature
    #[serde(rename = "eip1271")]
    EIP1271,
    /// ED25519 signature for solana
    #[serde(rename = "solana:ed25519")]
    SolanaED25519,
    /// ED25519 signature for tezos
    #[serde(rename = "tezos:ed25519")]
    TezosED25519,
    /// SECP256K1 signature for stacks
    #[serde(rename = "stacks:secp256k1")]
    StacksSECP256K1,
    /// SECP256K1 signature for webauthn
    #[serde(rename = "webauthn:p256")]
    WebAuthNP256,
    /// JWS signature
    #[serde(rename = "jws")]
    JWS,
}

impl SignatureType {
    /// Convert signature type to algorithm
    pub fn algorithm(&self) -> Algorithm {
        match self {
            SignatureType::EIP191 => Algorithm::ES256,
            SignatureType::EIP1271 => Algorithm::ES256,
            SignatureType::SolanaED25519 => Algorithm::EdDSA,
            SignatureType::TezosED25519 => Algorithm::EdDSA,
            SignatureType::StacksSECP256K1 => Algorithm::ES256K,
            SignatureType::WebAuthNP256 => Algorithm::ES256,
            SignatureType::JWS => Algorithm::ES256,
        }
    }
}

/// Values for unknown metadata
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum MetadataValue {
    /// Boolean value
    Boolean(bool),
    /// Integer value
    Integer(i64),
    /// Null value
    Null,
    /// String value
    String(String),
}

/// Metadata for signature
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SignatureMetadata {
    /// Algorithm for signature
    pub alg: String,
    /// Key ID for signature
    pub kid: String,
    /// Other metadata
    #[serde(flatten)]
    pub rest: HashMap<String, MetadataValue>,
}

/// Signature of a CACAO
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Signature {
    /// Metadata for signature
    #[serde(rename = "m", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<SignatureMetadata>,
    /// Type of signature
    #[serde(rename = "t")]
    pub r#type: SignatureType,
    /// Signature bytes
    #[serde(rename = "s")]
    pub signature: String,
}

#[derive(Debug)]
/// A sorted version of the metadata that can be used when verifying signatures
pub struct SortedMetadata<'a> {
    /// The header data
    pub header_data: Vec<(&'a str, &'a MetadataValue)>,
    /// The algorithm used
    pub alg: MetadataValue,
    /// The key ID used
    pub kid: MetadataValue,
}

impl<'a> From<&'a SignatureMetadata> for SortedMetadata<'a> {
    fn from(metadata: &'a SignatureMetadata) -> Self {
        let header_data: Vec<_> = metadata.rest.iter().map(|(k, v)| (k.as_str(), v)).collect();
        Self {
            header_data,
            alg: MetadataValue::String(metadata.alg.clone()),
            kid: MetadataValue::String(metadata.kid.clone()),
        }
    }
}

impl<'a> Serialize for SortedMetadata<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let mut header_data: Vec<_> = self
            .header_data
            .iter()
            .map(|(k, v)| (*k, *v))
            .chain(vec![("alg", &self.alg), ("kid", &self.kid)])
            .collect();
        header_data.sort_by(|a, b| a.0.cmp(b.0));
        let mut s = serializer.serialize_map(Some(header_data.len()))?;
        for (k, v) in &header_data {
            s.serialize_entry(k, v)?;
        }
        s.end()
    }
}
