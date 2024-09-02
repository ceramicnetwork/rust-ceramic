//! Structures for encoding and decoding CACAO capability objects.

use serde::{Deserialize, Serialize};
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
/// A wrapper around the a time value to hide the internal representation (which is currently a string).
/// Use `From<chrono::DateTime<chrono::Utc>>` to construct if needed.
pub struct CapabilityTime(String);

impl From<chrono::DateTime<chrono::Utc>> for CapabilityTime {
    fn from(time: chrono::DateTime<chrono::Utc>) -> Self {
        Self(time.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
    }
}

impl TryFrom<&CapabilityTime> for chrono::DateTime<chrono::Utc> {
    type Error = anyhow::Error;

    fn try_from(input: &CapabilityTime) -> Result<Self, Self::Error> {
        if let Ok(val) =
            chrono::DateTime::parse_from_rfc3339(&input.0).map(|dt| dt.with_timezone(&chrono::Utc))
        {
            Ok(val)
        } else if let Ok(unix_timestamp) = input.0.parse::<i64>() {
            let naive = chrono::DateTime::from_timestamp(unix_timestamp, 0)
                .ok_or_else(|| anyhow::anyhow!("failed to parse as unix timestamp: {}", input.0))?;
            Ok(naive)
        } else {
            anyhow::bail!(format!("failed to parse timestamp: {}", input.0))
        }
    }
}

impl CapabilityTime {
    /// Returns a string representation of the time
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Payload for a CACAO
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Payload {
    /// Audience for payload
    #[serde(rename = "aud")]
    pub audience: String,

    /// Domain for payload
    pub domain: String,

    /// Expiration time
    /// Not using a chrono::DateTime because we need to round trip the exact
    /// value we receive without modifying precision and changning the CID.
    /// The new CAIP proposes using ints but most of our cacaos still use ISO 8601 values.
    #[serde(rename = "exp", skip_serializing_if = "Option::is_none")]
    pub expiration: Option<CapabilityTime>,

    /// Issued at time.
    /// Not using a chrono::DateTime because we need to round trip the exact
    /// value we receive without modifying precision and changning the CID.
    /// The new CAIP proposes using ints but most of our cacaos still use ISO 8601 values.
    #[serde(rename = "iat")]
    pub issued_at: CapabilityTime,

    /// Issuer for payload. For capability will be DID in URI format
    #[serde(rename = "iss")]
    pub issuer: String,

    /// Not before time.
    /// Not using a chrono::DateTime because we need to round trip the exact
    /// value we receive without modifying precision and changning the CID.
    /// The new CAIP proposes using ints but most of our cacaos still use ISO 8601 values.
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

impl Payload {
    /// Parse the iat field as a chrono DateTime
    pub fn issued_at(&self) -> anyhow::Result<chrono::DateTime<chrono::Utc>> {
        (&self.issued_at)
            .try_into()
            .map_err(|e| anyhow::anyhow!("invalid issued_at format: {}", e))
    }

    /// Parse the nbf field as a chrono DateTime
    pub fn not_before(&self) -> anyhow::Result<Option<chrono::DateTime<chrono::Utc>>> {
        self.not_before
            .as_ref()
            .map(|nbf| nbf.try_into())
            .transpose()
            .map_err(|e| anyhow::anyhow!("invalid not_before format: {}", e))
    }

    /// Parse the exp field as a chrono DateTime
    pub fn expiration(&self) -> anyhow::Result<Option<chrono::DateTime<chrono::Utc>>> {
        self.expiration
            .as_ref()
            .map(|exp| exp.try_into())
            .transpose()
            .map_err(|e| anyhow::anyhow!("invalid expiration format: {}", e))
    }
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
pub struct JwsSignatureMetadata {
    /// Algorithm for signature
    pub alg: String,
    /// Key ID for signature
    pub kid: String,
    /// Other metadata
    #[serde(flatten)]
    pub rest: HashMap<String, MetadataValue>,
}

/// Variants for signature metadata
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum SignatureMetadata {
    /// The metadata for a JWS signature
    Jws(JwsSignatureMetadata),
    /// Ipld encoded metadata, used for WebAuthN authorization
    Ipld(ipld_core::ipld::Ipld),
}

impl SignatureMetadata {
    /// Returns the metadata if it's a JWS signature, else error
    pub fn try_as_jws(&self) -> anyhow::Result<&JwsSignatureMetadata> {
        match self {
            SignatureMetadata::Jws(jws) => Ok(jws),
            SignatureMetadata::Ipld(_) => anyhow::bail!("expected JWS signature, found IPLD"),
        }
    }

    /// Returns the metadata if it's a AdditionalAuthenticatorData, else error
    pub fn try_as_ipld(&self) -> anyhow::Result<&ipld_core::ipld::Ipld> {
        match self {
            SignatureMetadata::Jws(_) => anyhow::bail!("Expected IPLD metadata, found JWS"),
            SignatureMetadata::Ipld(ipld) => Ok(ipld),
        }
    }
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
