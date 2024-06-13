//! Structures for encoding and decoding CACAO capability objects.

use serde::{Deserialize, Serialize};

/// Capability object, see https://github.com/ChainAgnostic/CAIPs/blob/main/CAIPs/caip-74.md
#[derive(Debug, Serialize, Deserialize)]
pub struct Capability {
    #[serde(rename = "h")]
    header: Header,
    #[serde(rename = "p")]
    payload: Payload,
    #[serde(rename = "s")]
    signature: Signature,
}

impl Capability {
    /// Get the header
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Get the payload
    pub fn payload(&self) -> &Payload {
        &self.payload
    }

    /// Get the signature
    pub fn signature(&self) -> &Signature {
        &self.signature
    }
}
/// Header for a CACAO
#[derive(Debug, Serialize, Deserialize)]
pub struct Header {
    #[serde(rename = "t")]
    r#type: String,
}

impl Header {
    /// Get the type of the CACAO
    pub fn r#type(&self) -> &str {
        &self.r#type
    }
}

/// Payload for a CACAO
#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    domain: String,

    #[serde(rename = "iss")]
    issuer: String,

    #[serde(rename = "aud")]
    audience: String,

    version: String,

    nonce: String,

    #[serde(rename = "iat")]
    issued_at: String,

    #[serde(rename = "nbf", skip_serializing_if = "Option::is_none")]
    not_before: Option<String>,

    #[serde(rename = "exp", skip_serializing_if = "Option::is_none")]
    expiration: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    statement: Option<String>,

    #[serde(rename = "requestId", skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    resources: Option<Vec<String>>,
}

impl Payload {
    /// Get the domain
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Get the issuer as a DID pkh string
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Get the audience as a URI
    pub fn audience(&self) -> &str {
        &self.audience
    }

    /// Get the version
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Get the nonce
    pub fn nonce(&self) -> &str {
        &self.nonce
    }

    /// Get the issued at date and time as a RFC3339 string
    pub fn issued_at(&self) -> &str {
        &self.issued_at
    }

    /// Get the not before date and time as a RFC3339 string
    pub fn not_before(&self) -> Option<&String> {
        self.not_before.as_ref()
    }

    /// Get the expiration date and time as a RFC3339 string
    pub fn expiration(&self) -> Option<&String> {
        self.expiration.as_ref()
    }

    /// Get the statement
    pub fn statement(&self) -> Option<&String> {
        self.statement.as_ref()
    }

    /// Get the request Id
    pub fn request_id(&self) -> Option<&String> {
        self.request_id.as_ref()
    }

    /// Get the resources
    pub fn resources(&self) -> Option<&[String]> {
        self.resources.as_ref().map(|r| &r[..])
    }
}

/// Signature of a CACAO
#[derive(Debug, Serialize, Deserialize)]
pub struct Signature {
    #[serde(rename = "t")]
    r#type: String,
    #[serde(rename = "s")]
    signature: String,
}

impl Signature {
    /// Get the type of the signature
    pub fn r#type(&self) -> &str {
        &self.r#type
    }
    /// Get the signature bytes as hex encoded string prefixed with 0x
    pub fn signature(&self) -> &str {
        &self.signature
    }
}
