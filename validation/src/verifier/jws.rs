use anyhow::{anyhow, Result};
use base64::engine::{self, Engine as _};
use ceramic_core::Jwk;
use ceramic_event::unvalidated::signed::cacao::{JwsSignatureMetadata, MetadataValue};
use serde::{ser::SerializeMap as _, Serialize};
use tracing::warn;

pub struct VerifyJwsInput<'a> {
    pub jwk: &'a Jwk,
    pub header: &'a [u8],
    pub payload: &'a [u8],
    pub alg: ssi::jwk::Algorithm,
    pub signature: &'a [u8],
}

impl<'a> VerifyJwsInput<'a> {
    fn digest(&self) -> String {
        let header_str = engine::general_purpose::STANDARD_NO_PAD.encode(self.header);
        let payload_cid = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(self.payload);
        // JWS spec requires that the digest be the base64 url encoded header.payload
        // see the RFC for details https://datatracker.ietf.org/doc/html/rfc7515#section-5.2
        format!("{header_str}.{payload_cid}")
    }
}

/// Verify that a DID signed the header/payload bytes as a JWS using the requested algorithm.
/// If using Delegated (e.g. via CACAO aud), it will verify the resolved DID matches the `delegated_kid`.
pub async fn verify_jws(params: VerifyJwsInput<'_>) -> Result<()> {
    let digest = params.digest();
    let warnings = ssi::jws::verify_bytes_warnable(
        params.alg,
        digest.as_bytes(),
        params.jwk,
        params.signature,
    )
    .map_err(|e| anyhow!("failed to verify jws: {}", e))?;

    if !warnings.is_empty() {
        warn!(?warnings, "warnings while verifying jws");
    }
    Ok(())
}

#[derive(Debug)]
/// A sorted version of the metadata that can be used when verifying signatures
pub struct SortedJwsMetadata<'a> {
    /// The header data
    pub header_data: Vec<(&'a str, &'a MetadataValue)>,
    /// The algorithm used
    pub alg: MetadataValue,
    /// The key ID used
    pub kid: MetadataValue,
}

impl<'a> From<&'a JwsSignatureMetadata> for SortedJwsMetadata<'a> {
    fn from(metadata: &'a JwsSignatureMetadata) -> Self {
        let header_data: Vec<_> = metadata.rest.iter().map(|(k, v)| (k.as_str(), v)).collect();
        Self {
            header_data,
            alg: MetadataValue::String(metadata.alg.clone()),
            kid: MetadataValue::String(metadata.alg.clone()),
        }
    }
}

impl<'a> Serialize for SortedJwsMetadata<'a> {
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
