use anyhow::{anyhow, Result};
use base64::engine::{self, Engine as _};
use ceramic_core::Jwk;
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
