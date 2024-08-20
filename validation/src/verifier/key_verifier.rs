use anyhow::{anyhow, bail, Context, Result};
use base64::engine::{self, Engine as _};
use ceramic_core::Jwk;
use ceramic_event::unvalidated::signed::Envelope;
use tracing::{debug, warn};

#[async_trait::async_trait]
pub trait Verifier {
    /// Verify the signature. Optoinally requiring it matches a delegated KID
    async fn verify_signature(&self, delegated_kid: Option<&str>) -> Result<()>;
}

#[async_trait::async_trait]
impl Verifier for Envelope {
    async fn verify_signature(&self, delegated_kid: Option<&str>) -> Result<()> {
        let signature = self
            .signature()
            .first()
            .ok_or_else(|| anyhow!("missing signature on signed event"))?;
        let jws_header = self.jws_header().context("event envelope is not a jws")?;
        let protected = signature
            .protected
            .as_ref()
            .ok_or_else(|| anyhow!("missing protected field"))?;

        let did = &jws_header
            .key_id
            .ok_or_else(|| anyhow!("missing jws kid"))?;

        verify_did_jws(
            did,
            protected.as_slice(),
            self.payload().as_slice(),
            jws_header.algorithm,
            signature.signature.as_slice(),
            delegated_kid,
        )
        .await
    }
}

pub(crate) async fn verify_did_jws(
    did: &str,
    header: &[u8],
    payload: &[u8],
    alg: ssi::jwk::Algorithm,
    signature: &[u8],
    delegated_kid: Option<&str>,
) -> Result<()> {
    let did_doc = Jwk::resolve_did(did)
        .await
        .context("failed to resolve did")?;
    if delegated_kid.is_some() && delegated_kid.unwrap() != did_doc.id {
        debug!(
            ?delegated_kid,
            kid = %did_doc.id,
            "jws kid not delegated access"
        );
        bail!("jws kid not delegated access",);
    }

    let jwk = Jwk::new(&did_doc)
        .await
        .map_err(|e| anyhow!("failed to generate jws for did: {}", e))?;

    let header_str = engine::general_purpose::STANDARD_NO_PAD.encode(header);
    let payload_cid = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
    let digest = format!("{header_str}.{payload_cid}");
    let warnings = ssi::jws::verify_bytes_warnable(alg, digest.as_bytes(), &jwk, signature)
        .map_err(|e| anyhow!("failed to verify jws: {}", e))?;

    if !warnings.is_empty() {
        warn!(?warnings, "warnings while verifying jws");
    }
    Ok(())
}
