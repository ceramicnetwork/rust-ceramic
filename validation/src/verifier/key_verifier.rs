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

        let input = VerifyJwsInput {
            did,
            header: protected.as_slice(),
            payload: self.payload().as_slice(),
            alg: jws_header.algorithm,
            signature: signature.signature.as_slice(),
        };
        let params = match delegated_kid {
            Some(delgated_kid) => VerifyDidJwsInput::Delegated {
                input,
                delgated_kid,
            },
            None => VerifyDidJwsInput::Standard { input },
        };

        verify_did_jws(params).await
    }
}

pub(crate) struct VerifyJwsInput<'a> {
    pub(crate) did: &'a str,
    pub(crate) header: &'a [u8],
    pub(crate) payload: &'a [u8],
    pub(crate) alg: ssi::jwk::Algorithm,
    pub(crate) signature: &'a [u8],
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

pub(crate) enum VerifyDidJwsInput<'a> {
    /// Params to verify a JWS granted using delegated (CACAO) access to a kid
    Delegated {
        input: VerifyJwsInput<'a>,
        delgated_kid: &'a str,
    },
    /// Verify the JWS was correctly signed without delegation
    Standard { input: VerifyJwsInput<'a> },
}

/// Verify that a DID signed the header/payload bytes as a JWS using the requested algorithm.
/// If using Delegated (e.g. via CACAO), it will verify the resolved DID matches the `delegated_kid`.
pub(crate) async fn verify_did_jws(params: VerifyDidJwsInput<'_>) -> Result<()> {
    let (params, delegated_kid) = match &params {
        VerifyDidJwsInput::Delegated {
            input,
            delgated_kid,
        } => (input, Some(delgated_kid)),
        VerifyDidJwsInput::Standard { input } => (input, None),
    };

    let did_doc = Jwk::resolve_did(params.did)
        .await
        .context("failed to resolve did")?;
    if delegated_kid.map_or(false, |k| *k != did_doc.id) {
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

    let digest = params.digest();
    let warnings =
        ssi::jws::verify_bytes_warnable(params.alg, digest.as_bytes(), &jwk, params.signature)
            .map_err(|e| anyhow!("failed to verify jws: {}", e))?;

    if !warnings.is_empty() {
        warn!(?warnings, "warnings while verifying jws");
    }
    Ok(())
}
