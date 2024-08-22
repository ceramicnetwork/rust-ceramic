use anyhow::{bail, Context, Result};
use base64::Engine as _;
use ceramic_core::Jwk;
use ceramic_event::unvalidated::signed::cacao::{
    Capability, HeaderType, SignatureType, SortedMetadata,
};
use ssi::did_resolve::ResolutionInputMetadata;

use super::{
    jws::{verify_jws, VerifyJwsInput},
    opts::VerifyCacaoOpts,
};

use crate::signature::{pkh_ethereum::PkhEthereum, pkh_solana::PkhSolana};

#[async_trait::async_trait]
pub trait Verifier {
    /// Verify the signature of the CACAO and ensure it is valid.
    async fn verify_signature(&self, opts: &VerifyCacaoOpts) -> Result<()>;
    /// Verify the time checks for the CACAO using the `VerifyOpts`
    fn verify_time_checks(&self, opts: &VerifyCacaoOpts) -> Result<()>;
}

#[async_trait::async_trait]
impl Verifier for Capability {
    async fn verify_signature(&self, opts: &VerifyCacaoOpts) -> anyhow::Result<()> {
        // verify signed from js-did is not required as it won't deserialize without a signature
        // is that something that is ever expected?
        self.verify_time_checks(opts)?;

        match self.header.r#type {
            HeaderType::EIP4361 => PkhEthereum::verify(self),
            HeaderType::CAIP122 => match self.signature.r#type {
                SignatureType::EIP191 => PkhEthereum::verify(self),
                SignatureType::EIP1271 => bail!("EIP1271 signature validation is unimplmented"),
                SignatureType::SolanaED25519 => PkhSolana::verify(self),
                SignatureType::TezosED25519 => bail!("Tezos signature validation is unimplmented"),
                SignatureType::StacksSECP256K1 => {
                    bail!("Stacks signature validation is unimplmented")
                }
                SignatureType::WebAuthNP256 => {
                    bail!("WebAuthN signature validation is unimplmented")
                }
                SignatureType::JWS => {
                    let meta = if let Some(meta) = &self.signature.metadata {
                        meta
                    } else {
                        anyhow::bail!("no metadata found for jws");
                    };
                    let did = meta
                        .kid
                        .split_once("#")
                        .map_or(meta.kid.as_str(), |(k, _)| k);
                    // TODO: use time here?
                    let signer_did = Jwk::resolve_did(did, &ResolutionInputMetadata::default())
                        .await
                        .context("failed to resolve did")?;

                    let jwk = Jwk::new(&signer_did)
                        .await
                        .map_err(|e| anyhow::anyhow!("failed to generate jwk for did: {}", e))?;

                    let payload =
                        serde_json::to_vec(&self.payload).context("failed to serialize payload")?;
                    let header = serde_json::to_vec(&SortedMetadata::from(meta))
                        .context("failed to seralize metadata")?;
                    let sig = base64::prelude::BASE64_URL_SAFE_NO_PAD
                        .decode(self.signature.signature.as_bytes())
                        .map_err(|e| anyhow::anyhow!("invalid signature: {}", e))?;
                    verify_jws(VerifyJwsInput {
                        jwk: &jwk,
                        header: header.as_slice(),
                        payload: payload.as_slice(),
                        alg: self.signature.r#type.algorithm(),
                        signature: &sig,
                    })
                    .await
                }
            },
        }
    }

    fn verify_time_checks(&self, opts: &VerifyCacaoOpts) -> anyhow::Result<()> {
        let at_time = opts.at_time.unwrap_or_else(chrono::Utc::now);

        if self.payload.issued_at()? > at_time + opts.clock_skew
            || self
                .payload
                .not_before()?
                .map_or(false, |nb| nb > at_time + opts.clock_skew)
        {
            anyhow::bail!("CACAO is not valid yet")
        }
        if opts.check_exp {
            if let Some(exp) = self.payload.expiration()? {
                if exp + opts.revocation_phaseout_secs + opts.clock_skew < at_time {
                    anyhow::bail!("CACAO has expired")
                }
            }
        }

        Ok(())
    }
}
