use anyhow::{Context, Result};
use base64::Engine as _;
use ceramic_event::unvalidated::signed::cacao::{
    Capability, HeaderType, SignatureType, SortedMetadata,
};

use super::{key_verifier::verify_did_jws, opts::VerifyOpts};

use crate::signature::{pkh_ethereum::PkhEthereum, pkh_solana::PkhSolana};

#[async_trait::async_trait]
pub trait Verifier {
    /// Verify the signature of the CACAO and ensure it is valid.
    async fn verify_signature(&self, opts: &VerifyOpts) -> Result<()>;
    /// Verify the time checks for the CACAO using the `VerifyOpts`
    fn verify_time_checks(&self, opts: &VerifyOpts) -> Result<()>;
}

#[async_trait::async_trait]
impl Verifier for Capability {
    async fn verify_signature(&self, opts: &VerifyOpts) -> anyhow::Result<()> {
        // verify signed from js-did is not required as it won't deserialize without a signature
        // is that something that is ever expected?
        self.verify_time_checks(opts)?;

        match self.header.r#type {
            HeaderType::EIP4361 => PkhEthereum::verify(self),
            HeaderType::CAIP122 => match self.signature.r#type {
                SignatureType::EIP191 => PkhEthereum::verify(self),
                SignatureType::EIP1271 => todo!(),
                SignatureType::SolanaED25519 => PkhSolana::verify(self),
                SignatureType::TezosED25519 => todo!(),
                SignatureType::StacksSECP256K1 => todo!(),
                SignatureType::WebAuthNP256 => todo!(),
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

                    let payload =
                        serde_json::to_vec(&self.payload).context("failed to serialize payload")?;
                    let header = serde_json::to_vec(&SortedMetadata::from(meta))
                        .context("failed to seralize metadata")?;
                    let sig = base64::prelude::BASE64_URL_SAFE_NO_PAD
                        .decode(self.signature.signature.as_bytes())
                        .map_err(|e| anyhow::anyhow!("invalid signature: {}", e))?;
                    verify_did_jws(
                        did,
                        header.as_slice(),
                        payload.as_slice(),
                        self.signature.r#type.algorithm(),
                        &sig,
                        None,
                    )
                    .await
                }
            },
        }
    }

    fn verify_time_checks(&self, opts: &VerifyOpts) -> anyhow::Result<()> {
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
