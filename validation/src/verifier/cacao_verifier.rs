use anyhow::Result;
use ceramic_event::unvalidated::signed::cacao::{Capability, HeaderType, SignatureType};
use once_cell::sync::Lazy;

use crate::signature::{pkh_ethereum::PkhEthereum, pkh_solana::PkhSolana};

static DEFAULT_REVOCATION_PHASEOUT_SECS: Lazy<chrono::Duration> =
    Lazy::new(|| chrono::Duration::new(0, 0).expect("0 is a valid duration"));
static DEFAULT_CLOCK_SKEW: Lazy<chrono::Duration> =
    Lazy::new(|| chrono::Duration::new(5 * 60, 0).expect("5 minutes is a valid duration"));

#[derive(Clone, Debug)]
pub struct VerifyOpts {
    /// The point in time at which the capability must be valid, defaults to `now`.
    pub at_time: Option<chrono::DateTime<chrono::Utc>>,
    /// How long the capability stays valid for after it was expired.
    pub revocation_phaseout_secs: chrono::Duration,
    /// The clock tolerance when verifying iat, nbf, and exp.
    pub clock_skew: chrono::Duration,
    /// Do not verify expiration time when false.
    pub check_exp: bool,
}

impl Default for VerifyOpts {
    fn default() -> Self {
        Self {
            at_time: None,
            revocation_phaseout_secs: *DEFAULT_REVOCATION_PHASEOUT_SECS,
            clock_skew: *DEFAULT_CLOCK_SKEW,
            check_exp: true,
        }
    }
}

pub trait Verifier {
    #[allow(dead_code)]
    fn verify(&self, opts: &VerifyOpts) -> Result<()>;
    fn verify_time_checks(&self, opts: &VerifyOpts) -> anyhow::Result<()>;
}

impl Verifier for Capability {
    fn verify(&self, opts: &VerifyOpts) -> anyhow::Result<()> {
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
                SignatureType::JWS => todo!(),
            },
        }
    }

    fn verify_time_checks(&self, opts: &VerifyOpts) -> anyhow::Result<()> {
        let at_time = opts.at_time.unwrap_or_else(chrono::Utc::now);

        if self.payload.issued_at > at_time + opts.clock_skew
            || self
                .payload
                .not_before
                .map_or(false, |nb| nb > at_time + opts.clock_skew)
        {
            anyhow::bail!("CACAO is not valid yet")
        }
        if opts.check_exp {
            if let Some(exp) = self.payload.expiration {
                if exp + opts.revocation_phaseout_secs + opts.clock_skew < at_time {
                    anyhow::bail!("CACAO has expired")
                }
            }
        }

        Ok(())
    }
}
