use once_cell::sync::Lazy;

static DEFAULT_REVOCATION_PHASEOUT_SECS: Lazy<chrono::Duration> =
    Lazy::new(|| chrono::Duration::new(0, 0).expect("0 is a valid duration"));
static DEFAULT_CLOCK_SKEW: Lazy<chrono::Duration> =
    Lazy::new(|| chrono::Duration::new(5 * 60, 0).expect("5 minutes is a valid duration"));

#[derive(Clone, Debug)]
pub struct VerifyCacaoOpts {
    /// The point in time at which the capability must be valid, defaults to `now`.
    pub at_time: Option<chrono::DateTime<chrono::Utc>>,
    /// How long the capability stays valid for after it was expired.
    pub revocation_phaseout_secs: chrono::Duration,
    /// The clock tolerance when verifying iat, nbf, and exp.
    pub clock_skew: chrono::Duration,
    /// Do not verify expiration time when false.
    pub check_exp: bool,
}

impl Default for VerifyCacaoOpts {
    fn default() -> Self {
        Self {
            at_time: None,
            revocation_phaseout_secs: *DEFAULT_REVOCATION_PHASEOUT_SECS,
            clock_skew: *DEFAULT_CLOCK_SKEW,
            check_exp: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AtTime {
    /// None means now
    At(Option<chrono::DateTime<chrono::Utc>>),
    SkipTimeChecks,
}

#[derive(Clone, Debug)]
pub struct VerifyJwsOpts {
    /// The point in time at which the capability must be valid, defaults to `now`.
    pub at_time: AtTime,
    /// Number of seconds that a revoked key stays valid for after it was revoked
    pub revocation_phaseout_secs: chrono::Duration,
}

impl Default for VerifyJwsOpts {
    fn default() -> Self {
        Self {
            at_time: AtTime::At(None),
            revocation_phaseout_secs: *DEFAULT_REVOCATION_PHASEOUT_SECS,
        }
    }
}

impl From<VerifyJwsOpts> for VerifyCacaoOpts {
    fn from(value: VerifyJwsOpts) -> Self {
        let (at_time, check_exp) = match value.at_time {
            AtTime::At(t) => (t, true),
            AtTime::SkipTimeChecks => (None, false),
        };
        Self {
            at_time,
            revocation_phaseout_secs: value.revocation_phaseout_secs,
            check_exp,
            ..Default::default()
        }
    }
}
