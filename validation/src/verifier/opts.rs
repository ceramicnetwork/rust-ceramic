#[derive(Clone, Debug)]
pub struct VerifyOpts {
    /// The point in time at which the capability must be valid, defaults to `now`
    pub at_time: Option<chrono::DateTime<chrono::Utc>>,
    /// How long the capability stays valid for after it was expired
    pub revocation_phaseout_secs: chrono::Duration,
    /// The clock tolerance when verifying iat, nbf, and exp
    pub clock_skew: chrono::Duration,
    /// Do not verify expiration time when false
    pub check_exp: bool,
}

impl Default for VerifyOpts {
    fn default() -> Self {
        Self {
            at_time: None,
            revocation_phaseout_secs: chrono::Duration::new(0, 0).expect("0 is a valid duration"),
            clock_skew: chrono::Duration::new(5 * 60, 0).expect("5 minutes is a valid duration"),
            check_exp: true,
        }
    }
}
