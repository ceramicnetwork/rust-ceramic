use anyhow::anyhow;
use serde::{Deserialize, Serialize};

/// A resume token/highwater mark type for retrieving keys since a given point (a string wrapper).
/// Could be a generic on Recon/Store, or an enum like ResumeToken(EventFeed(String)) if needed.
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct ResumeToken(String);

impl std::fmt::Display for ResumeToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for ResumeToken {
    fn default() -> Self {
        Self("0".to_string())
    }
}

impl ResumeToken {
    /// Create a new ResumeToken
    pub fn new(value: String) -> Self {
        Self(value)
    }

    /// The resume token version that is passed to the client.
    /// Possibly want to encode somehow, and decode when re-creating but for now it's just a string integer value.
    pub fn into_resume_token(self) -> String {
        self.0
    }
}

impl TryFrom<&ResumeToken> for i64 {
    type Error = anyhow::Error;

    fn try_from(value: &ResumeToken) -> Result<Self, Self::Error> {
        value
            .0
            .parse::<i64>()
            .map_err(|e| anyhow!("Invalid highwater value (should be an integer): {:?}", e))
    }
}

impl TryFrom<ResumeToken> for i64 {
    type Error = anyhow::Error;

    fn try_from(value: ResumeToken) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}
