use anyhow::{anyhow, Result};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FeatureFlags {
    Authentication(bool),
    EventValidation(bool),
}

impl std::str::FromStr for FeatureFlags {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.splitn(2, '=').collect();
        if parts.len() != 2 {
            return Err(anyhow!("Invalid key=value format: '{}'", s));
        }

        match parts[0].to_ascii_lowercase().as_str() {
            "authentication" => Ok(FeatureFlags::Authentication(
                parts[1].trim().to_ascii_lowercase().parse().expect(
                    "Invalid value for the 'authentication' flag, should be 'true' or 'false'",
                ),
            )),
            "event-validation" => Ok(FeatureFlags::EventValidation(
                parts[1].trim().to_ascii_lowercase().parse().expect(
                    "Invalid value for the 'event-validation' flag, should be 'true' or 'false'",
                ),
            )),
            _ => Err(anyhow!("invalid value")),
        }
    }
}

impl std::fmt::Display for FeatureFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FeatureFlags::Authentication(b) => write!(f, "authentication={}", b),
            FeatureFlags::EventValidation(b) => write!(f, "event-validation={}", b),
        }
    }
}
