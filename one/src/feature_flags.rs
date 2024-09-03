use anyhow::{anyhow, Result};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ExperimentalFeatureFlags {
    None,
    Authentication,
    EventValidation,
}

impl std::str::FromStr for ExperimentalFeatureFlags {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.splitn(2, '=').collect();
        if parts.len() != 2 {
            return Err(anyhow!("Invalid key=value format: '{}'", s));
        }

        match parts[0].to_ascii_lowercase().as_str() {
            "authentication" => match parts[1].trim().to_ascii_lowercase().as_str() {
                "true" => Ok(ExperimentalFeatureFlags::Authentication),
                "false" => Ok(ExperimentalFeatureFlags::None),
                _ => Err(anyhow!(
                    "invalid value for the 'authentication' flag, should be 'true' or 'false'"
                )),
            },
            "event-validation" => match parts[1].trim().to_ascii_lowercase().as_str() {
                "true" => Ok(ExperimentalFeatureFlags::EventValidation),
                "false" => Ok(ExperimentalFeatureFlags::None),
                _ => Err(anyhow!(
                    "invalid value for the 'event-validation' flag, should be 'true' or 'false'"
                )),
            },
            _ => Err(anyhow!("invalid value")),
        }
    }
}

impl std::fmt::Display for ExperimentalFeatureFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExperimentalFeatureFlags::None => write!(f, "none"),
            ExperimentalFeatureFlags::Authentication => write!(f, "authentication"),
            ExperimentalFeatureFlags::EventValidation => write!(f, "event-validation"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum FeatureFlags {
    None,
}

impl std::fmt::Display for FeatureFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FeatureFlags::None => write!(f, "none"),
        }
    }
}

impl std::str::FromStr for FeatureFlags {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "none" => Ok(FeatureFlags::None),
            _ => Err(anyhow!("invalid value")),
        }
    }
}
