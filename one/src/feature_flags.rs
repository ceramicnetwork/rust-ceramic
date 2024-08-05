use anyhow::{anyhow, Result};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ExperimentalFeatureFlags {
    None,
    Authentication,
}

impl std::str::FromStr for ExperimentalFeatureFlags {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "none" => Ok(ExperimentalFeatureFlags::None),
            "authentication" => Ok(ExperimentalFeatureFlags::Authentication),
            _ => Err(anyhow!("invalid value")),
        }
    }
}

impl std::fmt::Display for ExperimentalFeatureFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExperimentalFeatureFlags::None => write!(f, "none"),
            ExperimentalFeatureFlags::Authentication => write!(f, "authentication"),
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
