//! Metadata information and types for ceramic

include!(concat!(env!("OUT_DIR"), "/built.rs"));

/// Version type
#[derive(Debug, serde::Serialize)]
pub struct Version {
    /// Version string
    pub version: String,
    /// Target arch
    pub arch: String,
    /// Target OS
    pub os: String,
    /// Git tag
    pub commit: String,
}

impl Default for Version {
    fn default() -> Self {
        Self {
            version: PKG_VERSION.to_string(),
            arch: CFG_TARGET_ARCH.to_string(),
            os: CFG_OS.to_string(),
            commit: GIT_VERSION.unwrap_or_default().to_string(),
        }
    }
}
