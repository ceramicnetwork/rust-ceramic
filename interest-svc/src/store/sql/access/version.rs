use std::str::FromStr;

use anyhow::anyhow;

use crate::store::{
    sql::{entities::VersionRow, SqlitePool},
    Error, Result,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// It's kind of pointless to roundtrip CARGO_PKG_VERSION through this struct,
/// but it makes it clear how we expect to format our versions in the database.
struct SemVer {
    major: u64,
    minor: u64,
    patch: u64,
}

impl std::fmt::Display for SemVer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl std::str::FromStr for SemVer {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 3 {
            Err(Error::new_invalid_arg(anyhow!(
                "Invalid version. Must have 3 parts: {}",
                s.to_string()
            )))
        } else {
            let major = parts[0].parse().map_err(|_| {
                Error::new_invalid_arg(anyhow!(
                    "Invalid version. Major did not parse: {}",
                    s.to_string()
                ))
            })?;
            let minor = parts[1].parse().map_err(|_| {
                Error::new_invalid_arg(anyhow!(
                    "Invalid version. Minor did not parse: {}",
                    s.to_string()
                ))
            })?;
            let patch = parts[2].parse().map_err(|_| {
                Error::new_invalid_arg(anyhow!(
                    "Invalid version. Patch did not parse: {}",
                    s.to_string()
                ))
            })?;
            Ok(Self {
                major,
                minor,
                patch,
            })
        }
    }
}

#[derive(Debug, Clone)]
/// Access to ceramic version information
pub struct CeramicOneVersion {}

impl CeramicOneVersion {
    /// Fetch the previous version from the database. May be None if no previous version exists.
    pub async fn fetch_previous(pool: &SqlitePool) -> Result<Option<VersionRow>> {
        let current = SemVer::from_str(env!("CARGO_PKG_VERSION"))?;
        VersionRow::_fetch_previous(pool, &current.to_string()).await
    }

    /// Insert the current version into the database
    pub async fn insert_current(pool: &SqlitePool) -> Result<()> {
        let current = SemVer::from_str(env!("CARGO_PKG_VERSION"))?;
        VersionRow::insert_current(pool, &current.to_string()).await
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::store::SqlitePool;

    #[tokio::test]
    async fn insert_version() {
        let mem = SqlitePool::connect_in_memory().await.unwrap();
        CeramicOneVersion::insert_current(&mem).await.unwrap();
    }

    #[tokio::test]
    async fn prev_version() {
        let mem = SqlitePool::connect_in_memory().await.unwrap();
        CeramicOneVersion::fetch_previous(&mem).await.unwrap();
    }
}
