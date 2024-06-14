mod error;
mod event;
mod interest;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use ceramic_store::{DataMigrator, SqlitePool};
pub use error::Error;
pub use event::CeramicEventService;
pub use interest::CeramicInterestService;

pub(crate) type Result<T> = std::result::Result<T, Error>;

/// The ceramic service holds the logic needed by the other components (e.g. api, recon) to access the store and process events
/// in a way that makes sense to the ceramic protocol, and not just as raw bytes.
#[derive(Debug)]
pub struct CeramicService {
    pub(crate) interest: Arc<CeramicInterestService>,
    pub(crate) event: Arc<CeramicEventService>,
}

impl CeramicService {
    /// Create a new CeramicService
    pub async fn try_new(pool: SqlitePool) -> Result<Self> {
        let interest = Arc::new(CeramicInterestService::new(pool.clone()));
        let event = Arc::new(CeramicEventService::new(pool).await?);
        Ok(Self { interest, event })
    }

    /// Get the interest service
    pub fn interest_service(&self) -> &Arc<CeramicInterestService> {
        &self.interest
    }

    /// Get the event service
    pub fn event_service(&self) -> &Arc<CeramicEventService> {
        &self.event
    }

    /// Get the data migrator
    pub async fn data_migrator(&self) -> Result<DataMigrator> {
        let m = DataMigrator::try_new(self.event.pool.clone()).await?;
        Ok(m)
    }
}
