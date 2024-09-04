//! The Event Service provides an API for ingesting and querying Ceramic Events.
#![warn(missing_docs)]

mod error;
mod event;
mod interest;
pub mod store;
#[cfg(test)]
mod tests;

pub use error::Error;
pub use event::{BlockStore, CeramicEventService};
pub use interest::CeramicInterestService;

use std::sync::Arc;

use store::{CeramicOneVersion, SqlitePool};

pub(crate) type Result<T> = std::result::Result<T, Error>;

/// The ceramic service holds the logic needed by the other components (e.g. api, recon) to access the store and process events
/// in a way that makes sense to the ceramic protocol, and not just as raw bytes.
#[derive(Debug)]
pub struct CeramicService {
    pub(crate) interest: Arc<CeramicInterestService>,
    pub(crate) event: Arc<CeramicEventService>,
}

impl CeramicService {
    /// Create a new CeramicService and process undelivered events if requested
    pub async fn try_new(pool: SqlitePool, enable_event_validation: bool) -> Result<Self> {
        // In the future, we may need to check the previous version to make sure we're not downgrading and risking data loss
        CeramicOneVersion::insert_current(&pool).await?;
        let interest = Arc::new(CeramicInterestService::new(pool.clone()));
        let event = Arc::new(CeramicEventService::new(pool, enable_event_validation).await?);
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
}
