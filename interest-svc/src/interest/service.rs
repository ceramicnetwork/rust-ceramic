use crate::store::SqlitePool;

/// A Service that understands how to process and store Ceramic Interests.
/// Implements the [`recon::Store`], [`iroh_bitswap::Store`], and [`ceramic_api::EventStore`] traits for [`ceramic_core::Interest`].
#[derive(Debug)]
pub struct InterestService {
    pub(crate) pool: SqlitePool,
}
impl InterestService {
    /// Construct a new interest service from a [`SqlitePool`].
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}
