use anyhow::Result;

use ceramic_store::SqlitePool;

/// A Service that understands how to process and store Ceramic Interests.
/// Implements the [`recon::Store`], [`iroh_bitswap::Store`], and [`ceramic_api::AccessModelStore`] traits for [`ceramic_core::Interest`].
pub struct CeramicInterestService {
    pub(crate) pool: SqlitePool,
}
impl CeramicInterestService {
    pub async fn new(pool: SqlitePool) -> Result<Self> {
        Ok(Self { pool })
    }
}