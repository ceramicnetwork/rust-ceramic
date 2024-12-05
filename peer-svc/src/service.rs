use std::ops::Range;

use ceramic_core::{NodeId, PeerKey};
use recon::{HashCount, InsertResult, ReconItem, Result as ReconResult, Sha256a};
use tracing::instrument;

use crate::store::PeerDB;
use crate::store::SqlitePool;
use crate::Error;

/// A Service that understands how to process and store Ceramic [`ceramic_core::PeerKey`]s.
/// Implements the [`recon::Store`], [`ceramic_p2p::PeerService`].
#[derive(Debug)]
pub struct PeerService {
    pub(crate) pool: SqlitePool,
}
impl PeerService {
    /// Construct a new interest service from a [`SqlitePool`].
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl recon::Store for PeerService {
    type Key = PeerKey;
    type Hash = Sha256a;

    #[instrument(skip(self))]
    async fn insert_many(
        &self,
        items: &[ReconItem<Self::Key>],
        _informant: NodeId,
    ) -> ReconResult<InsertResult<Self::Key>> {
        let keys = items.iter().map(|item| &item.key).collect::<Vec<_>>();
        Ok(PeerDB::insert_many(&self.pool, &keys)
            .await
            .map_err(Error::from)?)
    }

    #[instrument(skip(self))]
    async fn hash_range(&self, range: Range<&Self::Key>) -> ReconResult<HashCount<Self::Hash>> {
        Ok(PeerDB::hash_range(&self.pool, range)
            .await
            .map_err(Error::from)?)
    }

    #[instrument(skip(self))]
    async fn range(
        &self,
        range: Range<&Self::Key>,
    ) -> ReconResult<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        Ok(Box::new(
            PeerDB::range(&self.pool, range)
                .await
                .map_err(Error::from)?
                .into_iter(),
        ))
    }
    #[instrument(skip(self))]
    async fn first(&self, range: Range<&Self::Key>) -> ReconResult<Option<Self::Key>> {
        Ok(PeerDB::first(&self.pool, range)
            .await
            .map_err(Error::from)?)
    }
    #[instrument(skip(self))]
    async fn middle(&self, range: Range<&Self::Key>) -> ReconResult<Option<Self::Key>> {
        Ok(PeerDB::middle(&self.pool, range)
            .await
            .map_err(Error::from)?)
    }

    #[instrument(skip(self))]
    async fn count(&self, range: Range<&Self::Key>) -> ReconResult<usize> {
        Ok(PeerDB::count(&self.pool, range)
            .await
            .map_err(Error::from)?)
    }

    #[instrument(skip(self))]
    async fn value_for_key(&self, _key: &Self::Key) -> ReconResult<Option<Vec<u8>>> {
        Ok(Some(vec![]))
    }
}

#[async_trait::async_trait]
impl ceramic_p2p::PeerService for PeerService {
    async fn insert(&self, peer: &PeerKey) -> anyhow::Result<()> {
        PeerDB::insert_many(&self.pool, &[peer])
            .await
            .map_err(Error::from)?;
        Ok(())
    }
    async fn delete_range(&self, range: Range<&PeerKey>) -> anyhow::Result<()> {
        PeerDB::delete_range(&self.pool, range)
            .await
            .map_err(Error::from)?;
        Ok(())
    }
    async fn all_peers(&self) -> anyhow::Result<Vec<PeerKey>> {
        Ok(PeerDB::range(
            &self.pool,
            &PeerKey::builder().with_min_expiration().build_fencepost()
                ..&PeerKey::builder().with_max_expiration().build_fencepost(),
        )
        .await
        .map_err(Error::from)?)
    }
}
