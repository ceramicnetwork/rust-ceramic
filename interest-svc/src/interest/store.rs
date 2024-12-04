use std::ops::Range;

use ceramic_core::{Interest, NodeId};
use recon::{HashCount, InsertResult, ReconItem, Result as ReconResult, Sha256a};
use tracing::instrument;

use crate::store::CeramicOneInterest;
use crate::Error;
use crate::InterestService;

#[async_trait::async_trait]
impl recon::Store for InterestService {
    type Key = Interest;
    type Hash = Sha256a;

    #[instrument(skip(self))]
    async fn insert_many(
        &self,
        items: &[ReconItem<Self::Key>],
        // the recon::Store trait is shared between InterestService and EventService but only events track a source.
        _informant: NodeId,
    ) -> ReconResult<InsertResult<Interest>> {
        let keys = items.iter().map(|item| &item.key).collect::<Vec<_>>();
        Ok(CeramicOneInterest::insert_many(&self.pool, &keys)
            .await
            .map_err(Error::from)?)
    }

    #[instrument(skip(self))]
    async fn hash_range(&self, range: Range<&Self::Key>) -> ReconResult<HashCount<Self::Hash>> {
        Ok(CeramicOneInterest::hash_range(&self.pool, range)
            .await
            .map_err(Error::from)?)
    }

    #[instrument(skip(self))]
    async fn range(
        &self,
        range: Range<&Self::Key>,
    ) -> ReconResult<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        Ok(Box::new(
            CeramicOneInterest::range(&self.pool, range)
                .await
                .map_err(Error::from)?
                .into_iter(),
        ))
    }
    #[instrument(skip(self))]
    async fn first(&self, range: Range<&Self::Key>) -> ReconResult<Option<Self::Key>> {
        Ok(CeramicOneInterest::first(&self.pool, range)
            .await
            .map_err(Error::from)?)
    }
    #[instrument(skip(self))]
    async fn middle(&self, range: Range<&Self::Key>) -> ReconResult<Option<Self::Key>> {
        Ok(CeramicOneInterest::middle(&self.pool, range)
            .await
            .map_err(Error::from)?)
    }

    #[instrument(skip(self))]
    async fn count(&self, range: Range<&Self::Key>) -> ReconResult<usize> {
        Ok(CeramicOneInterest::count(&self.pool, range)
            .await
            .map_err(Error::from)?)
    }

    #[instrument(skip(self))]
    async fn value_for_key(&self, _key: &Interest) -> ReconResult<Option<Vec<u8>>> {
        Ok(Some(vec![]))
    }
}

#[async_trait::async_trait]
impl ceramic_api::InterestService for InterestService {
    async fn insert(&self, key: Interest) -> anyhow::Result<bool> {
        Ok(CeramicOneInterest::insert(&self.pool, &key).await?)
    }
    async fn range(&self, start: &Interest, end: &Interest) -> anyhow::Result<Vec<Interest>> {
        Ok(CeramicOneInterest::range(&self.pool, start..end).await?)
    }
}
