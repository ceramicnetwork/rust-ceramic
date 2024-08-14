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

    /// Insert new keys into the key space.
    /// Returns true for each key if it did not previously exist, in the
    /// same order as the input iterator.
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

    /// Return the hash of all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    /// Returns ReconResult<(Hash, count), Err>
    #[instrument(skip(self))]
    async fn hash_range(&self, range: Range<&Self::Key>) -> ReconResult<HashCount<Self::Hash>> {
        Ok(CeramicOneInterest::hash_range(&self.pool, range)
            .await
            .map_err(Error::from)?)
    }

    /// Return all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    #[instrument(skip(self))]
    async fn range(
        &self,
        range: Range<&Self::Key>,

        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        Ok(Box::new(
            CeramicOneInterest::range(&self.pool, range, offset, limit)
                .await
                .map_err(Error::from)?
                .into_iter(),
        ))
    }

    /// Return all keys and values in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    #[instrument(skip(self))]
    async fn range_with_values(
        &self,
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        let res = CeramicOneInterest::range(&self.pool, range, offset, limit)
            .await
            .map_err(Error::from)?;
        Ok(Box::new(res.into_iter().map(|key| (key, vec![]))))
    }
    /// Return the number of keys within the range.
    #[instrument(skip(self))]
    async fn count(&self, range: Range<&Self::Key>) -> ReconResult<usize> {
        Ok(CeramicOneInterest::count(&self.pool, range)
            .await
            .map_err(Error::from)?)
    }

    /// value_for_key returns
    /// Ok(Some(value)) if stored,
    /// Ok(None) if not stored, and
    /// Err(e) if retrieving failed.
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
    async fn range(
        &self,
        start: &Interest,
        end: &Interest,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<Interest>> {
        Ok(CeramicOneInterest::range(&self.pool, start..end, offset, limit).await?)
    }
}
