use std::ops::Range;

use ceramic_core::Interest;
use ceramic_store::CeramicOneInterest;
use recon::{HashCount, InsertResult, ReconItem, Result as ReconResult, Sha256a};
use tracing::instrument;

use crate::CeramicInterestService;

#[async_trait::async_trait]
impl recon::Store for CeramicInterestService {
    type Key = Interest;
    type Hash = Sha256a;

    #[instrument(skip(self))]
    async fn insert(&self, item: &ReconItem<'_, Self::Key>) -> ReconResult<bool> {
        Ok(CeramicOneInterest::insert(&self.pool, item.key).await?)
    }

    /// Insert new keys into the key space.
    /// Returns true for each key if it did not previously exist, in the
    /// same order as the input iterator.
    #[instrument(skip(self))]
    async fn insert_many(&self, items: &[ReconItem<'_, Self::Key>]) -> ReconResult<InsertResult> {
        let keys = items.iter().map(|item| item.key).collect::<Vec<_>>();
        Ok(CeramicOneInterest::insert_many(&self.pool, &keys).await?)
    }

    /// Return the hash of all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    /// Returns ReconResult<(Hash, count), Err>
    #[instrument(skip(self))]
    async fn hash_range(&self, range: Range<&Self::Key>) -> ReconResult<HashCount<Self::Hash>> {
        Ok(CeramicOneInterest::hash_range(&self.pool, range).await?)
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
                .await?
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
        let res = CeramicOneInterest::range(&self.pool, range, offset, limit).await?;
        Ok(Box::new(res.into_iter().map(|key| (key, vec![]))))
    }
    /// Return the number of keys within the range.
    #[instrument(skip(self))]
    async fn count(&self, range: Range<&Self::Key>) -> ReconResult<usize> {
        Ok(CeramicOneInterest::count(&self.pool, range).await?)
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
impl ceramic_api::AccessInterestStore for CeramicInterestService {
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
