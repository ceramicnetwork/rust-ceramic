use std::ops::Range;

use ceramic_core::EventId;

use cid::Cid;
use iroh_bitswap::Block;
use recon::{HashCount, InsertResult, ReconItem, Result as ReconResult, Sha256a};

use crate::event::CeramicEventService;

#[async_trait::async_trait]
impl recon::Store for CeramicEventService {
    type Key = EventId;
    type Hash = Sha256a;

    async fn insert(&self, item: &ReconItem<'_, Self::Key>) -> ReconResult<bool> {
        self.store.insert(item).await

        // if no value, we store it? do nothing?
        // parse the value into a ceramic event
        // we check if it exists already and return true/false
        // check if all conditions are met to store: prev exists, etc
        // store it
    }
    /// Insert new keys into the key space.
    /// Returns true for each key if it did not previously exist, in the
    /// same order as the input iterator.
    async fn insert_many(&self, items: &[ReconItem<'_, Self::Key>]) -> ReconResult<InsertResult> {
        self.store.insert_many(items).await
    }

    /// Return the hash of all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    /// Returns ReconResult<(Hash, count), Err>
    async fn hash_range(&self, range: Range<&Self::Key>) -> ReconResult<HashCount<Self::Hash>> {
        self.store.hash_range(range).await
    }

    /// Return all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    async fn range(
        &self,
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        self.store.range(range, offset, limit).await
    }

    /// Return all keys and values in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    async fn range_with_values(
        &self,
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        self.store.range_with_values(range, offset, limit).await
    }
    /// Return the number of keys within the range.
    async fn count(&self, range: Range<&Self::Key>) -> ReconResult<usize> {
        self.store.count(range).await
    }

    /// value_for_key returns
    /// Ok(Some(value)) if stored,
    /// Ok(None) if not stored, and
    /// Err(e) if retrieving failed.
    async fn value_for_key(&self, key: &Self::Key) -> ReconResult<Option<Vec<u8>>> {
        self.store.value_for_key(key).await
    }
}

#[async_trait::async_trait]
impl iroh_bitswap::Store for CeramicEventService {
    async fn get_size(&self, cid: &Cid) -> anyhow::Result<usize> {
        self.store.get_size(cid).await
    }
    async fn get(&self, cid: &Cid) -> anyhow::Result<Block> {
        self.store.get(cid).await
    }
    async fn has(&self, cid: &Cid) -> anyhow::Result<bool> {
        self.store.has(cid).await
    }
    async fn put(&self, block: &Block) -> anyhow::Result<bool> {
        self.store.put(block).await
    }
}

#[async_trait::async_trait]
impl ceramic_api::AccessModelStore for CeramicEventService {
    async fn insert_many(&self, items: &[(EventId, Vec<u8>)]) -> anyhow::Result<Vec<bool>> {
        self.store.insert_many(items).await
    }

    async fn range_with_values(
        &self,
        range: Range<EventId>,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<(Cid, Vec<u8>)>> {
        self.store.range_with_values(range, offset, limit).await
    }
    async fn value_for_order_key(&self, key: &EventId) -> anyhow::Result<Option<Vec<u8>>> {
        self.store.value_for_order_key(key).await
    }

    async fn value_for_cid(&self, key: &Cid) -> anyhow::Result<Option<Vec<u8>>> {
        self.store.value_for_cid(key).await
    }

    async fn events_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
    ) -> anyhow::Result<(i64, Vec<Cid>)> {
        self.store
            .events_since_highwater_mark(highwater, limit)
            .await
    }

    async fn get_block(&self, cid: &Cid) -> anyhow::Result<Option<Vec<u8>>> {
        self.store.get_block(cid).await
    }
}
