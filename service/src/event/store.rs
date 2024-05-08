use anyhow::anyhow;
use ceramic_core::{EventId, RangeOpen};
use ceramic_store::{CeramicOneBlock, CeramicOneEvent};
use cid::Cid;
use iroh_bitswap::Block;
use recon::{HashCount, InsertResult, ReconItem, Result as ReconResult, Sha256a};

use crate::event::CeramicEventService;

#[async_trait::async_trait]
impl recon::Store for CeramicEventService {
    type Key = EventId;
    type Hash = Sha256a;

    async fn insert(&self, item: &ReconItem<'_, Self::Key>) -> ReconResult<bool> {
        let val = item.value.unwrap_or_default();
        let res =
            CeramicOneEvent::insert_raw_carfiles(&self.pool, &[(item.key.clone(), val)]).await?;
        Ok(res.keys.first().copied().unwrap_or(false))

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
        let items = items
            .iter()
            .filter_map(|item| item.value.map(|val| (item.key.clone(), val)))
            .collect::<Vec<_>>();
        let res = CeramicOneEvent::insert_raw_carfiles(&self.pool, &items[..]).await?;
        Ok(res)
    }

    /// Return the hash of all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    /// Returns ReconResult<(Hash, count), Err>
    async fn hash_range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> ReconResult<HashCount<Self::Hash>> {
        let res = CeramicOneEvent::hash_range(&self.pool, left_fencepost, right_fencepost).await?;
        Ok(res)
    }

    /// Return all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    async fn range(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        Ok(Box::new(
            CeramicOneEvent::range(&self.pool, left_fencepost, right_fencepost, offset, limit)
                .await?
                .into_iter(),
        ))
    }

    /// Return all keys and values in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    ///
    /// Offset and limit values are applied within the range of keys.
    async fn range_with_values(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        Ok(Box::new(
            CeramicOneEvent::range_with_values(
                &self.pool,
                left_fencepost,
                right_fencepost,
                offset,
                limit,
            )
            .await?
            .into_iter(),
        ))
    }
    /// Return the number of keys within the range.
    async fn count(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> ReconResult<usize> {
        Ok(CeramicOneEvent::count(&self.pool, left_fencepost, right_fencepost).await?)
    }

    /// Return the first key within the range.
    async fn first(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> ReconResult<Option<Self::Key>> {
        Ok(CeramicOneEvent::first(&self.pool, left_fencepost, right_fencepost).await?)
    }
    /// Return the last key within the range.
    async fn last(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> ReconResult<Option<Self::Key>> {
        Ok(CeramicOneEvent::last(&self.pool, left_fencepost, right_fencepost).await?)
    }

    /// Return the first and last keys within the range.
    /// If the range contains only a single key it will be returned as both first and last.
    async fn first_and_last(
        &self,
        left_fencepost: &Self::Key,
        right_fencepost: &Self::Key,
    ) -> ReconResult<Option<(Self::Key, Self::Key)>> {
        Ok(CeramicOneEvent::first_and_last(&self.pool, left_fencepost, right_fencepost).await?)
    }

    /// value_for_key returns
    /// Ok(Some(value)) if stored,
    /// Ok(None) if not stored, and
    /// Err(e) if retrieving failed.
    async fn value_for_key(&self, key: &Self::Key) -> ReconResult<Option<Vec<u8>>> {
        Ok(CeramicOneEvent::value_by_order_key(&self.pool, key).await?)
    }

    /// Report all keys in the range that are missing a value.
    async fn keys_with_missing_values(
        &self,
        range: RangeOpen<Self::Key>,
    ) -> ReconResult<Vec<Self::Key>> {
        Ok(CeramicOneEvent::keys_with_missing_values(&self.pool, range).await?)
    }
}

#[async_trait::async_trait]
impl iroh_bitswap::Store for CeramicEventService {
    async fn get_size(&self, cid: &Cid) -> anyhow::Result<usize> {
        Ok(CeramicOneBlock::get_size(&self.pool, cid).await?)
    }
    async fn get(&self, cid: &Cid) -> anyhow::Result<Block> {
        let maybe = CeramicOneBlock::get(&self.pool, cid).await?;
        maybe.ok_or_else(|| anyhow!("block {} does not exist", cid))
    }
    async fn has(&self, cid: &Cid) -> anyhow::Result<bool> {
        Ok(CeramicOneBlock::has(&self.pool, cid).await?)
    }
    async fn put(&self, block: &Block) -> anyhow::Result<bool> {
        Ok(CeramicOneBlock::put(&self.pool, block).await?)
    }
}

#[async_trait::async_trait]
impl ceramic_api::AccessModelStore for CeramicEventService {
    async fn insert_many(
        &self,
        items: &[(EventId, Option<Vec<u8>>)],
    ) -> anyhow::Result<(Vec<bool>, usize)> {
        let items = items
            .iter()
            .filter_map(|(key, val)| val.as_ref().map(|v| (key.clone(), v.as_slice())))
            .collect::<Vec<_>>();
        let res = CeramicOneEvent::insert_raw_carfiles(&self.pool, &items[..]).await?;
        Ok((res.keys, res.value_count))
    }

    async fn range_with_values(
        &self,
        start: &EventId,
        end: &EventId,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<(Cid, Vec<u8>)>> {
        CeramicOneEvent::range_with_values(&self.pool, start, end, offset, limit)
            .await?
            .into_iter()
            .map(|(event_id, value)| {
                Ok((
                    event_id
                        .cid()
                        .ok_or_else(|| anyhow!("EventId does not have an event CID"))?,
                    value,
                ))
            })
            .collect()
    }
    async fn value_for_order_key(&self, key: &EventId) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(CeramicOneEvent::value_by_order_key(&self.pool, key).await?)
    }

    async fn value_for_cid(&self, key: &Cid) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(CeramicOneEvent::value_by_cid(&self.pool, key).await?)
    }

    async fn events_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
    ) -> anyhow::Result<(i64, Vec<Cid>)> {
        Ok(CeramicOneEvent::new_events_since_value(&self.pool, highwater, limit).await?)
    }

    async fn get_block(&self, cid: &Cid) -> anyhow::Result<Option<Vec<u8>>> {
        let block = CeramicOneBlock::get(&self.pool, cid).await?;
        Ok(block.map(|b| b.data.to_vec()))
    }
}
