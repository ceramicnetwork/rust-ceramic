use std::ops::Range;

use anyhow::anyhow;
use ceramic_core::EventId;
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
        let res = self
            .insert_events_from_carfiles_remote_history(&[item.to_owned()])
            .await?;

        Ok(res.keys.first().copied().unwrap_or(false))
    }

    /// Insert new keys into the key space.
    /// Returns true for each key if it did not previously exist, in the
    /// same order as the input iterator.
    async fn insert_many(&self, items: &[ReconItem<'_, Self::Key>]) -> ReconResult<InsertResult> {
        let res = self
            .insert_events_from_carfiles_remote_history(items)
            .await?;
        Ok(res)
    }

    /// Return the hash of all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    /// Returns ReconResult<(Hash, count), Err>
    async fn hash_range(&self, range: Range<&Self::Key>) -> ReconResult<HashCount<Self::Hash>> {
        let res = CeramicOneEvent::hash_range(&self.pool, range).await?;
        Ok(res)
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
        Ok(Box::new(
            CeramicOneEvent::range(&self.pool, range, offset, limit)
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
        range: Range<&Self::Key>,
        offset: usize,
        limit: usize,
    ) -> ReconResult<Box<dyn Iterator<Item = (Self::Key, Vec<u8>)> + Send + 'static>> {
        Ok(Box::new(
            CeramicOneEvent::range_with_values(&self.pool, range, offset, limit)
                .await?
                .into_iter(),
        ))
    }
    /// Return the number of keys within the range.
    async fn count(&self, range: Range<&Self::Key>) -> ReconResult<usize> {
        Ok(CeramicOneEvent::count(&self.pool, range).await?)
    }

    /// value_for_key returns
    /// Ok(Some(value)) if stored,
    /// Ok(None) if not stored, and
    /// Err(e) if retrieving failed.
    async fn value_for_key(&self, key: &Self::Key) -> ReconResult<Option<Vec<u8>>> {
        Ok(CeramicOneEvent::value_by_order_key(&self.pool, key).await?)
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
impl ceramic_api::EventStore for CeramicEventService {
    async fn insert_many(&self, items: &[(EventId, Vec<u8>)]) -> anyhow::Result<Vec<bool>> {
        let items = items
            .iter()
            .map(|(key, val)| ReconItem::new(key, val.as_slice()))
            .collect::<Vec<_>>();
        let res = self
            .insert_events_from_carfiles_local_history(&items[..])
            .await?;
        Ok(res.keys)
    }

    async fn range_with_values(
        &self,
        range: Range<EventId>,
        offset: usize,
        limit: usize,
    ) -> anyhow::Result<Vec<(Cid, Vec<u8>)>> {
        CeramicOneEvent::range_with_values(&self.pool, &range.start..&range.end, offset, limit)
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

    async fn highwater_mark(&self) -> anyhow::Result<i64> {
        Ok(CeramicOneEvent::get_highwater_mark(&self.pool).await?)
    }

    async fn get_block(&self, cid: &Cid) -> anyhow::Result<Option<Vec<u8>>> {
        let block = CeramicOneBlock::get(&self.pool, cid).await?;
        Ok(block.map(|b| b.data.to_vec()))
    }
}
