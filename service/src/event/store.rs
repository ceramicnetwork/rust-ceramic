use std::ops::Range;

use anyhow::anyhow;
use ceramic_core::EventId;
use ceramic_store::{CeramicOneBlock, CeramicOneEvent};
use cid::Cid;
use iroh_bitswap::Block;
use recon::{HashCount, Key, ReconItem, Result as ReconResult, Sha256a};

use crate::event::{CeramicEventService, DeliverableRequirement};

#[async_trait::async_trait]
impl recon::Store for CeramicEventService {
    type Key = EventId;
    type Hash = Sha256a;

    async fn insert<'a>(&self, item: &ReconItem<'a, Self::Key>) -> ReconResult<bool> {
        let res = self
            .insert_events(&[item.to_owned()], DeliverableRequirement::Asap)
            .await?;

        Ok(res
            .store_result
            .inserted
            .first()
            .map_or(false, |i| i.new_key))
    }

    /// Insert new keys into the key space.
    /// Returns true for each key if it did not previously exist, in the
    /// same order as the input iterator.
    async fn insert_many<'a>(
        &self,
        items: &[ReconItem<'a, Self::Key>],
    ) -> ReconResult<recon::InsertResult> {
        let res = self
            .insert_events(items, DeliverableRequirement::Asap)
            .await?;
        let mut keys = vec![false; items.len()];
        // we need to put things back in the right order that the recon trait expects, even though we don't really care about the result
        for (i, item) in items.iter().enumerate() {
            let new_key = res
                .store_result
                .inserted
                .iter()
                .find(|e| e.order_key == *item.key)
                .map_or(false, |e| e.new_key); // TODO: should we error if it's not in this set
            keys[i] = new_key;
        }
        Ok(recon::InsertResult::new(keys))
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
    async fn insert_many(
        &self,
        items: &[(EventId, Vec<u8>)],
    ) -> anyhow::Result<Vec<ceramic_api::EventInsertResult>> {
        let items = items
            .iter()
            .map(|(key, val)| ReconItem::new(key, val.as_slice()))
            .collect::<Vec<_>>();
        let res = self
            .insert_events(&items[..], DeliverableRequirement::Immediate)
            .await?;

        Ok(res.into())
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
        include_data: ceramic_api::IncludeEventData,
    ) -> anyhow::Result<(i64, Vec<ceramic_api::EventDataResult>)> {
        let res = match include_data {
            ceramic_api::IncludeEventData::None => {
                let (hw, cids) =
                    CeramicOneEvent::new_events_since_value(&self.pool, highwater, limit).await?;
                let res = cids
                    .into_iter()
                    .map(|cid| ceramic_api::EventDataResult::new(cid, None))
                    .collect();
                (hw, res)
            }
            ceramic_api::IncludeEventData::Full => {
                let (hw, data) =
                    CeramicOneEvent::new_events_since_value_with_data(&self.pool, highwater, limit)
                        .await?;
                let mut res = Vec::with_capacity(data.len());
                for (cid, value) in data {
                    res.push(ceramic_api::EventDataResult::new(
                        cid,
                        Some(value.encode_car().await?),
                    ));
                }
                (hw, res)
            }
        };

        Ok(res)
    }

    async fn highwater_mark(&self) -> anyhow::Result<i64> {
        Ok(CeramicOneEvent::get_highwater_mark(&self.pool).await?)
    }

    async fn get_block(&self, cid: &Cid) -> anyhow::Result<Option<Vec<u8>>> {
        let block = CeramicOneBlock::get(&self.pool, cid).await?;
        Ok(block.map(|b| b.data.to_vec()))
    }
}

#[async_trait::async_trait]
impl ceramic_rpc::EventStore for CeramicEventService {
    async fn events_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
    ) -> anyhow::Result<(i64, Vec<(EventId, Vec<u8>)>)> {
        Ok((
            0,
            CeramicOneEvent::range_with_values(
                &self.pool,
                &EventId::min_value()..&EventId::max_value(),
                0,
                limit as usize,
            )
            .await?,
        ))
    }
}
