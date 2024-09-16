use std::ops::Range;

use anyhow::anyhow;
use ceramic_core::{EventId, NodeId};
use cid::Cid;
use iroh_bitswap::Block;
use recon::{HashCount, ReconItem, Result as ReconResult, Sha256a};
use tracing::info;

use crate::event::{DeliverableRequirement, EventService};
use crate::store::{CeramicOneBlock, CeramicOneEvent};
use crate::Error;

use super::service::{InsertResult, InvalidItem, ValidationRequirement};

impl From<InsertResult> for recon::InsertResult<EventId> {
    fn from(value: InsertResult) -> Self {
        let mut pending = 0;
        let mut invalid = Vec::new();
        for ev in value.rejected {
            match ev {
                InvalidItem::InvalidFormat { key, reason } => {
                    info!(key=%key, %reason, "invalid format for recon event");
                    invalid.push(recon::InvalidItem::InvalidFormat { key })
                }
                InvalidItem::InvalidSignature { key, reason } => {
                    info!(key=%key, %reason, "invalid signature for recon event");
                    invalid.push(recon::InvalidItem::InvalidSignature { key })
                }
                // once we implement enough validation to actually return these items,
                // the service will need to track them and retry them when the required CIDs are discovered
                InvalidItem::RequiresHistory { .. } => pending += 1,
            };
        }
        recon::InsertResult::new_err(value.store_result.count_new_keys(), invalid, pending)
    }
}

#[async_trait::async_trait]
impl recon::Store for EventService {
    type Key = EventId;
    type Hash = Sha256a;

    /// Insert new keys into the key space.
    /// Returns true for each key if it did not previously exist, in the
    /// same order as the input iterator.
    async fn insert_many(
        &self,
        items: &[ReconItem<Self::Key>],
        informant: NodeId,
    ) -> ReconResult<recon::InsertResult<EventId>> {
        let res = self
            .insert_events(
                items,
                DeliverableRequirement::Asap,
                Some(informant),
                Some(ValidationRequirement::new_recon()),
            )
            .await?;

        Ok(res.into())
    }

    /// Return the hash of all keys in the range between left_fencepost and right_fencepost.
    /// Both range bounds are exclusive.
    /// Returns ReconResult<(Hash, count), Err>
    async fn hash_range(&self, range: Range<&Self::Key>) -> ReconResult<HashCount<Self::Hash>> {
        let res = CeramicOneEvent::hash_range(&self.pool, range)
            .await
            .map_err(Error::from)?;
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
                .await
                .map_err(Error::from)?
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
                .await
                .map_err(Error::from)?
                .into_iter(),
        ))
    }
    /// Return the number of keys within the range.
    async fn count(&self, range: Range<&Self::Key>) -> ReconResult<usize> {
        Ok(CeramicOneEvent::count(&self.pool, range)
            .await
            .map_err(Error::from)?)
    }

    /// value_for_key returns
    /// Ok(Some(value)) if stored,
    /// Ok(None) if not stored, and
    /// Err(e) if retrieving failed.
    async fn value_for_key(&self, key: &Self::Key) -> ReconResult<Option<Vec<u8>>> {
        Ok(CeramicOneEvent::value_by_order_key(&self.pool, key)
            .await
            .map_err(Error::from)?)
    }
}

#[async_trait::async_trait]
impl iroh_bitswap::Store for EventService {
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

impl From<InsertResult> for Vec<ceramic_api::EventInsertResult> {
    fn from(res: InsertResult) -> Self {
        let mut api_res = Vec::with_capacity(res.store_result.inserted.len() + res.rejected.len());
        for ev in res.store_result.inserted {
            api_res.push(ceramic_api::EventInsertResult::new_ok(ev.order_key));
        }

        for ev in res.rejected {
            let (key, reason) = match ev {
                InvalidItem::InvalidFormat { key, reason } => {
                    (key, format!("Event data could not be parsed: {reason}"))
                }
                InvalidItem::InvalidSignature { key, reason } => {
                    (key, format!("Event had invalid signature: {reason}"))
                }
                InvalidItem::RequiresHistory { key } => (
                    key,
                    "Failed to insert event as `prev` event was missing".to_owned(),
                ),
            };
            api_res.push(ceramic_api::EventInsertResult::new_failed(key, reason));
        }
        api_res
    }
}

#[async_trait::async_trait]
impl ceramic_api::EventService for EventService {
    async fn insert_many(
        &self,
        items: Vec<ceramic_api::ApiItem>,
        informant: NodeId,
    ) -> anyhow::Result<Vec<ceramic_api::EventInsertResult>> {
        let items = items
            .into_iter()
            .map(|i| ReconItem {
                key: i.key,
                value: i.value,
            })
            .collect::<Vec<_>>();
        let res = self
            .insert_events(
                &items,
                DeliverableRequirement::Immediate,
                Some(informant),
                Some(ValidationRequirement::new_local()),
            )
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
                for row in data {
                    res.push(ceramic_api::EventDataResult::new(
                        row.cid,
                        Some(row.event.encode_car()?),
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
