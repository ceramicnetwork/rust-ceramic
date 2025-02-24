use std::ops::Range;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use ceramic_core::{EventId, NodeId};
use ceramic_event::unvalidated::Event;
use cid::Cid;
use iroh_bitswap::Block;
use recon::{HashCount, ReconItem, Result as ReconResult, Sha256a};
use tracing::info;

use crate::event::{DeliverableRequirement, EventService};
use crate::store::{BlockAccess, EventInsertable};
use crate::Error;

use super::service::{InsertResult, ValidationError, ValidationRequirement};

impl From<InsertResult> for recon::InsertResult<EventId> {
    fn from(value: InsertResult) -> Self {
        let mut invalid = Vec::new();
        for ev in value.rejected {
            match ev {
                ValidationError::InvalidFormat { key, reason } => {
                    info!(key=%key, %reason, "invalid format for recon event");
                    invalid.push(recon::InvalidItem::InvalidFormat { key })
                }
                ValidationError::InvalidSignature { key, reason } => {
                    info!(key=%key, %reason, "invalid signature for recon event");
                    invalid.push(recon::InvalidItem::InvalidSignature { key })
                }
                ValidationError::RequiresHistory { key } => {
                    unreachable!("recon items should never require history: {:?}", key)
                }
                ValidationError::InvalidTimeProof { key, reason } => {
                    info!(key=%key, %reason, "invalid time proof for recon event");
                    invalid.push(recon::InvalidItem::InvalidSignature { key })
                }
            };
        }
        recon::InsertResult::new_err(value.new.len(), invalid, value.pending_count)
    }
}

#[async_trait::async_trait]
impl recon::Store for EventService {
    type Key = EventId;
    type Hash = Sha256a;

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

    async fn hash_range(&self, range: Range<&Self::Key>) -> ReconResult<HashCount<Self::Hash>> {
        let res = self
            .event_access
            .hash_range(range)
            .await
            .map_err(Error::from)?;
        Ok(res)
    }

    async fn range(
        &self,
        range: Range<&Self::Key>,
    ) -> ReconResult<Box<dyn Iterator<Item = Self::Key> + Send + 'static>> {
        Ok(Box::new(
            self.event_access
                .range(range)
                .await
                .map_err(Error::from)?
                .into_iter(),
        ))
    }
    async fn first(&self, range: Range<&Self::Key>) -> ReconResult<Option<Self::Key>> {
        Ok(self.event_access.first(range).await.map_err(Error::from)?)
    }
    async fn middle(&self, range: Range<&Self::Key>) -> ReconResult<Option<Self::Key>> {
        Ok(self.event_access.middle(range).await.map_err(Error::from)?)
    }

    async fn count(&self, range: Range<&Self::Key>) -> ReconResult<usize> {
        Ok(self.event_access.count(range).await.map_err(Error::from)?)
    }
    async fn value_for_key(&self, key: &Self::Key) -> ReconResult<Option<Vec<u8>>> {
        Ok(self
            .event_access
            .value_by_order_key(key)
            .await
            .map_err(Error::from)?)
    }
}

#[async_trait::async_trait]
impl iroh_bitswap::Store for EventService {
    async fn get_size(&self, cid: &Cid) -> anyhow::Result<usize> {
        Ok(BlockAccess::get_size(&self.pool, cid).await?)
    }
    async fn get(&self, cid: &Cid) -> anyhow::Result<Block> {
        let maybe = BlockAccess::get(&self.pool, cid).await?;
        maybe.ok_or_else(|| anyhow!("block {} does not exist", cid))
    }
    async fn has(&self, cid: &Cid) -> anyhow::Result<bool> {
        Ok(BlockAccess::has(&self.pool, cid).await?)
    }
    async fn put(&self, block: &Block) -> anyhow::Result<bool> {
        Ok(BlockAccess::put(&self.pool, block).await?)
    }
}

impl From<InsertResult> for Vec<ceramic_api::EventInsertResult> {
    fn from(res: InsertResult) -> Self {
        let mut api_res = Vec::with_capacity(res.new.len() + res.rejected.len());
        for ev in res.new.into_iter().chain(res.existed.into_iter()) {
            api_res.push(ceramic_api::EventInsertResult::new_ok(ev));
        }

        for ev in res.rejected {
            let (key, reason) = match ev {
                ValidationError::InvalidFormat { key, reason } => {
                    (key, format!("Event data could not be parsed: {reason}"))
                }
                ValidationError::InvalidSignature { key, reason } => {
                    (key, format!("Event had invalid signature: {reason}"))
                }
                ValidationError::RequiresHistory { key } => (
                    key,
                    "Failed to insert event as `prev` event was missing".to_owned(),
                ),
                ValidationError::InvalidTimeProof { key, reason } => (
                    key,
                    format!("Failed to validate time event inclusion proof: {reason}"),
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
        offset: u32,
        limit: u32,
    ) -> anyhow::Result<Vec<(Cid, Vec<u8>)>> {
        self.event_access
            .range_with_values(&range.start..&range.end, offset, limit)
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
        Ok(self.event_access.value_by_order_key(key).await?)
    }

    async fn value_for_cid(&self, key: &Cid) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.event_access.value_by_cid(key).await?)
    }

    async fn events_since_highwater_mark(
        &self,
        highwater: i64,
        limit: i64,
        include_data: ceramic_api::IncludeEventData,
    ) -> anyhow::Result<(i64, Vec<ceramic_api::EventDataResult>)> {
        let res = match include_data {
            ceramic_api::IncludeEventData::None => {
                let (hw, cids) = self
                    .event_access
                    .new_events_since_value(highwater, limit)
                    .await?;
                let res = cids
                    .into_iter()
                    .map(|cid| ceramic_api::EventDataResult::new(cid, None))
                    .collect();
                (hw, res)
            }
            ceramic_api::IncludeEventData::Full => {
                let (hw, data) = self
                    .event_access
                    .new_events_since_value_with_data(highwater, limit)
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
        Ok(self.event_access.get_highwater_mark().await?)
    }

    async fn get_block(&self, cid: &Cid) -> anyhow::Result<Option<Vec<u8>>> {
        let block = BlockAccess::get(&self.pool, cid).await?;
        Ok(block.map(|b| b.data.to_vec()))
    }
}

#[async_trait]
impl ceramic_anchor_service::Store for EventService {
    async fn insert_many(
        &self,
        items: Vec<ceramic_anchor_service::TimeEventInsertable>,
        informant: NodeId,
    ) -> Result<()> {
        let items = items
            .into_iter()
            .map(|insertable| {
                EventInsertable::try_new(
                    insertable.event_id,
                    insertable.cid,
                    true,
                    Arc::new(Event::Time(Box::new(insertable.event))),
                    Some(informant),
                )
                .map_err(|e| anyhow!("could not create EventInsertable: {}", e))
            })
            .collect::<Result<Vec<EventInsertable>>>()?;
        self.event_access
            .insert_many(items.iter())
            .await
            .context("anchoring insert_many failed")?;
        Ok(())
    }

    async fn events_since_high_water_mark(
        &self,
        informant: NodeId,
        high_water_mark: i64,
        limit: i64,
    ) -> Result<Vec<ceramic_anchor_service::AnchorRequest>> {
        // Fetch event CIDs from the events table using the previous high water mark
        Ok(self
            .event_access
            .data_events_by_informant(informant, high_water_mark, limit)
            .await
            .map_err(|e| Error::new_app(anyhow!("could not fetch events by informant: {}", e)))?)
    }
}
