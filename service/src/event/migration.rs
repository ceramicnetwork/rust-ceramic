use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{anyhow, bail, Context as _, Result};
use arrow::array::{ArrayRef, BinaryArray, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, SchemaBuilder};
use ceramic_core::{EventId, Network};
use ceramic_event::unvalidated::{self, signed::cacao::Capability};
use ceramic_store::CeramicOneEvent;
use cid::Cid;
use futures::TryStreamExt;
use ipld_core::ipld::Ipld;
use parquet::{arrow::AsyncArrowWriter, basic::Compression, file::properties::WriterProperties};
use recon::{AssociativeHash, Key, Sha256a};
use serde::Deserialize;
use thiserror::Error;
use tracing::{debug, error, info, instrument, Level};

use crate::{
    event::{BlockStore, DeliverableRequirement},
    CeramicEventService,
};

pub struct FromIpfsMigrator<'a, S> {
    service: &'a CeramicEventService,
    network: Network,
    blocks: S,
    batch: Vec<(EventId, Vec<u8>)>,

    // All unsigned init payloads we have found.
    unsigned_init_payloads: BTreeSet<Cid>,
    // All unsigned init payloads we have found that are referenced from a signed init event
    // envelope.
    // We use two sets because we do not know the order in which we find blocks.
    // Simply removing the referenced blocks from the unsigned_init_payloads set as we find them is
    // not sufficient.
    referenced_unsigned_init_payloads: BTreeSet<Cid>,

    error_count: usize,
    event_count: usize,
}

impl<'a, S: BlockStore> FromIpfsMigrator<'a, S> {
    pub async fn new(
        service: &'a CeramicEventService,
        network: Network,
        blocks: S,
    ) -> Result<Self> {
        Ok(Self {
            network,
            service,
            blocks,
            batch: Default::default(),
            unsigned_init_payloads: Default::default(),
            referenced_unsigned_init_payloads: Default::default(),
            error_count: 0,
            event_count: 0,
        })
    }

    async fn load_block(&self, cid: &Cid) -> Result<Vec<u8>> {
        if let Some(block) = self.blocks.block_data(cid).await? {
            Ok(block)
        } else {
            Err(AnalyzeError::MissingBlock(*cid).into())
        }
    }

    #[instrument(skip(self), ret(level = Level::DEBUG))]
    pub async fn migrate(mut self) -> Result<()> {
        const PROGRESS_COUNT: usize = 1_000;

        let mut all_blocks = self.blocks.blocks();
        let mut count = 0;
        while let Some((cid, data)) = all_blocks.try_next().await? {
            let ret = self.process_block(cid, &data).await;
            if let Err(err) = ret {
                self.error_count += 1;
                error!(%cid, err=format!("{err:#}"), "error processing block");
            }
            if self.batch.len() > 1000 {
                self.write_batch().await?
            }
            count += 1;
            if count % PROGRESS_COUNT == 0 {
                info!(last_block=%cid, count, error_count = self.error_count, "migrated blocks");
            }
        }
        self.write_batch().await?;

        self.process_unreferenced_init_payloads().await?;

        info!(
            event_count = self.event_count,
            error_count = self.error_count,
            "migration finished"
        );
        Ok(())
    }
    // Decodes the block and if it is a Ceramic event, it and related blocks are constructed into an
    // event.
    #[instrument(skip(self, data), ret(level = Level::DEBUG))]
    async fn process_block(&mut self, cid: Cid, data: &[u8]) -> Result<()> {
        let event: Result<unvalidated::RawEvent<Ipld>, _> = serde_ipld_dagcbor::from_slice(data);
        match event {
            Ok(unvalidated::RawEvent::Unsigned(_)) => {
                self.unsigned_init_payloads.insert(cid);
            }
            Ok(unvalidated::RawEvent::Signed(event)) => {
                self.process_signed_event(cid, event).await?
            }
            Ok(unvalidated::RawEvent::Time(event)) => self.process_time_event(cid, *event).await?,
            // Ignore blocks that are not Ceramic events
            Err(_) => {}
        }

        Ok(())
    }
    // For any unsigned init payload blocks there are two possibilities:
    //     1. It is an unsigned init event
    //     2. It is the payload of another signed init event.
    // Therefore once we have processed all other events then the remaining init payloads must be
    // the blocks that are unsigned init events.
    #[instrument(skip(self), ret(level = Level::DEBUG))]
    async fn process_unreferenced_init_payloads(&mut self) -> Result<()> {
        let init_events: Vec<_> = self
            .unsigned_init_payloads
            .difference(&self.referenced_unsigned_init_payloads)
            .cloned()
            .collect();
        for cid in init_events {
            let data = self
                .load_block(&cid)
                .await
                .context("finding init event block")?;
            let payload: unvalidated::init::Payload<Ipld> = serde_ipld_dagcbor::from_slice(&data)?;
            let event_builder = EventBuilder::new(
                cid,
                payload.header().model().to_vec(),
                payload.header().controllers()[0].clone(),
                cid,
            );
            let event = unvalidated::Event::from(payload);
            self.batch
                .push(event_builder.build(&self.network, event).await?);
            if self.batch.len() > 1000 {
                self.write_batch().await?
            }
        }
        self.write_batch().await?;
        Ok(())
    }
    async fn write_batch(&mut self) -> Result<()> {
        let items = self
            .batch
            .iter()
            .map(|(id, body)| recon::ReconItem::new(id, body))
            .collect::<Vec<_>>();
        self.service
            .insert_events(&items, DeliverableRequirement::Lazy)
            .await?;
        self.event_count += self.batch.len();
        self.batch.truncate(0);
        Ok(())
    }

    // Find and add all blocks related to this signed event
    #[instrument(skip(self, event), ret(level = Level::DEBUG))]
    async fn process_signed_event(
        &mut self,
        cid: Cid,
        event: unvalidated::signed::Envelope,
    ) -> Result<()> {
        let link = event
            .link()
            .ok_or_else(|| anyhow!("event envelope must have a link"))?;
        let payload_data = self
            .load_block(&link)
            .await
            .context("finding payload link block")?;
        let payload: unvalidated::Payload<Ipld> = serde_ipld_dagcbor::from_slice(&payload_data)
            .context("decoding payload")
            .map_err(|err| {
                if self.is_tile_doc(&payload_data) {
                    anyhow!("found Tile Document, skipping")
                } else {
                    anyhow!("{err}")
                }
            })?;
        let event_builder = match &payload {
            unvalidated::Payload::Init(payload) => {
                self.referenced_unsigned_init_payloads.insert(link);
                EventBuilder::new(
                    cid,
                    payload.header().model().to_vec(),
                    payload.header().controllers()[0].clone(),
                    cid,
                )
            }
            unvalidated::Payload::Data(payload) => {
                let init_payload = self.find_init_payload(payload.id()).await?;
                EventBuilder::new(
                    cid,
                    init_payload.header().model().to_vec(),
                    init_payload.header().controllers()[0].clone(),
                    *payload.id(),
                )
            }
        };
        let mut capability = None;
        if let Some(capability_cid) = event.capability() {
            debug!(%capability_cid, "found cap chain");
            let data = self
                .load_block(&capability_cid)
                .await
                .context("finding capability block")?;
            // Parse capability to ensure it is valid
            let cap: Capability = serde_ipld_dagcbor::from_slice(&data)?;
            debug!(%capability_cid, ?cap, "capability");
            capability = Some((capability_cid, cap));
        }
        let s = unvalidated::signed::Event::new(cid, event, link, payload, capability);
        let event = unvalidated::Event::from(s);
        self.batch
            .push(event_builder.build(&self.network, event).await?);
        Ok(())
    }
    fn is_tile_doc(&self, data: &[u8]) -> bool {
        // Attempt to decode the payload as a loose TileDocument.
        // If we succeed produce a meaningful error message.
        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        struct TileDocPayload {
            data: Ipld,
        }
        serde_ipld_dagcbor::from_slice::<TileDocPayload>(data).is_ok()
    }
    async fn find_init_payload(&self, cid: &Cid) -> Result<unvalidated::init::Payload<Ipld>> {
        let init_data = self
            .load_block(cid)
            .await
            .context("finding init payload block")?;
        let init: unvalidated::RawEvent<Ipld> =
            serde_ipld_dagcbor::from_slice(&init_data).context("decoding init envelope")?;
        match init {
            unvalidated::RawEvent::Time(_) => bail!("init event must not be a time event"),
            unvalidated::RawEvent::Signed(init) => {
                let init_payload_data = self
                    .load_block(
                        &init
                            .link()
                            .ok_or_else(|| anyhow!("init envelope must have a link"))?,
                    )
                    .await
                    .context("finding init link block")?;

                serde_ipld_dagcbor::from_slice(&init_payload_data)
                    .context("decoding init payload")
                    .map_err(|err| {
                        if self.is_tile_doc(&init_payload_data) {
                            anyhow!("found Tile Document, skipping")
                        } else {
                            anyhow!("{err}")
                        }
                    })
            }
            unvalidated::RawEvent::Unsigned(payload) => Ok(payload),
        }
    }
    // Find and add all blocks related to this time event
    #[instrument(skip(self, event), ret(level = Level::DEBUG))]
    async fn process_time_event(
        &mut self,
        cid: Cid,
        event: unvalidated::RawTimeEvent,
    ) -> Result<()> {
        let init = event.id();
        let init_payload = self.find_init_payload(&event.id()).await?;
        let event_builder = EventBuilder::new(
            cid,
            init_payload.header().model().to_vec(),
            init_payload.header().controllers()[0].clone(),
            init,
        );
        let proof_id = event.proof();
        let data = self
            .load_block(&proof_id)
            .await
            .context("finding proof block")?;
        let proof: unvalidated::Proof = serde_ipld_dagcbor::from_slice(&data)?;
        let mut curr = proof.root();
        let mut proof_edges = Vec::new();
        for index in event.path().split('/') {
            if curr == event.prev() {
                // The time event's previous link is the same as the last link in the proof.
                // That block should already be included independently no need to include it here.
                break;
            }
            let idx: usize = index.parse().context("parsing path segment as index")?;
            let data = self
                .load_block(&curr)
                .await
                .context("finding witness block")?;
            let edge: unvalidated::ProofEdge =
                serde_ipld_dagcbor::from_slice(&data).context("dag cbor decode")?;

            // Follow path
            let maybe_link = edge.get(idx).cloned();
            // Add edge data to event
            proof_edges.push(edge);
            if let Some(Ipld::Link(link)) = maybe_link {
                curr = link;
            } else {
                error!(%curr, "missing block");
                break;
            }
        }
        let time = unvalidated::TimeEvent::new(event, proof, proof_edges);
        let event: unvalidated::Event<Ipld> = unvalidated::Event::from(Box::new(time));
        self.batch
            .push(event_builder.build(&self.network, event).await?);
        Ok(())
    }
}

#[derive(Error, Debug)]
enum AnalyzeError {
    #[error("missing linked block from event {0}")]
    MissingBlock(Cid),
}

struct EventBuilder {
    event_cid: Cid,
    sep: Vec<u8>,
    controller: String,
    init: Cid,
}

impl EventBuilder {
    fn new(event_cid: Cid, sep: Vec<u8>, controller: String, init: Cid) -> Self {
        Self {
            event_cid,
            sep,
            controller,
            init,
        }
    }

    async fn build(
        self,
        network: &Network,
        event: unvalidated::Event<Ipld>,
    ) -> Result<(EventId, Vec<u8>)> {
        let event_id = EventId::builder()
            .with_network(network)
            .with_sep("model", &self.sep)
            .with_controller(&self.controller)
            .with_init(&self.init)
            .with_event(&self.event_cid)
            .build();

        let body = event.encode_car().await?;
        Ok((event_id, body))
    }
}

pub struct FromSqliteMigrator<'a> {
    service: &'a CeramicEventService,
}

impl<'a> FromSqliteMigrator<'a> {
    pub fn new(service: &'a CeramicEventService) -> Self {
        Self { service }
    }

    pub async fn migrate(
        &self,
        output_parquet_path: impl AsRef<Path>,
        max: Option<usize>,
    ) -> Result<()> {
        tokio::fs::create_dir_all(&output_parquet_path).await?;
        let out_file = tokio::fs::File::create(
            PathBuf::new()
                .join(output_parquet_path)
                .join("events.parquet"),
        )
        .await?;

        // WriterProperties can be used to set Parquet file options
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut builder = SchemaBuilder::new();
        builder.push(Field::new("order_key", DataType::Binary, false));
        builder.push(Field::new("ahash_0", DataType::UInt32, false));
        builder.push(Field::new("ahash_1", DataType::UInt32, false));
        builder.push(Field::new("ahash_2", DataType::UInt32, false));
        builder.push(Field::new("ahash_3", DataType::UInt32, false));
        builder.push(Field::new("ahash_4", DataType::UInt32, false));
        builder.push(Field::new("ahash_5", DataType::UInt32, false));
        builder.push(Field::new("ahash_6", DataType::UInt32, false));
        builder.push(Field::new("ahash_7", DataType::UInt32, false));
        builder.push(Field::new("stream_id", DataType::Binary, false));
        builder.push(Field::new("cid", DataType::Binary, false));
        builder.push(Field::new("car", DataType::Binary, false));

        let mut writer =
            AsyncArrowWriter::try_new(out_file, Arc::new(builder.finish()), Some(props)).unwrap();

        let mut offset = 0;
        let limit = 100_000;
        loop {
            let events = CeramicOneEvent::range_with_values(
                &self.service.pool,
                &EventId::min_value()..&EventId::max_value(),
                offset,
                limit,
            )
            .await?;
            let order_keys = BinaryArray::from_iter_values(
                events.iter().map(|(event_id, _car)| event_id.as_slice()),
            );
            let hashes: Vec<_> = events
                .iter()
                .map(|(event_id, _car)| Sha256a::digest(event_id))
                .collect();

            let ahash_0 =
                UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[0]));
            let ahash_1 =
                UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[1]));
            let ahash_2 =
                UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[2]));
            let ahash_3 =
                UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[3]));
            let ahash_4 =
                UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[4]));
            let ahash_5 =
                UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[5]));
            let ahash_6 =
                UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[6]));
            let ahash_7 =
                UInt32Array::from_iter_values(hashes.iter().map(|hash| hash.as_u32s()[7]));
            let stream_ids = BinaryArray::from_iter_values(
                events
                    .iter()
                    .map(|(event_id, _car)| event_id.stream_id().unwrap()),
            );
            let cids = BinaryArray::from_iter_values(
                events
                    .iter()
                    .map(|(event_id, _car)| event_id.cid().unwrap().to_bytes()),
            );
            let cars =
                BinaryArray::from_iter_values(events.iter().map(|(_event_id, car)| car.as_slice()));

            let batch = RecordBatch::try_from_iter(vec![
                ("order_key", Arc::new(order_keys) as ArrayRef),
                ("ahash_0", Arc::new(ahash_0) as ArrayRef),
                ("ahash_1", Arc::new(ahash_1) as ArrayRef),
                ("ahash_2", Arc::new(ahash_2) as ArrayRef),
                ("ahash_3", Arc::new(ahash_3) as ArrayRef),
                ("ahash_4", Arc::new(ahash_4) as ArrayRef),
                ("ahash_5", Arc::new(ahash_5) as ArrayRef),
                ("ahash_6", Arc::new(ahash_6) as ArrayRef),
                ("ahash_7", Arc::new(ahash_7) as ArrayRef),
                ("stream_id", Arc::new(stream_ids) as ArrayRef),
                ("cid", Arc::new(cids) as ArrayRef),
                ("car", Arc::new(cars) as ArrayRef),
            ])?;
            writer.write(&batch).await?;
            if events.len() < limit {
                break;
            }
            offset += limit;
            info!(count = offset, "written events");
            if let Some(max) = max {
                if offset >= max {
                    break;
                }
            }
        }
        writer.close().await?;
        Ok(())
    }
}
