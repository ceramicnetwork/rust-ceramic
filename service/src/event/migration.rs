use std::collections::BTreeSet;

use anyhow::{anyhow, Context as _};
use ceramic_core::{EventId, Network};
use ceramic_event::unvalidated::{self, signed::cacao::Capability};
use cid::Cid;
use futures::TryStreamExt;
use ipld_core::ipld::Ipld;
use recon::ReconItem;
use serde::Deserialize;
use thiserror::Error;
use tracing::{debug, error, info, instrument, Level};

use crate::{
    event::{BlockStore, DeliverableRequirement},
    CeramicEventService,
};

pub struct Migrator<'a, S> {
    service: &'a CeramicEventService,
    network: Network,
    blocks: S,
    batch: Vec<ReconItem<EventId>>,
    ignore_tile_docs: bool,

    // All unsigned init payloads we have found.
    unsigned_init_payloads: BTreeSet<Cid>,
    // All unsigned init payloads we have found that are referenced from a signed init event
    // envelope.
    // We use two sets because we do not know the order in which we find blocks.
    // Simply removing the referenced blocks from the unsigned_init_payloads set as we find them is
    // not sufficient.
    referenced_unsigned_init_payloads: BTreeSet<Cid>,

    error_count: usize,
    tile_doc_count: usize,
    event_count: usize,
}

impl<'a, S: BlockStore> Migrator<'a, S> {
    pub async fn new(
        service: &'a CeramicEventService,
        network: Network,
        blocks: S,
        ignore_tile_docs: bool,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            network,
            service,
            blocks,
            ignore_tile_docs,
            batch: Default::default(),
            unsigned_init_payloads: Default::default(),
            referenced_unsigned_init_payloads: Default::default(),
            error_count: 0,
            tile_doc_count: 0,
            event_count: 0,
        })
    }

    async fn load_block(&self, cid: &Cid) -> Result<Vec<u8>> {
        if let Some(block) = self.blocks.block_data(cid).await? {
            Ok(block)
        } else {
            Err(Error::MissingBlock(*cid).into())
        }
    }

    #[instrument(skip(self), ret(level = Level::DEBUG))]
    pub async fn migrate(mut self) -> anyhow::Result<()> {
        const PROGRESS_COUNT: usize = 1_000;

        let mut all_blocks = self.blocks.blocks();
        let mut count = 0;
        while let Some((cid, data)) = all_blocks.try_next().await? {
            let ret = self.process_block(cid, &data).await;
            if let Err(err) = ret {
                let log = match err {
                    Error::FoundInitTileDoc(_) | Error::FoundDataTileDoc(_) => {
                        self.tile_doc_count += 1;
                        !self.ignore_tile_docs
                    }
                    Error::MissingBlock(_) | Error::Fatal(_) => {
                        self.error_count += 1;
                        true
                    }
                };
                if log {
                    error!(%cid, err=format!("{err:#}"), "error processing block");
                }
            }
            if self.batch.len() > 1000 {
                self.write_batch().await?
            }
            count += 1;
            if count % PROGRESS_COUNT == 0 {
                info!(
                    last_block=%cid,
                    block_count = count,
                    event_count = self.event_count,
                    error_count = self.error_count,
                    tile_doc_count = self.tile_doc_count,
                    "migrated blocks"
                );
            }
        }
        self.write_batch().await?;

        self.process_unreferenced_init_payloads().await?;

        info!(
            event_count = self.event_count,
            error_count = self.error_count,
            tile_doc_count = self.tile_doc_count,
            "migration finished"
        );
        Ok(())
    }
    // Decodes the block and if it is a Ceramic event, it and related blocks are constructed into an
    // event.
    #[instrument(skip(self, data), ret(level = Level::DEBUG))]
    async fn process_block(&mut self, cid: Cid, data: &[u8]) -> Result<()> {
        let event: Result<unvalidated::RawEvent<Ipld>> =
            serde_ipld_dagcbor::from_slice(data).map_err(Error::new_fatal);
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
            let payload: unvalidated::init::Payload<Ipld> =
                serde_ipld_dagcbor::from_slice(&data).map_err(Error::new_fatal)?;
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
        self.service
            .insert_events(&self.batch, DeliverableRequirement::Lazy)
            .await
            .map_err(Error::new_fatal)?;
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
                if self.is_tile_doc_data(&payload_data) {
                    Error::FoundDataTileDoc(cid)
                } else if self.is_tile_doc_init(&payload_data) {
                    Error::FoundInitTileDoc(cid)
                } else {
                    Error::new_fatal(err)
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
            let cap: Capability =
                serde_ipld_dagcbor::from_slice(&data).map_err(Error::new_fatal)?;
            debug!(%capability_cid, ?cap, "capability");
            capability = Some((capability_cid, cap));
        }
        let s = unvalidated::signed::Event::new(cid, event, link, payload, capability);
        let event = unvalidated::Event::from(s);
        self.batch
            .push(event_builder.build(&self.network, event).await?);
        Ok(())
    }
    fn is_tile_doc_init(&self, data: &[u8]) -> bool {
        // Attempt to decode the payload as a loose TileDocument.
        // An init event will always have a `header` field.
        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        struct TileDocInitPayload {
            header: Ipld,
        }
        serde_ipld_dagcbor::from_slice::<TileDocInitPayload>(data).is_ok()
    }
    fn is_tile_doc_data(&self, data: &[u8]) -> bool {
        // Attempt to decode the payload as a loose TileDocument.
        // A data event will always have a `data` field.
        #[allow(dead_code)]
        #[derive(Debug, Deserialize)]
        struct TileDocDataPayload {
            data: Ipld,
        }
        serde_ipld_dagcbor::from_slice::<TileDocDataPayload>(data).is_ok()
    }
    #[instrument(skip(self), ret(level = Level::DEBUG))]
    async fn find_init_payload(&self, cid: &Cid) -> Result<unvalidated::init::Payload<Ipld>> {
        let init_data = self
            .load_block(cid)
            .await
            .context("finding init payload block")?;
        let init: unvalidated::RawEvent<Ipld> = serde_ipld_dagcbor::from_slice(&init_data)
            .context("decoding init envelope")
            .map_err(|err| {
                if self.is_tile_doc_init(&init_data) {
                    Error::FoundInitTileDoc(*cid)
                } else {
                    Error::new_fatal(err)
                }
            })?;
        match init {
            unvalidated::RawEvent::Time(_) => {
                return Err(Error::new_fatal(anyhow!(
                    "init event must not be a time event"
                )))
            }
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
                        if self.is_tile_doc_init(&init_payload_data) {
                            Error::FoundInitTileDoc(*cid)
                        } else {
                            Error::new_fatal(err)
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
        let proof: unvalidated::Proof =
            serde_ipld_dagcbor::from_slice(&data).map_err(Error::new_fatal)?;
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

type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
enum Error {
    #[error("missing linked block from event {0}")]
    MissingBlock(Cid),
    #[error("block is an init tile document: {0}")]
    FoundInitTileDoc(Cid),
    #[error("block is a data tile document: {0}")]
    FoundDataTileDoc(Cid),
    #[error("fatal error: {0:#}")]
    Fatal(#[from] anyhow::Error),
}
impl Error {
    fn new_fatal(err: impl Into<anyhow::Error>) -> Self {
        Self::from(err.into())
    }
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
    ) -> Result<ReconItem<EventId>> {
        let event_id = EventId::builder()
            .with_network(network)
            .with_sep("model", &self.sep)
            .with_controller(&self.controller)
            .with_init(&self.init)
            .with_event(&self.event_cid)
            .build();

        let body = event.encode_car()?;
        Ok(ReconItem::new(event_id, body))
    }
}
