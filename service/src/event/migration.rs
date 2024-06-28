use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, bail, Context as _, Result};
use ceramic_core::{EventId, Network};
use ceramic_event::unvalidated::{self, signed::cacao::Capability};
use cid::Cid;
use futures::{Stream, StreamExt, TryStreamExt};
use ipld_core::ipld::Ipld;
use thiserror::Error;
use tracing::{debug, error, info, instrument, Level};

use crate::CeramicEventService;

use super::service::{Block, BoxedBlock};

pub struct Migrator<'a> {
    service: &'a CeramicEventService,
    network: Network,
    blocks: BTreeMap<Cid, Box<dyn Block>>,
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

impl<'a> Migrator<'a> {
    pub async fn new(
        service: &'a CeramicEventService,
        network: Network,
        blocks: impl Stream<Item = Result<BoxedBlock>>,
    ) -> Result<Self> {
        let blocks = blocks
            .map(|block| block.map(|block| (block.cid(), block)))
            .try_collect::<BTreeMap<Cid, BoxedBlock>>()
            .await?;
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

    fn find_block(&self, cid: &Cid) -> Result<&BoxedBlock, AnalyzeError> {
        if let Some(block) = self.blocks.get(cid) {
            Ok(block)
        } else {
            Err(AnalyzeError::MissingBlock(*cid))
        }
    }

    #[instrument(skip(self), ret(level = Level::DEBUG))]
    pub async fn migrate(mut self) -> Result<()> {
        let cids: Vec<Cid> = self.blocks.keys().cloned().collect();
        for cid in cids {
            let ret = self.process_block(cid).await;
            if let Err(err) = ret {
                self.error_count += 1;
                error!(%cid, %err, "error processing block");
            }
            if self.batch.len() > 1000 {
                self.write_batch().await?
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
    #[instrument(skip(self), ret(level = Level::DEBUG))]
    async fn process_block(&mut self, cid: Cid) -> Result<()> {
        let block = self.find_block(&cid)?;
        let data: Vec<u8> = block.data().await?;
        let event: Result<unvalidated::RawEvent<Ipld>, _> = serde_ipld_dagcbor::from_slice(&data);
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
            let block = self.find_block(&cid)?;
            let data = block.data().await?;
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
            .insert_events_from_carfiles_recon(&items)
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
        let block = self.find_block(&link)?;
        let payload_data = block.data().await?;
        let payload: unvalidated::Payload<Ipld> =
            serde_ipld_dagcbor::from_slice(&payload_data).context("decoding payload")?;
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
            let block = self.find_block(&capability_cid)?;
            let data = block.data().await?;
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
    async fn find_init_payload(&self, cid: &Cid) -> Result<unvalidated::init::Payload<Ipld>> {
        let init_block = self.find_block(cid)?;
        let init_data = init_block.data().await?;
        let init: unvalidated::RawEvent<Ipld> =
            serde_ipld_dagcbor::from_slice(&init_data).context("decoding init envelope")?;
        match init {
            unvalidated::RawEvent::Time(_) => bail!("init event must not be a time event"),
            unvalidated::RawEvent::Signed(init) => {
                let init_payload_block = self.find_block(
                    &init
                        .link()
                        .ok_or_else(|| anyhow!("init envelope must have a link"))?,
                )?;
                let init_payload_data = init_payload_block.data().await?;

                serde_ipld_dagcbor::from_slice(&init_payload_data).context("decoding init payload")
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
        let block = self.find_block(&proof_id)?;
        let data = block.data().await?;
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
            let block = self.find_block(&curr)?;
            let data = block.data().await.context("fetch block data")?;
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
