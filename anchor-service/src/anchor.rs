use anyhow::{anyhow, Context, Result};
use cid::Cid;
use indexmap::IndexMap;
use ipld_core::ipld::Ipld;
use serde::Serialize;
use std::fmt::Debug;
use tracing::info;

use ceramic_core::{EventId, SerializeExt};
use ceramic_event::unvalidated::{Proof, ProofEdge, RawTimeEvent, TimeEvent};

/// AnchorRequest for a Data Event on a Stream
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct AnchorRequest {
    /// The CID of the Init Event of the stream
    pub id: Cid,
    /// The CID of the Event to be anchored
    pub prev: Cid,
    /// ID for the Event being anchored
    pub event_id: EventId,
    /// the ResumeToken for the Event being anchored in the local database.
    pub resume_token: i64,
}

impl Debug for AnchorRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnchorRequest")
            .field("id", &format!("{:?}", &self.id))
            .field("prev", &format!("{:?}", &self.prev))
            .field("event_id", &format!("{:?}", &self.event_id))
            .field("resume_token", &self.resume_token)
            .finish()
    }
}

/// Merkle tree node
pub type MerkleNode = Vec<Option<Cid>>;

/// A collection of Merkle tree nodes.
#[derive(Default)]
pub struct MerkleNodes {
    /// This is a map from CIDs to Merkle Tree nodes that have those CIDs.
    /// We are using an IndexMap to keep the block in insert order.
    /// This keeps the remote and local block together for easier debugging.
    nodes: IndexMap<Cid, MerkleNode>,
}

impl MerkleNodes {
    /// Extend one map of MerkleNodes with another
    pub fn extend(&mut self, other: MerkleNodes) {
        self.nodes.extend(other.nodes);
    }

    /// Insert a new MerkleNode into the map
    pub fn insert(&mut self, key: Cid, value: MerkleNode) {
        self.nodes.insert(key, value);
    }

    /// Return an iterator over the MerkleNodes
    pub fn iter(&self) -> indexmap::map::Iter<Cid, MerkleNode> {
        self.nodes.iter()
    }

    /// get from the inner map cid -> MerkleNodes
    pub fn get(&self, key: &Cid) -> Option<&MerkleNode> {
        self.nodes.get(key)
    }
}

impl std::fmt::Debug for MerkleNodes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.nodes.iter().map(|(k, v)| {
                (
                    format!("{:?}", k),
                    v.iter().map(|x| format!("{:?}", x)).collect::<Vec<_>>(),
                )
            }))
            .finish()
    }
}

/// A list of Time Events
#[derive(Serialize)]
pub struct RawTimeEvents {
    /// The list of Time Events
    pub events: Vec<(AnchorRequest, RawTimeEvent)>,
}

impl std::fmt::Debug for RawTimeEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.events.iter()).finish()
    }
}

/// TimeEvents, MerkleNodes, and Proof emitted from anchoring
pub struct TimeEventBatch {
    /// The intermediate Merkle Tree Nodes
    pub merkle_nodes: MerkleNodes,
    /// The anchor proof
    pub proof: Proof,
    /// The Time Events
    pub raw_time_events: RawTimeEvents,
}

impl std::fmt::Debug for TimeEventBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeEventBatch")
            .field("merkle_nodes", &self.merkle_nodes)
            .field("proof", &self.proof)
            .field("raw_time_events", &self.raw_time_events)
            .finish()
    }
}

impl TimeEventBatch {
    /// Try to convert the TimeEventBatch into a list of TimeEventInsertables
    pub fn try_to_insertables(&self) -> Result<Vec<TimeEventInsertable>> {
        info!(
            "store anchor batch: proof={}, events={}",
            self.proof.to_cid()?,
            self.raw_time_events.events.len()
        );
        let events = self
            .raw_time_events
            .events
            .iter()
            .map(|(anchor_request, time_event)| {
                self.build_time_event_insertable(time_event, anchor_request)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(events)
    }

    /// Build a TimeEventInsertable from a RawTimeEvent and AnchorRequest
    fn build_time_event_insertable(
        &self,
        time_event: &RawTimeEvent,
        anchor_request: &AnchorRequest,
    ) -> Result<TimeEventInsertable> {
        let time_event_cid = time_event.to_cid().context(format!(
            "could not serialize time event for {} with batch proof {}",
            time_event.prev(),
            time_event.proof(),
        ))?;
        let blocks_in_path: Vec<ProofEdge> = self
            .find_tree_blocks_along_path(
                time_event.path(),
                &anchor_request.prev,
                &self.proof.root(),
                &self.merkle_nodes,
            )
            .context(format!(
                "could not build time event {} blocks for {} with batch proof {}",
                time_event_cid,
                (time_event.prev()),
                time_event.proof(),
            ))?
            // MerkleNodes to vec of IPLD nodes
            .into_iter()
            .map(|m: MerkleNode| -> ProofEdge {
                m.into_iter()
                    .map(|c: Option<Cid>| c.map_or(Ipld::Null, Ipld::Link))
                    .collect()
            })
            .collect();

        // RawTimeEvent to event
        Ok(TimeEventInsertable {
            event_id: anchor_request
                .event_id
                .swap_cid(&time_event_cid)
                .context(format!(
                    "could not swap {} into {}",
                    time_event_cid, anchor_request.event_id
                ))?,
            cid: time_event_cid,
            event: TimeEvent::new((*time_event).clone(), self.proof.clone(), blocks_in_path),
        })
    }

    fn find_tree_blocks_along_path(
        &self,
        path: &str,
        prev: &Cid,
        root: &Cid,
        merkle_nodes: &MerkleNodes,
    ) -> Result<Vec<MerkleNode>> {
        let mut blocks = Vec::new();
        let mut current_node_cid = *root;
        for part in path.split('/') {
            let merkle_node = merkle_nodes
                .get(&current_node_cid)
                .ok_or_else(|| anyhow!("missing merkle node for CID: {}", current_node_cid))?;
            blocks.push(merkle_node.clone());
            current_node_cid = match part {
                "0" => merkle_node[0].context("missing left node")?,
                "1" => merkle_node[1].context("missing right node")?,
                _ => return Err(anyhow!("invalid path part in time event path: {}", part)),
            }
        }
        if current_node_cid != *prev {
            return Err(anyhow!(
                "last node in path does not match prev CID: {} != {}",
                current_node_cid,
                prev
            ));
        }
        Ok(blocks)
    }
}

#[derive(Debug)]
/// The type we use to insert time events into the database
pub struct TimeEventInsertable {
    /// The event ID
    pub event_id: EventId,
    /// The time event CID
    pub cid: Cid,
    /// The parsed structure containing the Time Event
    pub event: TimeEvent,
}
