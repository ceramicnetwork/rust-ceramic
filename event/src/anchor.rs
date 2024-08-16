use cid::Cid;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use ceramic_core::EventId;

use crate::unvalidated::{Proof, RawTimeEvent};

/// AnchorRequest for a Data Event on a Stream
#[derive(Serialize, Deserialize, Clone)]
pub struct AnchorRequest {
    /// The CID of the stream Init Event
    pub init: Cid,
    /// The CID of the Event to be anchored
    pub prev: Cid,
    /// The Event ordering key
    pub order_key: EventId,
    /// The row ID of the Event in the database
    pub row_id: i64,
}

impl std::fmt::Debug for AnchorRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnchorRequest")
            .field("init", &format!("{:?}", self.init))
            .field("prev", &format!("{:?}", self.prev))
            .field("order_key", &format!("{:?}", self.order_key))
            .finish()
    }
}

/// Merkle tree node
pub type MerkleNode = Vec<Option<Cid>>;

/// A collection of Merkle tree nodes.
#[derive(Default)]
pub struct MerkleNodes {
    /// The Merkle tree nodes
    /// The keys are the CIDs of the Merkle nodes
    pub nodes: IndexMap<Cid, MerkleNode>,
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
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TimeEvents {
    /// The list of Time Events along with their original anchor requests
    pub events: Vec<(AnchorRequest, RawTimeEvent)>,
}

impl std::fmt::Debug for TimeEvents {
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
    pub time_events: TimeEvents,
}

impl std::fmt::Debug for TimeEventBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeEventBatch")
            .field("merkle_nodes", &self.merkle_nodes)
            .field("proof", &self.proof)
            .field("time_events", &self.time_events)
            .finish()
    }
}
