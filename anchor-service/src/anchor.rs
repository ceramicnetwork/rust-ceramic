use cid::Cid;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use ceramic_event::unvalidated::{Proof, RawTimeEvent};

/// AnchorRequest for a Data Event on a Stream
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AnchorRequest {
    /// The CID of the stream
    pub id: Cid,
    /// The CID of the Event to be anchored
    pub prev: Cid,
}

/// Merkle tree node
pub type MerkleNode = Vec<Option<Cid>>;

/// A collection of Merkle tree nodes.
#[derive(Default)]
pub struct MerkleNodes {
    /// This is a map from CIDs to Merkle Tree nodes that have those CIDs.
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
    /// The list of Time Events
    pub events: Vec<RawTimeEvent>,
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
