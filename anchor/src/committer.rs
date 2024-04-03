use anyhow::Result;
use ceramic_event::Cid;

use serde::{Deserialize, Serialize};

pub trait CommitTransaction {
    fn root(&self) -> &Cid;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DetachedTimeEvent {
    pub path: String,
    pub proof: Cid,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProofBlock {
    pub chain_id: String,
    pub root: Cid,
    pub tx_hash: Cid,
    pub tx_type: String,
}

#[derive(Clone, Debug)]
pub struct CommitResult {
    pub time_event: DetachedTimeEvent,
    pub proof_block: ProofBlock,
    pub nodes: Vec<Cid>,
}

#[async_trait::async_trait]
pub trait Committer {
    type Transaction: CommitTransaction;
    async fn commit(&self, transaction: Self::Transaction) -> Result<CommitResult>;
}
