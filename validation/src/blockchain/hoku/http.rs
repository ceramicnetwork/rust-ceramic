use ceramic_core::Cid;

use hoku_provider::{json_rpc::JsonRpcProvider, HttpClient};
use hoku_sdk::{
    machine::{accumulator::Accumulator, Machine as _},
    query::FvmQueryHeight,
};
use ssi::caip2;

use crate::blockchain::{hoku::HokuTxInput, ChainInclusion, Error, TimeProof};

/// A hoku RPC client using HTTP
pub struct HokuHttpClient {
    chain_id: caip2::ChainId,
    provider: JsonRpcProvider<HttpClient>,
}

impl HokuHttpClient {
    /// Create a new Hoku HTTP JSON RPC client for a given URL
    pub fn try_new() -> anyhow::Result<Self> {
        todo!()
    }
}

impl std::fmt::Debug for HokuHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HokuHttpClient")
            .field("chain_id", &self.chain_id)
            .field("provider", &"JsonRpcProvider<HttpClient>")
            .finish()
    }
}

#[async_trait::async_trait]
impl ChainInclusion for HokuHttpClient {
    type InclusionInput = HokuTxInput;

    fn chain_id(&self) -> &caip2::ChainId {
        &self.chain_id
    }

    async fn chain_inclusion_proof(&self, input: &HokuTxInput) -> Result<TimeProof, Error> {
        let machine = Accumulator::attach(input.address).await.map_err(|e| {
            Error::Application(anyhow::anyhow!("Failed to attach to accumulator: {}", e))
        })?;

        // Fetch the leaf at the given index
        let (timestamp, leaf) = machine
            .leaf(&self.provider, input.index, FvmQueryHeight::Committed)
            .await
            .map_err(|e| {
                Error::InvalidProof(format!(
                    "Accumulator with address {} had no entry at {} for time event: {e}",
                    input.address, input.index
                ))
            })?;
        let root_cid = Cid::try_from(leaf.as_slice())
            .map_err(|e| Error::InvalidProof(format!("Accumulator leaf is not a CID: {}", e)))?;

        return Ok(TimeProof {
            timestamp,
            root_cid,
        });
    }
}
