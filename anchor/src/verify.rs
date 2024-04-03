use anyhow::Result;
use ceramic_event::Cid;

#[derive(Clone, Debug)]
pub struct VerifyResult {
    pub chain_id: String,
    pub transaction_hash: String,
    pub block_number: u64,
    pub block_timestamp: time::OffsetDateTime,
}

#[async_trait::async_trait]
pub trait Verify {
    async fn verify(&self, cid: &Cid) -> Result<VerifyResult>;
}
