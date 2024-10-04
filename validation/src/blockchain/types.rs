use ceramic_core::Cid;
use ssi::caip2;

#[derive(Debug)]
/// The error variants expected from an Ethereum RPC request
pub enum Error {
    /// Invalid input with reason for rejection
    InvalidArgument(String),
    /// Transaction hash not found
    TxNotFound {
        /// The chain ID
        chain_id: caip2::ChainId,
        /// The transaction hash we tried to find
        tx_hash: String,
    },
    /// The transaction exists but has not been mined yet
    TxNotMined {
        /// The chain ID
        chain_id: caip2::ChainId,
        /// The transaction hash we found but didn't find a corresponding block
        tx_hash: String,
    },
    /// The block was included on the transaction but could not be found
    BlockNotFound {
        /// The chain ID
        chain_id: caip2::ChainId,
        /// The block hash we tried to find
        block_hash: String,
    },
    /// The proof was invalid for the given reason
    InvalidProof(String),
    /// No chain provider configured for the event, whether that's an error is up to the caller
    NoChainProvider(caip2::ChainId),
    /// This is a transient error related to the transport and may be retried
    Transient(anyhow::Error),
    /// This is a standard application error (e.g. server 500) and should not be retried.
    Application(anyhow::Error),
}


#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// A proof of time on the blockchain
pub struct TimeProof {
    /// The timestamp the proof was recorded
    pub timestamp: u64,
    /// The root CID of the proof
    pub root_cid: Cid,
}

#[async_trait::async_trait]
/// Ethereum RPC provider methods. This is a higher level type than the actual RPC calls neeed and
/// may wrap a multiple calls into a logical behavior of getting necessary information.
pub trait ChainInclusion {
    /// The input format needed to do the inclusion proof
    type InclusionInput;

    /// Get the CAIP2 chain ID supported by this RPC provider
    fn chain_id(&self) -> &caip2::ChainId;

    /// Get the block chain transaction if it exists with the block timestamp information
    async fn chain_inclusion_proof(&self, input: &Self::InclusionInput)
        -> Result<TimeProof, Error>;
}