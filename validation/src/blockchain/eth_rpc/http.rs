use std::{
    num::NonZero,
    str::FromStr,
    sync::{Arc, Mutex},
};

use alloy::{
    primitives::{BlockHash, TxHash},
    providers::{Provider, ProviderBuilder, RootProvider},
    rpc::types::{Block, BlockTransactionsKind, Transaction},
    transports::http::{Client, Http},
};
use lru::LruCache;
use ssi::caip2;
use tracing::trace;

use crate::eth_rpc::{ChainBlock, ChainTransaction, Error, EthRpc};

const TRANSACTION_CACHE_SIZE: usize = 50;
const BLOCK_CACHE_SIZE: usize = 50;

type Result<T> = std::result::Result<T, Error>;

impl From<&Block> for ChainBlock {
    fn from(value: &Block) -> Self {
        ChainBlock {
            hash: value.header.hash,
            number: value.header.number,
            timestamp: value.header.timestamp,
        }
    }
}

#[derive(Debug)]
/// Http client to interact with EIP chains
pub struct HttpEthRpc {
    chain_id: caip2::ChainId,
    url: reqwest::Url,
    tx_cache: Arc<Mutex<LruCache<TxHash, Transaction>>>,
    block_cache: Arc<Mutex<LruCache<BlockHash, ChainBlock>>>,
    provider: RootProvider<Http<Client>>,
}

impl HttpEthRpc {
    /// Create a new ethereum VM compatible HTTP client
    pub async fn try_new(url: &str) -> Result<Self> {
        let url = reqwest::Url::parse(url)
            .map_err(|e| Error::InvalidArgument(format!("invalid url: {}", e)))?;
        let provider = ProviderBuilder::new().on_http(url.clone());
        let chain_decimal = provider
            .get_chain_id()
            .await
            .map_err(|e| Error::InvalidArgument(format!("failed to retrieve chain ID: {}", e)))?;

        // assume we only support eip155 chain IDs for now
        let chain_id = caip2::ChainId::from_str(&format!("eip155:{chain_decimal}"))
            .map_err(|e| Error::InvalidArgument(format!("invalid chain ID: {}", e)))?;
        let tx_cache = Arc::new(Mutex::new(LruCache::new(
            NonZero::new(TRANSACTION_CACHE_SIZE).expect("transaction cache size must be non zero"),
        )));
        let block_cache = Arc::new(Mutex::new(LruCache::new(
            NonZero::new(BLOCK_CACHE_SIZE).expect("block cache size must be non zero"),
        )));
        Ok(Self {
            chain_id,
            url,
            tx_cache,
            block_cache,
            provider,
        })
    }

    /// Get a block by its hash. For now, we return and cache a [`ChainBlock`] as it's much smaller than
    /// the actual Block returned from the RPC endpoint and we don't need most of the information.
    ///
    /// curl https://mainnet.infura.io/v3/{api_token} \
    ///     -X POST \
    ///     -H "Content-Type: application/json" \
    ///     -d '{"jsonrpc":"2.0","method":"eth_getBlockByHash","params": ["0x{block_hash}",false],"id":1}'
    /// >> {"jsonrpc": "2.0", "id": 1, "result": {"number": "0x105f34f", "timestamp": "0x644fe98b"}}
    async fn eth_block_by_hash(&self, block_hash: BlockHash) -> Result<Option<ChainBlock>> {
        if let Some(blk) = self.block_cache.lock().unwrap().get(&block_hash) {
            return Ok(Some(blk.to_owned()));
        }
        let block = self
            .provider
            .get_block_by_hash(block_hash, BlockTransactionsKind::Hashes)
            .await?;
        let block = block.as_ref().map(ChainBlock::from);
        if let Some(blk) = &block {
            let mut cache = self.block_cache.lock().unwrap();
            cache.put(block_hash, blk.clone());
        }

        Ok(block)
    }

    /// Get the block_hash and input from the transaction
    ///
    /// curl https://mainnet.infura.io/v3/{api_token} \
    ///   -X POST \
    ///   -H "Content-Type: application/json" \
    ///   -d '{"jsonrpc":"2.0","method":"eth_getTransactionByHash",
    ///        "params":["0xBF7BC715A09DEA3177866AC4FC294AC9800EE2B49E09C55F56078579BFBBF158"],"id":1}'
    /// >> {"jsonrpc":"2.0", "id":1, "result": {
    /// >>    "blockHash": "0x783cd5a6febe13d08ac0d59fa7e666483d5e476542b29688a6f0bec3d15febd4",
    /// >>  "blockNumber": "0x105f34f",
    /// >>  "input": "0x97ad09eb41b6408c1b4be5016f652396ef47c0982c36d5877ebb874919bae3a9b854d8e1"
    /// >>  }}
    async fn eth_transaction_by_hash(
        &self,
        transaction_hash: TxHash,
    ) -> Result<Option<Transaction>> {
        if let Some(tx) = self.tx_cache.lock().unwrap().get(&transaction_hash) {
            return Ok(Some(tx.to_owned()));
        }
        let tx = self
            .provider
            .get_transaction_by_hash(transaction_hash)
            .await?;

        if let Some(tx) = &tx {
            let mut cache = self.tx_cache.lock().unwrap();
            cache.put(transaction_hash, tx.clone());
        }
        Ok(tx)
    }
}

#[async_trait::async_trait]
impl EthRpc for HttpEthRpc {
    fn chain_id(&self) -> &caip2::ChainId {
        &self.chain_id
    }

    fn url(&self) -> String {
        self.url.to_string()
    }

    async fn get_block_timestamp(&self, tx_hash: &str) -> Result<Option<ChainTransaction>> {
        // transaction to blockHash, blockNumber, input
        let tx_hash = TxHash::from_str(tx_hash)
            .map_err(|e| Error::InvalidArgument(format!("invalid transaction hash: {}", e)))?;
        let tx_hash_res = match self.eth_transaction_by_hash(tx_hash).await? {
            Some(tx) => tx,
            None => return Ok(None),
        };
        trace!(?tx_hash_res, "txByHash response");

        if let Some(block_hash) = &tx_hash_res.block_hash {
            // for now we ignore how old the block is i.e. we don't care if it's more than 3
            // it's left up to the implementer in
            // https://chainagnostic.org/CAIPs/caip-168 and https://namespaces.chainagnostic.org/eip155/caip168
            // this means nodes may have a slightly different answer to the exact time an event happened

            let blk_hash_res = self.eth_block_by_hash(*block_hash).await?;
            trace!(?blk_hash_res, "blockByHash response");
            let block = blk_hash_res.map(ChainBlock::from);
            Ok(Some(ChainTransaction {
                hash: tx_hash_res.hash,
                input: tx_hash_res.input.to_string(),
                block,
            }))
        } else {
            Ok(Some(ChainTransaction {
                hash: tx_hash_res.hash,
                input: tx_hash_res.input.to_string(),
                block: None,
            }))
        }
    }
}
