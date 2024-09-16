use std::{
    num::NonZero,
    str::FromStr,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Context, Result};
use lru::LruCache;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use ssi::caip2;
use tracing::trace;

const TRANSACTION_CACHE_SIZE: usize = 50;
const BLOCK_CACHE_SIZE: usize = 50;

static HTTP_CLIENT: Lazy<reqwest::Client> = Lazy::new(reqwest::Client::new);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
/// A blockchain transaction
pub struct ChainTransaction {
    /// Transaction hash
    pub hash: String,
    /// Transaction contract input
    pub input: String,
    /// Information about the block in which this transaction was mined.
    /// If None, the transaction exists but has not been mined yet.
    pub block: Option<ChainBlock>,
}

impl ChainTransaction {
    fn new_from_tx(tx: EthTransaction) -> Self {
        ChainTransaction {
            hash: tx.hash,
            input: tx.input,
            block: None,
        }
    }

    fn try_new_with_block(tx: EthTransaction, block: EthBlock) -> Result<Self> {
        Ok(ChainTransaction {
            hash: tx.hash,
            input: tx.input,
            block: Some(block.try_into()?),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
/// A blockchain block
pub struct ChainBlock {
    pub hash: String,
    /// the block number
    pub number: i64,
    /// the unix epoch timestamp
    pub timestamp: i64,
}

#[async_trait::async_trait]
pub trait EthRpc {
    fn chain_id(&self) -> &caip2::ChainId;

    async fn get_block_timestamp(&self, tx_hash: &str) -> Result<Option<ChainTransaction>>;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcResponse<T> {
    jsonrpc: String,
    id: i32,
    result: Option<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
/// The expected payload for eth_getTransactionByHash. It's a contract transaction
/// and expects fields (e.g. input) to exist that aren't required for all transactions.
struct EthTransaction {
    /// Block hash if mined
    block_hash: Option<String>,
    /// Transaction hash
    hash: String,
    /// Contract input data
    input: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
/// The block information returned by eth_getBlockByHash
#[serde(rename_all = "camelCase")]
struct EthBlock {
    hash: String,
    /// 0x prefixed hexadecimal block number
    number: String,
    /// 0x prefixed hexademical representing unix epoch time
    timestamp: String,
}

impl TryFrom<EthBlock> for ChainBlock {
    type Error = anyhow::Error;

    fn try_from(value: EthBlock) -> std::result::Result<Self, Self::Error> {
        Ok(ChainBlock {
            hash: value.hash,
            number: i64_from_hex(&value.number).context("invalid block number")?,
            timestamp: i64_from_hex(&value.timestamp).context("invalid block timestamp")?,
        })
    }
}

#[derive(Debug)]
/// Http client to interact with EIP chains
pub struct HttpEthRpc {
    chain_id: caip2::ChainId,
    url: reqwest::Url,
    tx_cache: Arc<Mutex<LruCache<String, EthTransaction>>>,
    block_cache: Arc<Mutex<LruCache<String, EthBlock>>>,
}

impl HttpEthRpc {
    /// Create a new ethereum VM compatible HTTP client
    pub async fn try_new(url: &str) -> Result<Self> {
        let url = reqwest::Url::parse(url).context("invalid url")?;
        let chain_id = Self::eth_chain_id(url.clone()).await?;
        let chain_decimal = i64_from_hex(&chain_id)?;
        let chain_id = caip2::ChainId::from_str(&format!("eip155:{chain_decimal}"))?;
        let tx_cache = Arc::new(Mutex::new(LruCache::new(
            NonZero::new(TRANSACTION_CACHE_SIZE).expect("transaction cache size must be non zero"),
        )));
        let block_cache = Arc::new(Mutex::new(LruCache::new(
            NonZero::new(BLOCK_CACHE_SIZE).expect("block cache size must be non zero"),
        )));
        Ok(Self {
            url,
            chain_id,
            tx_cache,
            block_cache,
        })
    }

    /// Retrieve the caip2 Chain ID
    ///
    /// ❯ curl https://cloudflare-eth.com/v1/mainnet -X POST --data '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":0}'
    ///    {"jsonrpc":"2.0","result":"0x1","id":0}
    ///
    /// ❯ curl https://rpc.ankr.com/filecoin -X POST --data '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":0}'
    /// >>  {"id":0,"jsonrpc":"2.0","result":"0x13a"}
    async fn eth_chain_id(url: reqwest::Url) -> Result<String> {
        let resp: RpcResponse<String> = HTTP_CLIENT
            .post(url.clone())
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_chainId",
                "params": [],
                "id": 0,
            }))
            .send()
            .await?
            .json()
            .await?;
        resp.result
            .ok_or_else(|| anyhow!("failed to retrieve chain ID"))
    }

    /// Get a block by its hash
    ///
    /// curl https://mainnet.infura.io/v3/{api_token} \
    ///     -X POST \
    ///     -H "Content-Type: application/json" \
    ///     -d '{"jsonrpc":"2.0","method":"eth_getBlockByHash","params": ["0x{block_hash}",false],"id":1}'
    /// >> {"jsonrpc": "2.0", "id": 1, "result": {"number": "0x105f34f", "timestamp": "0x644fe98b"}}
    async fn eth_block_by_hash(&self, block_hash: &str) -> Result<Option<EthBlock>> {
        if let Some(blk) = self.block_cache.lock().unwrap().get(block_hash) {
            return Ok(Some(blk.to_owned()));
        }
        let resp: RpcResponse<EthBlock> = HTTP_CLIENT
            .post(self.url.clone())
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_getBlockByHash",
                "params": [block_hash, false],
                "id": 1,
            }))
            .send()
            .await?
            .json()
            .await?;
        let block = match resp.result {
            Some(blk) => blk,
            None => return Ok(None),
        };

        let mut cache = self.block_cache.lock().unwrap();
        cache.put(block_hash.to_string(), block.clone());
        Ok(Some(block))
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
        transaction_hash: &str,
    ) -> Result<Option<EthTransaction>> {
        if let Some(tx) = self.tx_cache.lock().unwrap().get(transaction_hash) {
            return Ok(Some(tx.to_owned()));
        }
        let resp: RpcResponse<EthTransaction> = HTTP_CLIENT
            .post(self.url.clone())
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_getTransactionByHash",
                "params": [transaction_hash],
                "id": 1,
            }))
            .send()
            .await?
            .json()
            .await?;
        let tx = match resp.result {
            Some(tx) => tx,
            None => return Ok(None),
        };
        let mut cache = self.tx_cache.lock().unwrap();
        cache.put(transaction_hash.to_string(), tx.clone());
        Ok(Some(tx))
    }
}

#[async_trait::async_trait]
impl EthRpc for HttpEthRpc {
    fn chain_id(&self) -> &caip2::ChainId {
        &self.chain_id
    }

    async fn get_block_timestamp(&self, tx_hash: &str) -> Result<Option<ChainTransaction>> {
        // transaction to blockHash, blockNumber, input
        let tx_hash_res = match self.eth_transaction_by_hash(tx_hash).await? {
            Some(tx) => tx,
            None => return Ok(None),
        };
        trace!(?tx_hash_res, "txByHash response");

        let blk_hash = if let Some(hash) = &tx_hash_res.block_hash {
            hash
        } else {
            return Ok(Some(ChainTransaction::new_from_tx(tx_hash_res)));
        };

        // for now we ignore how old the block is i.e. we don't care if it's more than 3
        // it's left up to the implementer in
        // https://chainagnostic.org/CAIPs/caip-168 and https://namespaces.chainagnostic.org/eip155/caip168
        // this means nodes may have a slightly different answer to the exact time an event happened

        let blk_hash_res = self.eth_block_by_hash(blk_hash).await?;
        trace!(?blk_hash_res, "blockByHash response");

        let block = match blk_hash_res {
            Some(blk) => blk,
            None => return Ok(Some(ChainTransaction::new_from_tx(tx_hash_res))),
        };

        Ok(Some(ChainTransaction::try_new_with_block(
            tx_hash_res,
            block,
        )?))
    }
}

/// Get an i64 integer from a 0x prefixed hex string
fn i64_from_hex(val: &str) -> Result<i64> {
    val.strip_prefix("0x")
        .map(|v| i64::from_str_radix(v, 16))
        .transpose()?
        .ok_or_else(|| anyhow!("string is not valid hex: {}", val))
}
