use anyhow::{anyhow, Result};
use cid::{multihash, Cid};
use multihash::Multihash;
use tracing::debug;

pub trait EthRpc {
    async fn eth_transaction_by_hash(&self, cid: Cid) -> Result<(Cid, i64)>;
}

pub(crate) struct HttpEthRpc {
    url: String,
}

impl HttpEthRpc {
    pub fn new(url: String) -> Self {
        Self { url }
    }
}

impl EthRpc for HttpEthRpc {
    async fn eth_transaction_by_hash(&self, cid: Cid) -> Result<(Cid, i64)> {
        // curl https://mainnet.infura.io/v3/{api_token} \
        //   -X POST \
        //   -H "Content-Type: application/json" \
        //   -d '{"jsonrpc":"2.0","method":"eth_getTransactionByHash", "params":["0x{tx_hash}"],"id":1}'
        let tx_hash = format!("0x{}", hex::encode(cid.hash().digest()));
        let client = reqwest::Client::new();
        let res = client
            .post(&self.url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_getTransactionByHash",
                "params": [tx_hash],
                "id": 1,
            }))
            .send()
            .await?;
        let json: serde_json::Value = res.json().await?;
        debug!("txByHash response: {}", json);
        if let Some(result) = json.get("result") {
            if let Some(block_hash) = result.get("blockHash") {
                if let Some(block_hash) = block_hash.as_str() {
                    if let Some(input) = result.get("input") {
                        if let Some(input) = input.as_str() {
                            return Ok((
                                get_root_from_input(input)?,
                                eth_block_by_hash(block_hash, &self.url).await?,
                            ));
                        }
                    }
                }
            }
        }
        Err(anyhow!("missing fields"))
    }
}

async fn eth_block_by_hash(block_hash: &str, ethereum_rpc_url: &str) -> Result<i64> {
    // curl https://mainnet.infura.io/v3/{api_token} \
    //     -X POST \
    //     -H "Content-Type: application/json" \
    //     -d '{"jsonrpc":"2.0","method":"eth_getBlockByHash","params": ["0x{block_hash}",false],"id":1}'
    let client = reqwest::Client::new();
    let res = client
        .post(ethereum_rpc_url)
        .json(&serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByHash",
            "params": [block_hash, false],
            "id": 1,
        }))
        .send()
        .await?;
    let json: serde_json::Value = res.json().await?;
    debug!("blockByHash response: {}", json);
    if let Some(timestamp) = json["result"]["timestamp"].as_str() {
        get_timestamp_from_hex_string(timestamp)
    } else {
        Err(anyhow!("missing fields"))
    }
}

fn get_root_from_input(input: &str) -> Result<Cid> {
    if let Some(input) = input.strip_prefix("0x97ad09eb") {
        // Strip "0x97ad09eb" from the input and convert it into a cidv1 - dag-cbor - (sha2-256 : 256)
        // 0x12 -> sha2-256
        // 0x20 -> 256 bits of hash
        let root_bytes = [vec![0x12_u8, 0x20], hex::decode(input)?.to_vec()].concat();
        Ok(Cid::new_v1(0x71, Multihash::from_bytes(&root_bytes)?))
    } else {
        Err(anyhow!("input is not anchor-cbor"))
    }
}

fn get_timestamp_from_hex_string(ts: &str) -> Result<i64> {
    // Strip "0x" from the timestamp and convert it to a u64
    if let Some(ts) = ts.strip_prefix("0x") {
        Ok(i64::from_str_radix(ts, 16)?)
    } else {
        Err(anyhow!("timestamp is not valid hex"))
    }
}
