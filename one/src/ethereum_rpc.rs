use anyhow::{anyhow, Context, Result};
use cid::{multihash, Cid};
use multihash::Multihash;
use tracing::debug;

pub(crate) struct RootTime {
    pub root: Cid,
    pub block_hash: String,
    pub timestamp: i64,
}

pub trait EthRpc {
    async fn root_time_by_transaction_cid(&self, cid: Cid) -> Result<RootTime>;
}

pub(crate) struct HttpEthRpc {
    url: String,
    client: reqwest::Client,
}

impl HttpEthRpc {
    pub fn new(url: String) -> Self {
        Self {
            url,
            client: reqwest::Client::new(),
        }
    }

    async fn eth_block_number(&self) -> Result<serde_json::Value> {
        // Get the latest block number.
        // curl https://mainnet.infura.io/v3/{api_token} \
        // -X POST \
        // -H "Content-Type: application/json" \
        // -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params": [],"id":1}'
        // >> {"jsonrpc": "2.0", "id": 1, "result": "0x127cc18"}
        Ok(self
            .client
            .post(&self.url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": [],
                "id": 1,
            }))
            .send()
            .await?
            .json()
            .await?)
    }

    async fn eth_block_by_hash(&self, block_hash: &str) -> Result<serde_json::Value> {
        // curl https://mainnet.infura.io/v3/{api_token} \
        //     -X POST \
        //     -H "Content-Type: application/json" \
        //     -d '{"jsonrpc":"2.0","method":"eth_getBlockByHash","params": ["0x{block_hash}",false],"id":1}'
        // >> {"jsonrpc": "2.0", "id": 1, "result": {"number": "0x105f34f", "timestamp": "0x644fe98b"}}
        Ok(self
            .client
            .post(&self.url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_getBlockByHash",
                "params": [block_hash, false],
                "id": 1,
            }))
            .send()
            .await?
            .json()
            .await?)
    }

    async fn eth_transaction_by_hash(&self, transaction_hash: &str) -> Result<serde_json::Value> {
        // Get the block_hash and input from the transaction.
        // curl https://mainnet.infura.io/v3/{api_token} \
        //   -X POST \
        //   -H "Content-Type: application/json" \
        //   -d '{"jsonrpc":"2.0","method":"eth_getTransactionByHash",
        //        "params":["0xBF7BC715A09DEA3177866AC4FC294AC9800EE2B49E09C55F56078579BFBBF158"],"id":1}'
        // >> {"jsonrpc":"2.0", "id":1, "result": {
        //       "blockHash": "0x783cd5a6febe13d08ac0d59fa7e666483d5e476542b29688a6f0bec3d15febd4",
        //       "blockNumber": "0x105f34f",
        //       "input": "0x97ad09eb41b6408c1b4be5016f652396ef47c0982c36d5877ebb874919bae3a9b854d8e1"
        //    }}
        Ok(self
            .client
            .post(&self.url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_getTransactionByHash",
                "params": [transaction_hash],
                "id": 1,
            }))
            .send()
            .await?
            .json()
            .await?)
    }
}

impl EthRpc for HttpEthRpc {
    async fn root_time_by_transaction_cid(&self, cid: Cid) -> Result<RootTime> {
        // transaction to blockHash, blockNumber, input
        let tx_hash = format!("0x{}", hex::encode(cid.hash().digest()));
        let json: serde_json::Value = self.eth_transaction_by_hash(&tx_hash).await?;
        debug!("txByHash response: {}", json);
        let result = json.get("result").context("missing field result")?;
        let block_hash = result
            .get("blockHash")
            .context("missing field result.blockHash")?;
        let block_hash = block_hash
            .as_str()
            .context("missing field result.blockHash was not a string")?;
        let block_number = result
            .get("blockNumber")
            .context("missing field result.blockNumber")?;
        let block_number = block_number
            .as_str()
            .context("missing field result.blockNumber was not a string")?;
        let block_number = get_timestamp_from_hex_string(block_number)
            .context("missing field result.blockNumber was not a hex timestamp")?;
        let input = result.get("input").context("missing field result.input")?;
        let input = input
            .as_str()
            .context("missing field result.input was not a string")?;
        let root = get_root_from_input(input)?;

        // Get the block with block_hash if latest block number minus result.number grater then 3 the block is stable.
        let Some(Ok(latest_block_number)) = self.eth_block_number().await?["result"]
            .as_str()
            .map(get_timestamp_from_hex_string)
        else {
            return Err(anyhow!("latest_block_number not found"));
        };
        if latest_block_number - block_number < 3 {
            return Err(anyhow!("latest_block_number - block_number < 3"));
        }

        // Get the block time.
        let json: serde_json::Value = self.eth_block_by_hash(block_hash).await?;
        debug!("blockByHash response: {}", json);

        let Some(timestamp) = json["result"]["timestamp"].as_str() else {
            return Err(anyhow!(
                "missing field result.timestamp or was not a string"
            ));
        };
        let timestamp = get_timestamp_from_hex_string(timestamp)?;
        Ok(RootTime {
            root,
            block_hash: block_hash.to_string(),
            timestamp,
        })
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
