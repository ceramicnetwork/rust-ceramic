use crate::{CommitResult, CommitTransaction, Committer, DetachedTimeEvent, ProofBlock};
use alloy::network::Ethereum;
use alloy::primitives::{Address, FixedBytes};
use alloy::providers::RootProvider;
use alloy::sol_types::sol;
use alloy::transports::http::Http;
use ceramic_event::Cid;

sol! {
    #[sol(rpc)] // <-- Important! Generates the necessary `AnchorContract` struct and function methods.
    contract AnchorContract {
        #[derive(Debug)]
        function anchorDagCbor(bytes32 _root) public;
    }
}

pub struct EthTransaction {
    pub proof: Cid,
}

impl CommitTransaction for EthTransaction {
    fn root(&self) -> &Cid {
        &self.proof
    }
}

pub struct EthCommitter {
    provider: Http<Ethereum>,
    contract:
        AnchorContract::AnchorContractInstance<Ethereum, Http<reqwest::Client>, Http<Ethereum>>,
}

impl EthCommitter {
    pub fn new(url: url::Url, addr: Address) -> Self {
        let provider = RootProvider::<Ethereum, _>::new_http(url);
        let contract = AnchorContract::new(addr.into(), provider.clone());
        Self { provider, contract }
    }
}

#[async_trait::async_trait]
impl Committer for EthCommitter {
    type Transaction = EthTransaction;

    async fn commit(&self, transaction: Self::Transaction) -> anyhow::Result<CommitResult> {
        let bytes = FixedBytes::try_from(transaction.proof.to_bytes().as_slice())?;
        let call = self.contract.anchorDagCbor(bytes);
        let res = call.send().await?;
        let res = res.get_receipt().await?;
        let time_event = DetachedTimeEvent {
            path: transaction.proof.to_string(),
            proof: transaction.proof.clone(),
        };
        let proof_block = ProofBlock {
            chain_id: "self".to_string(),
            root: transaction.proof.clone(),
            tx_hash: transaction.proof.clone(),
            tx_type: "anchorDagCbor".to_string(),
        };
        return Ok(CommitResult {
            time_event,
            proof_block,
            nodes: vec![transaction.proof.clone()],
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::r#impl::tests::{mock_server, CID_STR};
    use alloy::primitives::{Bloom, Bytes, B256, U256};
    use alloy::rpc::types::eth as eth_types;
    use std::str::FromStr;

    sol! {
        #[sol(rpc)] // <-- Important! Generates the necessary `TestAnchorContract` struct and function methods.
        #[sol(bytecode = "0x1234")] // <-- Generates the `BYTECODE` static and the `deploy` method.
        contract TestAnchorContract {
            event DidAnchor(address indexed _service, bytes32 _root);

            constructor(){}

            #[derive(Debug)]
            function anchorDagCbor(bytes32 _root) public onlyAllowed {
                emit DidAnchor(msg.sender, _root);
            }
        }
    }

    #[derive(serde::Deserialize)]
    struct EthRequest {
        jsonrpc: String,
        id: u64,
        method: String,
        params: Option<serde_json::Value>,
    }

    #[derive(serde::Serialize)]
    struct EthResponse {
        jsonrpc: String,
        id: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<serde_json::Value>,
    }

    fn get_block() -> eth_types::Block {
        eth_types::Block {
            header: eth_types::Header {
                timestamp: 0,
                gas_limit: 0,
                base_fee_per_gas: None,
                blob_gas_used: None,
                excess_blob_gas: None,
                extra_data: Bytes::new(),
                gas_used: 0,
                hash: None,
                mix_hash: None,
                parent_beacon_block_root: None,
                parent_hash: B256::from(U256::from(0)),
                state_root: B256::from(U256::from(0)),
                total_difficulty: None,
                transactions_root: B256::from(U256::from(0)),
                uncles_hash: B256::from(U256::from(0)),
                withdrawals_root: None,
                miner: Address::default(),
                receipts_root: B256::from(U256::from(0)),
                logs_bloom: Bloom::default(),
                difficulty: U256::from(0),
                nonce: None,
                number: None,
            },
            transactions: eth_types::BlockTransactions::Hashes(vec![]),
            withdrawals: None,
            uncles: vec![],
            other: Default::default(),
            size: None,
        }
    }

    #[tokio::test]
    async fn can_submit_eth_transaction() {
        let (server, url) = mock_server(|mock| async {
            wiremock::Mock::given(wiremock::matchers::method("POST"))
                .and(wiremock::matchers::path("/"))
                .respond_with(|req: &wiremock::Request| {
                if let Some(h) = req.headers.get("content-type") {
                    if h != "application/json" {
                        panic!("Not a json-rpc request");
                    }
                } else {
                    panic!("No content type");
                }
                let req: EthRequest = serde_json::from_slice(&req.body).unwrap();
                let resp = match req.method {
                    m if m == "eth_sendTransaction" => {
                        let params: Vec<eth_types::Transaction> = serde_json::from_value(req.params.unwrap()).unwrap();
                        serde_json::Value::String(
                            "0x71C7656EC7ab88b098defB751B7401B5f6d8976Fab88b098defB751B7401B5f6".to_string()
                        )
                    }
                    m if m == "eth_blockNumber" => {
                        serde_json::Value::String(
                            "0x69B5B".to_string()
                        )
                    }
                    m if m == "eth_getBlockByNumber" => {
                        let block = get_block();
                        serde_json::to_value(block).unwrap()
                    }
                    _ => panic!("Unexpected method: {}", req.method),
                };
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(EthResponse {
                        jsonrpc: "2.0".to_string(),
                        id: 1,
                        result: Some(resp),
                        error: None,
                    })
            }).mount(&mock).await;
        }).await;

        let provider = RootProvider::<_, Ethereum>::new_http(url.clone());

        let contract = TestAnchorContract::deploy(&provider).await.unwrap();

        let committer = EthCommitter::new(url, contract.address().into());

        let proof = Cid::from_str(CID_STR).unwrap();

        let transaction = EthTransaction { proof };

        let result = committer.commit(transaction).await.unwrap();
    }
}
