use std::{collections::HashMap, str::FromStr, sync::Arc};

use anyhow::{bail, Result};
use ceramic_core::ssi::caip2;
use ceramic_event::unvalidated;
use ceramic_sql::sqlite::SqlitePool;
use tracing::warn;

use ceramic_validation::eth_rpc::{
    self, ChainInclusion, EthProofType, EthTxProofInput, HttpEthRpc,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Timestamp(u64);

impl Timestamp {
    /// A unix epoch timestamp
    #[allow(dead_code)]
    pub fn as_unix_ts(&self) -> u64 {
        self.0
    }
}

/// Provider for a remote Ethereum RPC endpoint.
pub type EthRpcProvider = Arc<dyn ChainInclusion<InclusionInput = EthTxProofInput> + Send + Sync>;

pub struct TimeEventValidator {
    /// we could support multiple providers for each chain (to get around rate limits)
    /// but we'll just force people to run a light client if they really need the throughput
    chain_providers: HashMap<caip2::ChainId, EthRpcProvider>,
}

impl std::fmt::Debug for TimeEventValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventTimestamper")
            .field(
                "chain_providers",
                &format!("{:?}", &self.chain_providers.keys()),
            )
            .finish()
    }
}

impl TimeEventValidator {
    /// Try to construct the validator by looking building the etherum rpc providers from the given URLsƒsw
    #[allow(dead_code)]
    pub async fn try_new(rpc_urls: &[String]) -> Result<Self> {
        let mut chain_providers = HashMap::with_capacity(rpc_urls.len());
        for url in rpc_urls {
            match HttpEthRpc::try_new(url).await {
                Ok(provider) => {
                    // use the first valid rpc client we find rather than replace one
                    // could support an array of clients for a chain if desired
                    let provider: EthRpcProvider = Arc::new(provider);
                    chain_providers
                        .entry(provider.chain_id().to_owned())
                        .or_insert_with(|| provider);
                }
                Err(err) => {
                    warn!(?err, "failed to create RPC client with url: '{url}'");
                }
            }
        }
        if chain_providers.is_empty() {
            bail!("failed to instantiate any RPC chain providers");
        }
        Ok(Self { chain_providers })
    }

    /// Create from known providers (e.g. inject mocks)
    /// Currently used in tests, may switch to this from service if we want to share RPC with anchoring.
    pub fn new_with_providers(providers: Vec<EthRpcProvider>) -> Self {
        Self {
            chain_providers: HashMap::from_iter(
                providers.into_iter().map(|p| (p.chain_id().to_owned(), p)),
            ),
        }
    }

    /// Get the CAIP2 Chain IDs that we can validate
    fn _supported_chains(&self) -> Vec<caip2::ChainId> {
        self.chain_providers.keys().cloned().collect()
    }

    /// Validate the chain inclusion proof for a time event, returning the block timestamp if found
    pub async fn validate_chain_inclusion(
        &self,
        _pool: &SqlitePool,
        event: &unvalidated::TimeEvent,
    ) -> Result<Timestamp, eth_rpc::Error> {
        let chain_id = caip2::ChainId::from_str(event.proof().chain_id())
            .map_err(|e| eth_rpc::Error::InvalidArgument(format!("invalid chain ID: {}", e)))?;

        let provider = self
            .chain_providers
            .get(&chain_id)
            .ok_or_else(|| eth_rpc::Error::NoChainProvider(chain_id.clone()))?;

        let input = EthTxProofInput {
            tx_hash: event.proof().tx_hash(),
            tx_type: EthProofType::from_str(event.proof().tx_type())
                .map_err(|e| eth_rpc::Error::InvalidProof(e.to_string()))?,
        };
        let proof = provider.chain_inclusion_proof(&input).await?;

        if proof.root_cid != event.proof().root() {
            return Err(eth_rpc::Error::InvalidProof(format!(
                "the root CID is not in the transaction (root={})",
                event.proof().root()
            )));
        }

        Ok(Timestamp(proof.timestamp))
    }
}

#[cfg(test)]
mod test {
    use ceramic_event::unvalidated;
    use ceramic_validation::eth_rpc;
    use cid::Cid;
    use ipld_core::ipld::Ipld;
    use mockall::{mock, predicate};
    use test_log::test;

    use super::*;

    const BLOCK_TIMESTAMP: u64 = 1725913338;

    fn time_event_single_event_batch() -> unvalidated::TimeEvent {
        unvalidated::Builder::time()
            .with_id(
                Cid::from_str("bagcqcerar2aga7747dm6fota3iipogz4q55gkaamcx2weebs6emvtvie2oha")
                    .unwrap(),
            )
            .with_tx(
                "eip155:11155111".into(),
                Cid::from_str("bagjqcgzadp7fstu7fz5tfi474ugsjqx5h6yvevn54w5m4akayhegdsonwciq")
                    .unwrap(),
                "f(bytes32)".into(),
            )
            .with_root(0, ipld_core::ipld! {[Cid::from_str("bagcqcerae5oqoglzjjgz53enwsttl7mqglp5eoh2llzbbvfktmzxleeiffbq").unwrap(), Ipld::Null, Cid::from_str("bafyreifjkogkhyqvr2gtymsndsfg3wpr7fg4q5r3opmdxoddfj4s2dyuoa").unwrap()]})
            .build()
            .expect("should be valid time event")
    }

    fn time_event_multi_event_batch() -> unvalidated::TimeEvent {
        unvalidated::Builder::time()
            .with_id(
                Cid::from_str("bagcqceraxr7s7s32wsashm6mm4fonhpkvfdky4rvw6sntlu2pxtl3fjhj2aa")
                    .unwrap(),
            )
            .with_tx(
                "eip155:11155111".into(),
                Cid::from_str("baeabeiamytbvhuehk5hojp3sdeuml27rhzua3rt7iqozrsyjgtlo55ilci")
                    .unwrap(),
                "f(bytes32)".into(),
            )
            .with_root(
                0,
                ipld_core::ipld!([
                    Cid::from_str("bafyreigyzzgpsarcwsiaoqbagihgqf2kdmq6mn6g52iplqo2cn4hpqbsk4")
                        .unwrap(),
                    Ipld::Null,
                ]),
            )
            .with_witness_node(
                0,
                ipld_core::ipld!([
                    Cid::from_str("bafyreicuu43ajn4gmigwdhv2kfsyhmhmsbz7thdltuqwp735wmaaxlzvdm")
                        .unwrap(),
                    Cid::from_str("baeabeicjhmihwfyx7eukvfefhck7albjmyt4xgghhi72q5cg5fwuxak3hm")
                        .unwrap(),
                ]),
            )
            .with_witness_node(
                0,
                ipld_core::ipld!([
                    Cid::from_str("bafyreigiyzwc7lh2us6xjui4weijkvfrq23yc45lu4mbkftvxfcqoianqi")
                        .unwrap(),
                    Ipld::Null,
                ]),
            )
            .with_witness_node(
                0,
                ipld_core::ipld!([
                    Cid::from_str("bafyreidexf3n3ji5yvno7rs3eyi42y4xgtntdnfdscw65cefwbtbxfedn4")
                        .unwrap(),
                    Ipld::Null,
                ]),
            )
            .with_witness_node(
                0,
                ipld_core::ipld!([
                    Cid::from_str("bafyreiaikclmu72enf4wemzcpxs2iicugzzmpxdfmzamlf7mpgteqhdqom")
                        .unwrap(),
                    Ipld::Null,
                ]),
            )
            .with_witness_node(
                0,
                ipld_core::ipld!([
                    Cid::from_str("bafyreiezfdh57dn5gvrhaggs6m7oj3egyw6hfwelgk5mflp2mbwgjqqxgy")
                        .unwrap(),
                    Cid::from_str("baeabeibfht5n57gyyvffv77de22smn66dbqiurk6rabs4kngh7gqw37ioe")
                        .unwrap(),
                ]),
            )
            .with_witness_node(
                0,
                ipld_core::ipld!([
                    Cid::from_str("bafyreiayw5uvplis64yky7oycdaep3xzoth3ick4mni5r7z3qpyftz4ckq")
                        .unwrap(),
                    Cid::from_str("baeabeiayulxmo26bv3psp4rljm5o23stmd6csqh2q7mnbalxeo5h6d7uqu")
                        .unwrap(),
                ]),
            )
            .with_witness_node(
                0,
                ipld_core::ipld!([
                    Cid::from_str("bafyreiexyd67nfvmrk3hgskirocyedvulrbouxfvc2cmkpynusqwnn7wcm")
                        .unwrap(),
                    Cid::from_str("baeabeieargrkzus5ijtgosvsne2wxzkqtly4ojfocfmexlxjm44muli5rq")
                        .unwrap(),
                ]),
            )
            .with_witness_node(
                0,
                ipld_core::ipld!([
                    Cid::from_str("bafyreifz3udm4qd5uxhx2whjnuohbzqqu2tnsp3ozcx2ppkqe3kewdlmuy")
                        .unwrap(),
                    Cid::from_str("baeabeif2muwy33aphh3dg2guzxf2tsthalrlwpjsopxh6ebgqeycckegeu")
                        .unwrap(),
                ]),
            )
            .with_witness_node(
                0,
                ipld_core::ipld!([
                    Cid::from_str("bagcqceraxr7s7s32wsashm6mm4fonhpkvfdky4rvw6sntlu2pxtl3fjhj2aa")
                        .unwrap(),
                    Cid::from_str("baeabeif423tedaykqve2xmapfpsgdmyos4hzbd77dt5se564akfumyksym")
                        .unwrap(),
                ]),
            )
            .build()
            .expect("should be valid time event")
    }

    mock! {
        pub EthRpcProviderTest {}
        #[async_trait::async_trait]
        impl ChainInclusion for EthRpcProviderTest {
            type InclusionInput = EthTxProofInput;

            fn chain_id(&self) -> &caip2::ChainId;
            async fn chain_inclusion_proof(&self, input: &EthTxProofInput) -> Result<eth_rpc::TimeProof, eth_rpc::Error>;
        }
    }

    async fn get_mock_provider(
        input: eth_rpc::EthTxProofInput,
        root_cid: Cid,
    ) -> TimeEventValidator {
        let mut mock_provider = MockEthRpcProviderTest::new();
        let chain =
            caip2::ChainId::from_str("eip155:11155111").expect("eip155:11155111 is a valid chain");

        mock_provider.expect_chain_id().once().return_const(chain);
        mock_provider
            .expect_chain_inclusion_proof()
            .once()
            .with(predicate::eq(input))
            .return_once(move |_| {
                Ok(eth_rpc::TimeProof {
                    timestamp: BLOCK_TIMESTAMP,
                    root_cid,
                })
            });
        TimeEventValidator::new_with_providers(vec![Arc::new(mock_provider)])
    }

    #[test(tokio::test)]
    async fn valid_proof_single() {
        let event = time_event_single_event_batch();
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let input = EthTxProofInput {
            tx_hash: event.proof().tx_hash(),
            tx_type: event.proof().tx_type().parse().unwrap(),
        };

        let verifier = get_mock_provider(input, event.proof().root()).await;
        match verifier.validate_chain_inclusion(&pool, &event).await {
            Ok(ts) => {
                assert_eq!(ts.as_unix_ts(), BLOCK_TIMESTAMP);
            }
            Err(e) => panic!("should have passed: {:?}", e),
        }
    }

    #[test(tokio::test)]
    async fn invalid_proof_single() {
        let event = time_event_single_event_batch();
        let pool = SqlitePool::connect_in_memory().await.unwrap();
        let input = EthTxProofInput {
            tx_hash: event.proof().tx_hash(),
            tx_type: event.proof().tx_type().parse().unwrap(),
        };

        let random_root =
            Cid::from_str("bagcqceraxr7s7s32wsashm6mm4fonhpkvfdky4rvw6sntlu2pxtl3fjhj2aa").unwrap();
        let verifier = get_mock_provider(input, random_root).await;
        match verifier.validate_chain_inclusion(&pool, &event).await {
            Ok(v) => {
                panic!("should have failed: {:?}", v)
            }
            Err(e) => match e {
                eth_rpc::Error::InvalidProof(e) => assert!(
                    e.contains("the root CID is not in the transaction"),
                    "{:#}",
                    e
                ),
                err => panic!("got wrong error: {:?}", err),
            },
        }
    }

    #[test(tokio::test)]
    async fn valid_proof_multi() {
        let event = time_event_multi_event_batch();
        let pool = SqlitePool::connect_in_memory().await.unwrap();

        let input = EthTxProofInput {
            tx_hash: event.proof().tx_hash(),
            tx_type: event.proof().tx_type().parse().unwrap(),
        };

        let verifier = get_mock_provider(input, event.proof().root()).await;

        match verifier.validate_chain_inclusion(&pool, &event).await {
            Ok(ts) => {
                assert_eq!(ts.as_unix_ts(), BLOCK_TIMESTAMP);
            }
            Err(e) => panic!("should have passed: {:?}", e),
        }
    }

    #[test(tokio::test)]
    async fn invalid_root_tx_proof_cid_multi() {
        let event = time_event_multi_event_batch();
        let pool = SqlitePool::connect_in_memory().await.unwrap();

        let input = EthTxProofInput {
            tx_hash: event.proof().tx_hash(),
            tx_type: event.proof().tx_type().parse().unwrap(),
        };

        let random_root =
            Cid::from_str("bagcqceraxr7s7s32wsashm6mm4fonhpkvfdky4rvw6sntlu2pxtl3fjhj2aa").unwrap();
        let verifier = get_mock_provider(input, random_root).await;
        match verifier.validate_chain_inclusion(&pool, &event).await {
            Ok(v) => {
                panic!("should have failed: {:?}", v)
            }
            Err(e) => match e {
                eth_rpc::Error::InvalidProof(e) => assert!(
                    e.contains("the root CID is not in the transaction"),
                    "{:#}",
                    e
                ),
                err => panic!("got wrong error: {:?}", err),
            },
        }
    }
}
