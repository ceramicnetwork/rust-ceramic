use std::{collections::HashMap, str::FromStr, sync::Arc};

use anyhow::{anyhow, bail, Result};
use ceramic_core::ssi::caip2;
use ceramic_core::Cid;
use ceramic_event::unvalidated;
use ceramic_sql::sqlite::SqlitePool;
// use fendermint_vm_message::query::FvmQueryHeight;
use fvm_shared::address::Address;
use hoku_provider::json_rpc::JsonRpcProvider;
use hoku_provider::json_rpc::{HttpClient, Url};
use hoku_sdk::machine::accumulator::FvmQueryHeight;
use hoku_sdk::machine::{accumulator::Accumulator, Machine};
use multihash::Multihash;
use once_cell::sync::Lazy;
use tracing::warn;

use ceramic_validation::eth_rpc::{self, ChainBlock, EthRpc, HttpEthRpc};

const V0_PROOF_TYPE: &str = "raw";
const V1_PROOF_TYPE: &str = "f(bytes32)"; // See: https://namespaces.chainagnostic.org/eip155/caip168
const DAG_CBOR_CODEC: u64 = 0x71;

static BLOCK_THRESHHOLDS: Lazy<HashMap<&str, u64>> = Lazy::new(|| {
    HashMap::from_iter(vec![
        ("eip155:1", 16688195),       //mainnet
        ("eip155:3", 1000000000),     //ropsten
        ("eip155:5", 8498671),        //goerli
        ("eip155:100", 26509835),     //gnosis
        ("eip155:11155111", 5518585), // sepolia
        ("eip155:1337", 1),           //ganache
    ])
});

#[derive(Debug)]
pub enum ChainInclusionError {
    /// Transaction hash not found
    TxNotFound {
        chain_id: caip2::ChainId,
        tx_hash: String,
    },
    /// The transaction exists but has not been mined yet
    TxNotMined {
        chain_id: caip2::ChainId,
        tx_hash: String,
    },
    /// The proof was invalid for the given reason
    InvalidProof(String),
    /// No chain provider configured for the event, whether that's an error is up to the caller
    NoChainProvider(caip2::ChainId),
    /// The transaction was invalid or could not be verified for some reason
    /// This includes transient errors that need to be split out in the future.
    /// Plan to handle than after switching to alloy as the eth RPC client.
    Error(anyhow::Error),
}

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
pub type EthRpcProvider = Arc<dyn EthRpc + Send + Sync>;

pub struct TimeEventValidator {
    /// we could support multiple providers for each chain (to get around rate limits)
    /// but we'll just force people to run a light client if they really need the throughput
    chain_providers: HashMap<caip2::ChainId, EthRpcProvider>,
    // map of hoku chains to their rpc providers
    hoku_providers: HashMap<caip2::ChainId, JsonRpcProvider<HttpClient>>,
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
    pub async fn try_new(eth_rpc_urls: &[String], hoku_rpc_urls: &[&str]) -> Result<Self> {
        let mut chain_providers = HashMap::with_capacity(eth_rpc_urls.len());
        for url in eth_rpc_urls {
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
        let mut hoku_providers: HashMap<_, JsonRpcProvider<HttpClient>> =
            HashMap::with_capacity(hoku_rpc_urls.len());

        for url in hoku_rpc_urls {
            match JsonRpcProvider::new_http(url.parse()?, None, None) {
                Ok(provider) => {
                    // use the first valid rpc client we find rather than replace one
                    // could support an array of clients for a chain if desired
                    hoku_providers
                        .entry("hoku:mainnet".parse()?)
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
        if hoku_providers.is_empty() {
            bail!("failed to instantiate any RPC hoku providers");
        }
        Ok(Self {
            chain_providers,
            hoku_providers,
        })
    }

    /// Create from known providers (e.g. inject mocks)
    /// Currently used in tests, may switch to this from service if we want to share RPC with anchoring.
    pub fn new_with_providers(
        eth_providers: Vec<EthRpcProvider>,
        hoku_providers: Vec<JsonRpcProvider<HttpClient>>,
    ) -> Self {
        Self {
            chain_providers: HashMap::from_iter(
                eth_providers
                    .into_iter()
                    .map(|p| (p.chain_id().to_owned(), p)),
            ),
            hoku_providers: HashMap::from_iter(
                hoku_providers
                    .into_iter()
                    .map(|p| ("hoku:mainnet".parse().unwrap(), p)),
            ),
        }
    }

    /// Get the CAIP2 Chain IDs that we can validate
    fn _supported_chains(&self) -> Vec<caip2::ChainId> {
        self.chain_providers.keys().cloned().collect()
    }

    pub async fn validate_chain_inclusion(
        &self,
        _pool: &SqlitePool,
        event: &unvalidated::TimeEvent,
    ) -> Result<Timestamp, ChainInclusionError> {
        match event.proof().chain_id() {
            "hoku:mainnet" => self.validate_hoku(_pool, event).await,
            _ => self.validate_eth(_pool, event).await,
        }
    }

    /// Validate the chain inclusion proof for a time event, returning the block timestamp if found
    /// {
    //   "chainId": "hoku:mainnet:",
    //   "root": {"/": "bafyreicbwzaiyg2l4uaw6zjds3xupqeyfq3nlb36xodusgn24ou3qvgy4e"},
    //   "txHash": {"/": "t2xplsbor65en7jome74tk73e5gcgqazuwm5qqamy:42"},
    //   "txType": "hoku(address:index)"
    // }
    pub async fn validate_hoku(
        &self,
        _pool: &SqlitePool,
        event: &unvalidated::TimeEvent,
    ) -> Result<Timestamp, ChainInclusionError> {
        // todo!("hoku validation not implemented yet");
        let chain_id = caip2::ChainId::from_str(event.proof().chain_id())
            .map_err(|e| ChainInclusionError::Error(anyhow!("invalid chain ID: {}", e)))?;

        let provider = self
            .hoku_providers
            .get(&chain_id)
            .ok_or_else(|| ChainInclusionError::NoChainProvider(chain_id.clone()))?;
        let (accumulator_address, leaf_index) = Self::parse_hoku_tx_type(event.proof().tx_type())?;

        let machine = Accumulator::attach(accumulator_address)
            .await
            .map_err(|e| {
                ChainInclusionError::Error(anyhow!("Failed to attach to accumulator: {}", e))
            })?;

        // Fetch the leaf at the given index
        let (timestamp, leaf) = machine
            .leaf(provider, leaf_index, FvmQueryHeight::Committed)
            .await
            .unwrap();

        if leaf != event.proof().root().to_bytes() {
            return Err(ChainInclusionError::InvalidProof(
                "The root CID does not match the leaf in the accumulator".into(),
            ));
        }
        return Ok(Timestamp(timestamp));
    }

    /// Validate the chain inclusion proof for a time event, returning the block timestamp if found
    pub async fn validate_eth(
        &self,
        _pool: &SqlitePool,
        event: &unvalidated::TimeEvent,
    ) -> Result<Timestamp, ChainInclusionError> {
        let chain_id = caip2::ChainId::from_str(event.proof().chain_id())
            .map_err(|e| ChainInclusionError::Error(anyhow!("invalid chain ID: {}", e)))?;

        let provider = self
            .chain_providers
            .get(&chain_id)
            .ok_or_else(|| ChainInclusionError::NoChainProvider(chain_id.clone()))?;
        let tx_hash = Self::expected_tx_hash(event.proof().tx_hash());

        // TODO: check db or lru cache for transaction.
        //     if known => return it
        //     else if new => query it
        //     else if we've tried before and it's been "long enough" => query it
        // for now, we just use the rpc endpoint again which has a small internal LRU cache
        let (root_cid, block) = match self
            .get_block(provider, &tx_hash, event.proof())
            .await
            .map_err(ChainInclusionError::Error)?
        {
            Some(v) => match v.1 {
                Some(block) => (v.0, block),
                None => return Err(ChainInclusionError::TxNotMined { chain_id, tx_hash }), // block has not been mined yet so time information can't be determined
            },
            None => return Err(ChainInclusionError::TxNotFound { chain_id, tx_hash }),
        };

        if root_cid != event.proof().root() {
            return Err(ChainInclusionError::InvalidProof(format!(
                "the root CID is not in the transaction (root={})",
                event.proof().root()
            )));
        }

        if let Some(threshold) = BLOCK_THRESHHOLDS.get(event.proof().chain_id()) {
            if block.number < *threshold {
                return Err(ChainInclusionError::InvalidProof("V0 anchor proofs are not supported. Please report this error on the forum: https://forum.ceramic.network/".into()));
            } else if event.proof().tx_type() != V1_PROOF_TYPE {
                return Err(ChainInclusionError::InvalidProof(format!("Any anchor proofs created after block {threshold} for chain {} must include the txType field={V1_PROOF_TYPE}. Anchor txn blockNumber: {}", event.proof().chain_id(), block.number)));
            }
        }

        Ok(Timestamp(block.timestamp))
    }

    /// Input is the data input to the contract for the transaction
    fn get_root_cid_from_input(input: &str, tx_type: &str) -> Result<Cid> {
        let input = input.strip_prefix("0x").unwrap_or(input);
        match tx_type {
            V0_PROOF_TYPE => {
                bail!("V0 anchor proofs are not supported. Tx input={input} Please report this error on the forum: https://forum.ceramic.network/");
            }
            V1_PROOF_TYPE => {
                /*
                From the CAIP: https://namespaces.chainagnostic.org/eip155/caip168

                The first 4 bytes are the function signature and can be discarded, the next 32 bytes is the first argument of the function, which is expected to be a 32 byte hex encoded partial CID.
                The partial CID is the multihash portion of the original CIDv1. It does not include the multibase, the CID version or the IPLD codec segments.
                It is assumed that the IPLD codec is dag-cbor.

                We could explicitly strip "0x97ad09eb" to make sure it's actually our contract address (0x231055A0852D67C7107Ad0d0DFeab60278fE6AdC) but we are more lax for now.
                */
                let decoded = hex::decode(input.as_bytes())?;
                if decoded.len() != 36 {
                    bail!("transaction input should be 36 bytes not {}", decoded.len())
                }
                // 0x12 -> sha2-256
                // 0x20 -> 32 bytes (256 bits) of hash
                let root_bytes =
                    Multihash::from_bytes(&[&[0x12_u8, 0x20], &decoded.as_slice()[4..]].concat())?;
                Ok(Cid::new_v1(DAG_CBOR_CODEC, root_bytes))
            }
            v => {
                bail!("Unknown proof type: {}", v)
            }
        }
    }

    async fn get_block(
        &self,
        provider: &EthRpcProvider,
        tx_hash: &str,
        proof: &unvalidated::Proof,
    ) -> Result<Option<(Cid, Option<ChainBlock>)>> {
        match provider.get_block_timestamp(tx_hash).await {
            Ok(Some(tx)) => {
                let root_cid = Self::get_root_cid_from_input(&tx.input, proof.tx_type())?;

                // TODO: persist transaction and block information somewhere (lru cache, database)
                // so it can be found for conclusions without needing to hit the rpc endpoint again
                Ok(Some((root_cid, tx.block)))
            }

            Ok(None) => {
                // no transaction will be turned into an error at the next level.
                // we should probably persist something so we know that it's bad and we don't keep trying
                Ok(None)
            }
            Err(eth_rpc::Error::Application(error)) => bail!(error),
            Err(eth_rpc::Error::InvalidArgument(reason)) => {
                bail!(format!("Invalid ethereum rpc argument: {reason}"))
            }
            Err(eth_rpc::Error::Transient(error)) => {
                // TODO: actually retry something
                bail!(error);
            }
        }
    }

    /// Validate the chain inclusion proof for a time event, returning the block timestamp if found
    /// {
    //   "chainId": "hoku:mainnet:",
    //   "root": {"/": "bafyreicbwzaiyg2l4uaw6zjds3xupqeyfq3nlb36xodusgn24ou3qvgy4e"},
    //   "txHash": {"/": "t2xplsbor65en7jome74tk73e5gcgqazuwm5qqamy:42"},
    //   "txType": "hoku(address:index)"
    // }
    fn parse_hoku_tx_type(tx_type: &str) -> Result<(Address, u64), ChainInclusionError> {
        let tx_type = tx_type.strip_prefix("hoku(").unwrap_or(tx_type);
        let tx_type = tx_type.strip_suffix(")").unwrap_or(tx_type);
        let parts: Vec<&str> = tx_type.split(':').collect();
        if parts.len() != 2 {
            return Err(ChainInclusionError::Error(anyhow!(
                "Invalid Hoku tx_type format"
            )));
        }

        let address = Address::from_str(parts[0]).map_err(|e| {
            ChainInclusionError::Error(anyhow!("Invalid accumulator address: {}", e))
        })?;
        let index = parts[1].parse::<u64>().unwrap();
        Ok((address, index))
    }

    fn expected_tx_hash(cid: Cid) -> String {
        format!("0x{}", hex::encode(cid.hash().digest()))
    }
}

#[cfg(test)]
mod test {
    use ceramic_event::unvalidated;
    use ceramic_validation::eth_rpc::TxHash;
    use ipld_core::ipld::Ipld;
    use mockall::{mock, predicate};
    use test_log::test;

    use super::*;

    const BLOCK_TIMESTAMP: u64 = 1725913338;
    const SINGLE_TX_HASH: &str =
        "0x1bfe594e9f2e7b32a39fe50d24c2fd3fb15255bde5bace0140c1c861c9cdb091";
    const MULTI_TX_HASH: &str =
        "0x0cc4c353d087574ee4bf721928c5ebf13e680dc67f441d98cb0934d6eef50b12";

    const SINGLE_TX_HASH_INPUT: &str =
        "0x97ad09eb7d6b5b17e15037a18de992fc3ad1efa7662b1a598b6d9c243a5e9463edc050d1";
    const MULTI_TX_HASH_INPUT: &str =
        "0x97ad09eb1a315930c36a6252c157a565b7fca969230faa8cd695172538138ac579488f65";

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
        impl EthRpc for EthRpcProviderTest {
            fn chain_id(&self) -> &caip2::ChainId;
            fn url(&self) -> String;
            async fn get_block_timestamp(&self, tx_hash: &str) -> Result<Option<ceramic_validation::eth_rpc::ChainTransaction>, eth_rpc::Error>;
        }
    }

    async fn get_mock_provider(tx_hash: String, tx_input: String) -> TimeEventValidator {
        let tx_hash_bytes = TxHash::from_str(&tx_hash).expect("invalid tx hash");
        let mut mock_provider = MockEthRpcProviderTest::new();
        let chain =
            caip2::ChainId::from_str("eip155:11155111").expect("eip155:11155111 is a valid chain");

        mock_provider.expect_chain_id().once().return_const(chain);
        mock_provider
            .expect_get_block_timestamp()
            .once()
            .with(predicate::eq(tx_hash.clone()))
            .return_once(move |_| {
                Ok(Some(ceramic_validation::eth_rpc::ChainTransaction {
                    hash: tx_hash_bytes,
                    input: tx_input,
                    block: Some(ChainBlock {
                        hash: TxHash::from_str(
                            "0x783cd5a6febe13d08ac0d59fa7e666483d5e476542b29688a6f0bec3d15febd4",
                        )
                        .unwrap(),
                        number: 5558585,
                        timestamp: BLOCK_TIMESTAMP,
                    }),
                }))
            });
        TimeEventValidator::new_with_providers(vec![Arc::new(mock_provider)])
    }

    #[test(tokio::test)]
    async fn valid_proof_single() {
        let event = time_event_single_event_batch();
        let pool = SqlitePool::connect_in_memory().await.unwrap();

        let verifier =
            get_mock_provider(SINGLE_TX_HASH.to_string(), SINGLE_TX_HASH_INPUT.into()).await;
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

        let verifier =
            get_mock_provider(SINGLE_TX_HASH.to_string(), MULTI_TX_HASH_INPUT.to_string()).await;
        match verifier.validate_chain_inclusion(&pool, &event).await {
            Ok(v) => {
                panic!("should have failed: {:?}", v)
            }
            Err(e) => match e {
                ChainInclusionError::InvalidProof(e) => assert!(
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

        let verifier =
            get_mock_provider(MULTI_TX_HASH.to_string(), MULTI_TX_HASH_INPUT.to_string()).await;
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

        let verifier =
            get_mock_provider(MULTI_TX_HASH.to_string(), SINGLE_TX_HASH_INPUT.to_string()).await;
        match verifier.validate_chain_inclusion(&pool, &event).await {
            Ok(v) => {
                panic!("should have failed: {:?}", v)
            }
            Err(e) => match e {
                ChainInclusionError::InvalidProof(e) => assert!(
                    e.contains("the root CID is not in the transaction"),
                    "{:#}",
                    e
                ),
                err => panic!("got wrong error: {:?}", err),
            },
        }
    }

    #[test]
    fn parse_tx_input_data_v1() {
        assert_eq!(
            Cid::from_str("bafyreigs2yqh2olnwzrsykyt6gvgsabk7hu5e7gtmjrkobq25af5x3y7be").unwrap(),
            TimeEventValidator::get_root_cid_from_input(
                "0x97ad09ebd2d6207d396db6632c2b13f1aa69002af9e9d27cd36262a7061ae80bdbef1f09",
                V1_PROOF_TYPE,
            )
            .unwrap()
        );
    }

    #[test]
    fn parse_tx_input_data_v0_error() {
        assert!(TimeEventValidator::get_root_cid_from_input(
            "0x01711220d2d6207d396db6632c2b13f1aa69002af9e9d27cd36262a7061ae80bdbef1f09",
            V0_PROOF_TYPE,
        )
        .is_err());
    }
}
