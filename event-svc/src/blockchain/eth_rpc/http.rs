use std::{
    collections::HashMap,
    num::NonZero,
    str::FromStr,
    sync::{Arc, Mutex},
};

use crate::eth_rpc::{
    types::ChainProofMetadata, ChainInclusion, ChainInclusionProof, Error, Timestamp,
};
use alloy::{
    hex,
    primitives::{BlockHash, TxHash},
    providers::{Provider, ProviderBuilder, RootProvider},
    rpc::types::{Block, BlockTransactionsKind, Transaction},
    transports::http::{Client, Http},
};
use anyhow::bail;
use ceramic_core::Cid;
use ceramic_event::unvalidated::AnchorProof;
use lru::LruCache;
use multihash_codetable::Multihash;
use once_cell::sync::Lazy;
use ssi::caip2;
use tracing::trace;

use super::EthProofType;

const DAG_CBOR_CODEC: u64 = 0x71;

static BLOCK_THRESHHOLDS: Lazy<HashMap<caip2::ChainId, u64>> = Lazy::new(|| {
    HashMap::from_iter(vec![
        (
            caip2::ChainId::from_str("eip155:1").expect("eip155:1 is valid"),
            16688195,
        ), //mainnet
        (
            caip2::ChainId::from_str("eip155:3").expect("eip155:1 is valid"),
            1000000000,
        ), //ropsten
        (
            caip2::ChainId::from_str("eip155:5").expect("eip155:5 is valid"),
            8498671,
        ), //goerli
        (
            caip2::ChainId::from_str("eip155:100").expect("eip155:100 is valid"),
            26509835,
        ), //gnosis
        (
            caip2::ChainId::from_str("eip155:11155111").expect("eip155:11155111 is valid"),
            5518585,
        ), // sepolia
        (
            caip2::ChainId::from_str("eip155:1337").expect("eip155:1337 is valid"),
            1,
        ), //ganache
    ])
});

const TRANSACTION_CACHE_SIZE: usize = 1_000;
const BLOCK_CACHE_SIZE: usize = 50;

type Result<T> = std::result::Result<T, Error>;

/// Http client to interact with EIP chains
pub struct HttpEthRpc {
    chain_id: caip2::ChainId,
    url: reqwest::Url,
    tx_cache: Arc<Mutex<LruCache<TxHash, Transaction>>>,
    block_cache: Arc<Mutex<LruCache<BlockHash, Block>>>,
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

    /// The url this provider is using as its RPC endpoint
    pub fn url(&self) -> String {
        self.url.to_string()
    }

    /// The chain ID this RPC is a provider for
    pub fn chain_id(&self) -> &caip2::ChainId {
        &self.chain_id
    }

    /// Get a block by its hash. For now, we return and cache a [`ChainBlock`] as it's much smaller than
    /// the actual Block returned from the RPC endpoint and we don't need most of the information.
    ///
    /// curl https://mainnet.infura.io/v3/{api_token} \
    ///     -X POST \
    ///     -H "Content-Type: application/json" \
    ///     -d '{"jsonrpc":"2.0","method":"eth_getBlockByHash","params": ["0x{block_hash}",false],"id":1}'
    /// >> {"jsonrpc": "2.0", "id": 1, "result": {"number": "0x105f34f", "timestamp": "0x644fe98b"}}
    async fn eth_block_by_hash(&self, block_hash: BlockHash) -> Result<Option<Block>> {
        if let Some(blk) = self.block_cache.lock().unwrap().get(&block_hash) {
            return Ok(Some(blk.to_owned()));
        }
        let block = self
            .provider
            .get_block_by_hash(block_hash, BlockTransactionsKind::Hashes)
            .await?;
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

/// Input is the data input to the contract for the transaction
fn get_root_cid_from_input(input: &str, tx_type: EthProofType) -> anyhow::Result<Cid> {
    let input = input.strip_prefix("0x").unwrap_or(input);
    match tx_type {
        EthProofType::V0 => {
            bail!("V0 anchor proofs are not supported. Tx input={input} Please report this error on the forum: https://forum.ceramic.network/");
        }
        EthProofType::V1 => {
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
    }
}

#[async_trait::async_trait]
impl ChainInclusion for HttpEthRpc {
    fn chain_id(&self) -> &caip2::ChainId {
        &self.chain_id
    }

    /// Get the block chain transaction if it exists with the block timestamp information
    async fn get_chain_inclusion_proof(&self, input: &AnchorProof) -> Result<ChainInclusionProof> {
        // transaction to blockHash, blockNumber, input
        let tx_hash = crate::blockchain::tx_hash_try_from_cid(input.tx_hash())
            .map_err(|e| Error::InvalidArgument(format!("invalid transaction hash: {}", e)))?;
        let tx_hash_res = match self.eth_transaction_by_hash(tx_hash).await? {
            Some(tx) => tx,
            None => {
                return Err(Error::TxNotFound {
                    chain_id: self.chain_id.clone(),
                    tx_hash: input.tx_hash().to_string(),
                })
            }
        };
        trace!(?tx_hash_res, "txByHash response");

        if let Some(block_hash) = &tx_hash_res.block_hash {
            // for now we ignore how old the block is i.e. we don't care if it's more than 3
            // it's left up to the implementer in
            // https://chainagnostic.org/CAIPs/caip-168 and https://namespaces.chainagnostic.org/eip155/caip168
            // this means nodes may have a slightly different answer to the exact time an event happened

            let blk_hash_res = match self.eth_block_by_hash(*block_hash).await? {
                Some(b) => b,
                None => {
                    return Err(Error::BlockNotFound {
                        chain_id: self.chain_id.clone(),
                        block_hash: block_hash.to_string(),
                    });
                }
            };
            trace!(?blk_hash_res, "blockByHash response");
            let tx_type = EthProofType::from_str(input.tx_type())
                .map_err(|e| Error::InvalidProof(e.to_string()))?;
            let tx_input = tx_hash_res.input.to_string();
            let root_cid = get_root_cid_from_input(&tx_input, tx_type)
                .map_err(|e| Error::InvalidProof(e.to_string()))?;

            if let Some(threshold) = BLOCK_THRESHHOLDS.get(self.chain_id()) {
                if blk_hash_res.header.number < *threshold {
                    return Err(Error::InvalidProof("V0 anchor proofs are not supported. Please report this error on the forum: https://forum.ceramic.network/".into()));
                } else if tx_type != EthProofType::V1 {
                    return Err(Error::InvalidProof(format!("Any anchor proofs created after block {threshold} for chain {} must include the txType field={}. Anchor txn blockNumber: {}", 
                    self.chain_id(), EthProofType::V1, blk_hash_res.header.number)));
                }
            }

            Ok(ChainInclusionProof {
                timestamp: Timestamp::from_unix_ts(blk_hash_res.header.timestamp),
                block_hash: block_hash.to_string(),
                root_cid,
                metadata: ChainProofMetadata {
                    chain_id: self.chain_id.clone(),
                    tx_hash: tx_hash.to_string(),
                    tx_input,
                },
            })
        } else {
            Err(Error::TxNotMined {
                chain_id: self.chain_id.clone(),
                tx_hash: input.tx_hash().to_string(),
            })
        }
    }
}

#[cfg(test)]
mod test {
    use ceramic_event::unvalidated;
    use ipld_core::ipld::Ipld;
    use test_log::test;

    use crate::eth_rpc::types::{V0_PROOF_TYPE, V1_PROOF_TYPE};

    use super::*;

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

    #[test(tokio::test)]
    async fn valid_proof_single() {
        let event = time_event_single_event_batch();
        let tx_type = event.proof().tx_type().parse().unwrap();
        let root = get_root_cid_from_input(SINGLE_TX_HASH_INPUT, tx_type).unwrap();
        assert_eq!(root, event.proof().root());
    }

    #[test(tokio::test)]
    async fn invalid_proof_single() {
        let event = time_event_single_event_batch();
        let tx_type = event.proof().tx_type().parse().unwrap();
        let root = get_root_cid_from_input(MULTI_TX_HASH_INPUT, tx_type).unwrap();
        assert_ne!(root, event.proof().root());
    }

    #[test(tokio::test)]
    async fn valid_proof_multi() {
        let event = time_event_multi_event_batch();
        let tx_type = event.proof().tx_type().parse().unwrap();
        let root = get_root_cid_from_input(MULTI_TX_HASH_INPUT, tx_type).unwrap();
        assert_eq!(root, event.proof().root());
    }

    #[test(tokio::test)]
    async fn invalid_root_tx_proof_cid_multi() {
        let event = time_event_multi_event_batch();
        let tx_type = event.proof().tx_type().parse().unwrap();
        let root = get_root_cid_from_input(SINGLE_TX_HASH_INPUT, tx_type).unwrap();
        assert_ne!(root, event.proof().root());
    }

    #[test]
    fn parse_tx_input_data_v1() {
        assert_eq!(
            Cid::from_str("bafyreigs2yqh2olnwzrsykyt6gvgsabk7hu5e7gtmjrkobq25af5x3y7be").unwrap(),
            get_root_cid_from_input(
                "0x97ad09ebd2d6207d396db6632c2b13f1aa69002af9e9d27cd36262a7061ae80bdbef1f09",
                V1_PROOF_TYPE.parse().expect("f(bytes32) is valid"),
            )
            .unwrap()
        );
    }

    #[test]
    fn parse_tx_input_data_v0_error() {
        assert!(get_root_cid_from_input(
            "0x01711220d2d6207d396db6632c2b13f1aa69002af9e9d27cd36262a7061ae80bdbef1f09",
            V0_PROOF_TYPE.parse().expect("raw is valid"),
        )
        .is_err());
    }
}
