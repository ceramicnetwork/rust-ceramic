use std::{collections::HashMap, str::FromStr, sync::Arc};

use anyhow::{anyhow, bail, Context as _, Result};
use ceramic_core::ssi::caip2;
use ceramic_core::Cid;
use ceramic_event::unvalidated;
use ceramic_sql::sqlite::SqlitePool;
use multihash::Multihash;
use once_cell::sync::Lazy;
use tracing::warn;

use ceramic_validation::eth_rpc::{ChainBlock, EthRpc, HttpEthRpc};

const V0_PROOF_TYPE: &str = "raw";
const V1_PROOF_TYPE: &str = "f(bytes32)"; // See: https://namespaces.chainagnostic.org/eip155/caip168
const DAG_CBOR_CODEC: u64 = 0x71;

static BLOCK_THRESHHOLDS: Lazy<HashMap<&str, i64>> = Lazy::new(|| {
    HashMap::from_iter(vec![
        ("eip155:1", 16688195),       //mainnet
        ("eip155:3", 1000000000),     //ropsten
        ("eip155:5", 8498671),        //goerli
        ("eip155:100", 26509835),     //gnosis
        ("eip155:11155111", 5518585), // sepolia
        ("eip155:1337", 1),           //ganache
    ])
});

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    /// A unix epoch timestamp
    pub fn as_unix_ts(&self) -> i64 {
        self.0
    }
}

#[async_trait::async_trait]
pub trait BlockchainVerifier {
    /// Get the CAIP-2 chains that are supported for validating time events
    fn supported_chains(&self) -> Vec<caip2::ChainId>;

    /// Verify the time event anchor information
    async fn validate_chain_inclusion(
        &self,
        event: &unvalidated::TimeEvent,
    ) -> Result<Option<Timestamp>>;
}

pub type EthRpcProvider = Arc<dyn EthRpc + Send + Sync>;

pub struct EventTimestamper<'a> {
    // TODO: will be needed to persist transaction and proof information
    _pool: &'a SqlitePool,
    /// we could support multiple providers for each chain (to get around rate limits)
    /// but we'll just force people to run a light client if they really need the throughput
    chain_providers: HashMap<caip2::ChainId, EthRpcProvider>,
}

impl<'a> EventTimestamper<'a> {
    pub async fn try_new(pool: &'a SqlitePool, urls: &[&str]) -> Result<Self> {
        let mut chain_providers = HashMap::with_capacity(urls.len());
        for url in urls {
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
                    warn!("failed to create RCP client with url: '{url}': {err}");
                }
            }
        }
        if chain_providers.is_empty() {
            bail!("failed to instantiate any RPC chain providers");
        }
        Ok(Self {
            _pool: pool,
            chain_providers,
        })
    }

    /// Create from known providers (e.g. inject mocks)
    pub fn new_with_providers(pool: &'a SqlitePool, providers: Vec<EthRpcProvider>) -> Self {
        Self {
            _pool: pool,
            chain_providers: HashMap::from_iter(
                providers.into_iter().map(|p| (p.chain_id().to_owned(), p)),
            ),
        }
    }

    /// Input is the data input to the contract for the transaction
    fn get_root_cid_from_input(input: &str, tx_type: &str) -> Result<Cid> {
        let input = input.strip_prefix("0x").unwrap_or(input);
        match tx_type {
            V0_PROOF_TYPE => {
                // A hex-encoded CID. The data value is a byte-friendly string.
                // If its length is odd, a single '0' may be prepended to make its length even
                let root_bytes = if input.as_bytes().len() % 2 != 0 {
                    hex::decode([&[0_u8], input.as_bytes()].concat())?
                } else {
                    hex::decode(input.as_bytes())?
                };

                Ok(Cid::read_bytes(root_bytes.as_slice()).context("invalid v0 proof CID")?)
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
        match provider.get_block_timestamp(tx_hash).await? {
            Some(tx) => {
                let root_cid = Self::get_root_cid_from_input(&tx.input, proof.tx_type())?;

                // TODO: persist transaction and block information somewhere (lru cache, database)
                // so it can be found for conclusions without needing to hit the rpc endpoint again
                Ok(Some((root_cid, tx.block)))
            }

            None => {
                // no transaction will be turned into an error at the next level.
                // we should probably persist something so we know that it's bad and we don't keep trying
                Ok(None)
            }
        }
    }

    fn expected_tx_hash(cid: Cid) -> String {
        format!("0x{}", hex::encode(cid.hash().digest()))
    }
}

#[async_trait::async_trait]
impl<'a> BlockchainVerifier for EventTimestamper<'a> {
    fn supported_chains(&self) -> Vec<caip2::ChainId> {
        self.chain_providers.keys().cloned().collect()
    }

    async fn validate_chain_inclusion(
        &self,
        event: &unvalidated::TimeEvent,
    ) -> Result<Option<Timestamp>> {
        let chain_id =
            caip2::ChainId::from_str(event.proof().chain_id()).context("invalid proof chain ID")?;

        let provider = self
            .chain_providers
            .get(&chain_id)
            .ok_or_else(|| anyhow!("missing rpc verifier for chain ID"))?;
        let tx_hash = Self::expected_tx_hash(event.proof().tx_hash());

        // TODO: check db or lru cache for transaction.
        //     if known => return it
        //     else if new => query it
        //     else if we've tried before and it's been "long enough" => query it
        // for now, we just use the rpc endpoint again which has a small internal LRU cache
        let (root_cid, block) = match self.get_block(provider, &tx_hash, event.proof()).await? {
            Some(v) => match v.1 {
                Some(block) => (v.0, block),
                None => return Ok(None), // block has not been mined yet so time information can't be determined
            },
            None => {
                bail!("transaction {tx_hash} not found");
            }
        };

        if root_cid != event.proof().root() {
            bail!(
                "the root CID is not in the transaction (root={})",
                event.proof().root()
            )
        }

        if let Some(threshold) = BLOCK_THRESHHOLDS.get(event.proof().chain_id()) {
            if block.number < *threshold {
                return Ok(Some(Timestamp(block.timestamp)));
            } else if event.proof().tx_type() != V1_PROOF_TYPE {
                bail!("Any anchor proofs created after block {threshold} for chain {} must include the txType field={V1_PROOF_TYPE}. Anchor txn blockNumber: {}", event.proof().chain_id(), block.number);
            }
        }

        Ok(Some(Timestamp(block.timestamp)))
    }
}

#[cfg(test)]
mod test {
    use ceramic_event::unvalidated;
    use mockall::{mock, predicate};
    use test_log::test;

    use super::*;

    pub const TIME_EVENT_CAR_SINGLE_EVENT_BATCH: &str = "uOqJlcm9vdHOB2CpYJQABcRIgcmqgb7eHSgQ32hS1NGVKZruLJGcKDI1f4lqOyNYn3eVndmVyc2lvbgG3AQFxEiByaqBvt4dKBDfaFLU0ZUpmu4skZwoMjV_iWo7I1ifd5aRiaWTYKlgmAAGFARIgjoBgf_z42eK6YNoQ9xs8h3plAAwV9WIQMvEZWdUE045kcGF0aGEwZHByZXbYKlgmAAGFARIgJ10HGXlKTZ7sjbSnNf2QMt_SOPpa8hDUqpszdZCIKUNlcHJvb2bYKlglAAFxEiAFKLx3fi7-yD1aPNyqnblI_r_5XllReVz55jBMvMxs9q4BAXESIAUovHd-Lv7IPVo83KqduUj-v_leWVF5XPnmMEy8zGz2pGRyb2902CpYJQABcRIgfWtbF-FQN6GN6ZL8OtHvp2YrGlmLbZwkOl6UY-3AUNFmdHhIYXNo2CpYJgABkwEbIBv-WU6fLnsyo5_lDSTC_T-xUlW95brOAUDByGHJzbCRZnR4VHlwZWpmKGJ5dGVzMzIpZ2NoYWluSWRvZWlwMTU1OjExMTU1MTExeQFxEiB9a1sX4VA3oY3pkvw60e-nZisaWYttnCQ6XpRj7cBQ0YPYKlgmAAGFARIgJ10HGXlKTZ7sjbSnNf2QMt_SOPpa8hDUqpszdZCIKUP22CpYJQABcRIgqVOMo-IVjo08Mk0cim3Z8flNyHY7c9g7uGMqeS0PFHA";
    pub const TIME_EVENT_CAR_MULTI_EVENT_BATCH: &str = "mOqJlcm9vdHOB2CpYJQABcRIgL4UtIONTCPoEwUqgviwfyYNUmHFm/2CX5zTiYjE1v35ndmVyc2lvbgHJAQFxEiAvhS0g41MI+gTBSqC+LB/Jg1SYcWb/YJfnNOJiMTW/fqRiaWTYKlgmAAGFARIgvH8vy3q0gSOzzGcK5p3qqUascjW3pNmumn3mvZUnToBkcGF0aHMwLzAvMC8wLzAvMC8wLzAvMC8wZHByZXbYKlgmAAGFARIgvH8vy3q0gSOzzGcK5p3qqUascjW3pNmumn3mvZUnToBlcHJvb2bYKlglAAFxEiBI8mz+1T/2/X8/intyZfWokF1bkIl092NtMxkdmFnE5K0BAXESIEjybP7VP/b9fz+Ke3Jl9aiQXVuQiXT3Y20zGR2YWcTkpGRyb2902CpYJQABcRIgGjFZMMNqYlLBV6Vlt/ypaSMPqozWlRclOBOKxXlIj2VmdHhIYXNo2CpYJQABABIgDMTDU9CHV07kv3IZKMXr8T5oDcZ/RB2Yywk01u71CxJmdHhUeXBlamYoYnl0ZXMzMilnY2hhaW5JZG9laXAxNTU6MTExNTUxMTFPAXESIBoxWTDDamJSwVelZbf8qWkjD6qM1pUXJTgTisV5SI9lgtgqWCUAAXESINjOTPkCIrSQB0AgMg5oF0obIeY3xu6Q9cHaE3h3wDJX9ncBcRIg2M5M+QIitJAHQCAyDmgXShsh5jfG7pD1wdoTeHfAMleC2CpYJQABcRIgVKc2BLeGYg1hnrpRZYOw7JBz+ZxrnSFn/32zAAuvNRvYKlglAAEAEiBJOxB7Fxf5KKqUhTiV8CwpZifLmMc6P6h0RultS4FbO08BcRIgVKc2BLeGYg1hnrpRZYOw7JBz+ZxrnSFn/32zAAuvNRuC2CpYJQABcRIgyMZsL6z6pL100RyxEJVUsYa3gXOrpxgVFnW5RQcgDYL2TwFxEiDIxmwvrPqkvXTRHLEQlVSxhreBc6unGBUWdblFByANgoLYKlglAAFxEiBkuXbdpR3FWu/GWyYRzWOXNNsxtKOQre6IhbBmG5SDb/ZPAXESIGS5dt2lHcVa78ZbJhHNY5c02zG0o5Ct7oiFsGYblINvgtgqWCUAAXESIAhQlsp/RGl5YjMifeWkIFQ2csfcZWZAxZfseaZIHHBz9ncBcRIgCFCWyn9EaXliMyJ95aQgVDZyx9xlZkDFl+x5pkgccHOC2CpYJQABcRIgmSjP3429NWJwGNLzPuTshsW8ctiLMrrCrfpgbGTCFzbYKlglAAEAEiAlPPre/NjFSlr/4ya1JjfeGGCKRV6IAy4ppj/NC2/ocXcBcRIgmSjP3429NWJwGNLzPuTshsW8ctiLMrrCrfpgbGTCFzaC2CpYJQABcRIgGLdpV60S9zCsfdgQwEfu+XTPtAlcY1HY/zuD8FnnglTYKlglAAEAEiAYou7Ha8Gu3yfyK0s67W5TYPwpQPqH2NCBdyO6fw/0hXcBcRIgGLdpV60S9zCsfdgQwEfu+XTPtAlcY1HY/zuD8FnnglSC2CpYJQABcRIgl8D99pasirZzSUiLhYIOtFxC6ly1FoTFPw2koWa39hPYKlglAAEAEiCAiaKs0l1CZmdKsmk1a+VQmvHHJK4RWEuu6Wc4yi0djHcBcRIgl8D99pasirZzSUiLhYIOtFxC6ly1FoTFPw2koWa39hOC2CpYJQABcRIgud0GzkB9pc99WOltHHDmEKam2T9uyK+nvVAm1EsNbKbYKlglAAEAEiC6ZS2N7A859jNo1M3LqcpnAuK7PTJz7n8QJoEwISiGJXgBcRIgud0GzkB9pc99WOltHHDmEKam2T9uyK+nvVAm1EsNbKaC2CpYJgABhQESILx/L8t6tIEjs8xnCuad6qlGrHI1t6TZrpp95r2VJ06A2CpYJQABABIgvNbmQYMKhUmrsA8r5GGzDpcPkI//HPsid9wCi0ZhUsM";

    const BLOCK_TIMESTAMP: i64 = 1725913338;
    const SINGLE_TX_HASH: &str =
        "0x1bfe594e9f2e7b32a39fe50d24c2fd3fb15255bde5bace0140c1c861c9cdb091";
    const MULTI_TX_HASH: &str =
        "0x0cc4c353d087574ee4bf721928c5ebf13e680dc67f441d98cb0934d6eef50b12";

    const SINGLE_TX_HASH_INPUT: &str =
        "0x97ad09eb7d6b5b17e15037a18de992fc3ad1efa7662b1a598b6d9c243a5e9463edc050d1";
    const MULTI_TX_HASH_INPUT: &str =
        "0x97ad09eb1a315930c36a6252c157a565b7fca969230faa8cd695172538138ac579488f65";

    mock! {
        pub EthRpcProviderTest {}
        #[async_trait::async_trait]
        impl EthRpc for EthRpcProviderTest {
            fn chain_id(&self) -> &caip2::ChainId;
            async fn get_block_timestamp(&self, tx_hash: &str) -> Result<Option<ceramic_validation::eth_rpc::ChainTransaction>>;
        }
    }

    async fn get_mock_provider(
        pool: &SqlitePool,
        tx_hash: String,
        tx_input: String,
    ) -> EventTimestamper<'_> {
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
                    hash: tx_hash,
                    input: tx_input,
                    block: Some(ChainBlock {
                        hash: "0x783cd5a6febe13d08ac0d59fa7e666483d5e476542b29688a6f0bec3d15febd4"
                            .into(),
                        number: 5558585,
                        timestamp: BLOCK_TIMESTAMP,
                    }),
                }))
            });
        let verifier = EventTimestamper::new_with_providers(pool, vec![Arc::new(mock_provider)]);
        verifier
    }

    #[test(tokio::test)]
    async fn valid_proof_single() {
        let (_base, data) = multibase::decode(TIME_EVENT_CAR_SINGLE_EVENT_BATCH).unwrap();
        let (_cid, event) =
            unvalidated::Event::<ipld_core::ipld::Ipld>::decode_car(data.as_slice(), true).unwrap();
        let pool = SqlitePool::connect_in_memory().await.unwrap();

        let verifier = get_mock_provider(
            &pool,
            SINGLE_TX_HASH.to_string(),
            SINGLE_TX_HASH_INPUT.into(),
        )
        .await;
        match event {
            unvalidated::Event::Time(t) => match verifier.validate_chain_inclusion(&t).await {
                Ok(ts) => {
                    let ts = ts.expect("should have timestamp");
                    assert_eq!(ts.as_unix_ts(), BLOCK_TIMESTAMP);
                }
                Err(e) => panic!("should have passed: {:#}", e),
            },
            unvalidated::Event::Signed(_) => unreachable!("not signed"),
            unvalidated::Event::Unsigned(_) => unreachable!("not unsigned"),
        }
    }

    #[test(tokio::test)]
    async fn invalid_proof_single() {
        let (_base, data) = multibase::decode(TIME_EVENT_CAR_SINGLE_EVENT_BATCH).unwrap();
        let (_cid, event) =
            unvalidated::Event::<ipld_core::ipld::Ipld>::decode_car(data.as_slice(), true).unwrap();
        let pool = SqlitePool::connect_in_memory().await.unwrap();

        let verifier = get_mock_provider(
            &pool,
            SINGLE_TX_HASH.to_string(),
            MULTI_TX_HASH_INPUT.to_string(),
        )
        .await;
        match event {
            unvalidated::Event::Time(t) => match verifier.validate_chain_inclusion(&t).await {
                Ok(v) => {
                    panic!("should have failed: {:?}", v)
                }
                Err(e) => assert!(
                    e.to_string()
                        .contains("the root CID is not in the transaction"),
                    "{:#}",
                    e
                ),
            },
            unvalidated::Event::Signed(_) => unreachable!("not signed"),
            unvalidated::Event::Unsigned(_) => unreachable!("not unsigned"),
        }
    }

    #[test(tokio::test)]
    async fn valid_proof_multi() {
        let (_base, data) = multibase::decode(TIME_EVENT_CAR_MULTI_EVENT_BATCH).unwrap();
        let (_cid, event) =
            unvalidated::Event::<ipld_core::ipld::Ipld>::decode_car(data.as_slice(), true).unwrap();
        let pool = SqlitePool::connect_in_memory().await.unwrap();

        let verifier = get_mock_provider(
            &pool,
            MULTI_TX_HASH.to_string(),
            MULTI_TX_HASH_INPUT.to_string(),
        )
        .await;
        match event {
            unvalidated::Event::Time(t) => match verifier.validate_chain_inclusion(&t).await {
                Ok(ts) => {
                    let ts = ts.expect("should have timestamp");
                    assert_eq!(ts.as_unix_ts(), BLOCK_TIMESTAMP);
                }
                Err(e) => panic!("should have passed: {:#}", e),
            },
            unvalidated::Event::Signed(_) => unreachable!("not signed"),
            unvalidated::Event::Unsigned(_) => unreachable!("not unsigned"),
        }
    }

    #[test(tokio::test)]
    async fn invalid_root_tx_proof_cid_multi() {
        let (_base, data) = multibase::decode(TIME_EVENT_CAR_MULTI_EVENT_BATCH).unwrap();
        let (_cid, event) =
            unvalidated::Event::<ipld_core::ipld::Ipld>::decode_car(data.as_slice(), true).unwrap();
        let pool = SqlitePool::connect_in_memory().await.unwrap();

        let verifier = get_mock_provider(
            &pool,
            MULTI_TX_HASH.to_string(),
            SINGLE_TX_HASH_INPUT.to_string(),
        )
        .await;
        match event {
            unvalidated::Event::Time(t) => match verifier.validate_chain_inclusion(&t).await {
                Ok(v) => {
                    panic!("should have failed: {:?}", v)
                }
                Err(e) => assert!(
                    e.to_string()
                        .contains("the root CID is not in the transaction"),
                    "{:#}",
                    e
                ),
            },
            unvalidated::Event::Signed(_) => unreachable!("not signed"),
            unvalidated::Event::Unsigned(_) => unreachable!("not unsigned"),
        }
    }

    #[test]
    fn parse_tx_input_data_v1() {
        assert_eq!(
            Cid::from_str("bafyreigs2yqh2olnwzrsykyt6gvgsabk7hu5e7gtmjrkobq25af5x3y7be").unwrap(),
            EventTimestamper::get_root_cid_from_input(
                "0x97ad09ebd2d6207d396db6632c2b13f1aa69002af9e9d27cd36262a7061ae80bdbef1f09",
                V1_PROOF_TYPE,
            )
            .unwrap()
        );
    }

    #[test]
    fn parse_tx_input_data_v0() {
        assert_eq!(
            Cid::from_str("bafyreigs2yqh2olnwzrsykyt6gvgsabk7hu5e7gtmjrkobq25af5x3y7be").unwrap(),
            EventTimestamper::get_root_cid_from_input(
                "0x01711220d2d6207d396db6632c2b13f1aa69002af9e9d27cd36262a7061ae80bdbef1f09",
                V0_PROOF_TYPE,
            )
            .unwrap()
        );
    }
}
