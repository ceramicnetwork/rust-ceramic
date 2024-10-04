use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD_NO_PAD as b64_standard, Engine as _};
use bytes::Bytes;
use fvm_shared::address::Address;
use hoku_provider::json_rpc::JsonRpcProvider;
use hoku_provider::tx::TxReceipt;
use hoku_sdk::machine::accumulator::{Accumulator, PushReturn};
use hoku_sdk::machine::Machine;
use hoku_signer::key::SecretKey;
use hoku_signer::{key::parse_secret_key, AccountKind, SubnetID, Wallet};

use ceramic_anchor_service::{
    DetachedTimeEvent, MerkleNode, MerkleNodes, RootTimeEvent, TransactionManager,
};
use ceramic_core::{Cid, SerializeExt};
use ceramic_event::unvalidated::Proof;

/// Hoku RPC
pub struct HokuRpc {
    secret_key: SecretKey,
    time_hub: Accumulator,
    provider: JsonRpcProvider,
    subnet_id: SubnetID,
    poll_interval: Duration,
    poll_retry_count: u64,
}

#[async_trait]
impl TransactionManager for HokuRpc {
    async fn anchor_root(&self, root_cid: Cid) -> Result<RootTimeEvent> {
        let tx_receipt = self.create_anchor_request(root_cid).await?;
        let proof = Proof::new(
            "hoku:mainnet".to_string(),
            root_cid,
            root_cid,
            format!(
                "hoku({}:{})",
                self.time_hub.address(),
                tx_receipt.data.unwrap().index
            ),
        );
        let proof_cid = proof.to_cid()?;
        return Ok(RootTimeEvent {
            proof,
            detached_time_event: DetachedTimeEvent {
                path: "".to_string(),
                proof: proof_cid,
            },
            remote_merkle_nodes: Default::default(),
        });
    }
}

impl HokuRpc {
    /// Create a new Hoku RPC instance
    pub async fn new(
        private_key: &str,
        hoku_rpc_url: &str,
        hoku_timehub_address: &str,
        hoku_subnet: &str,
        anchor_poll_interval: Duration,
        anchor_poll_retry_count: u64,
    ) -> Result<Self> {
        let secret_key = parse_ed25519_private_key(private_key)?;
        let secret_key = parse_secret_key(secret_key.as_str())?;
        let provider = JsonRpcProvider::new_http(hoku_rpc_url.parse()?, None, None)?;
        let subnet_id = hoku_subnet.parse()?;
        Ok(Self {
            secret_key,
            time_hub: Accumulator::attach(Address::from_bytes(hoku_timehub_address.as_ref())?)
                .await?,
            provider,
            subnet_id,
            poll_interval: anchor_poll_interval,
            poll_retry_count: anchor_poll_retry_count,
        })
    }

    /// Create an anchor request on the remote CAS
    pub async fn create_anchor_request(&self, root_cid: Cid) -> Result<TxReceipt<PushReturn>> {
        let mut signer = Wallet::new_secp256k1(
            self.secret_key.clone(),
            AccountKind::Ethereum,
            self.subnet_id.clone(),
        )?;
        signer.init_sequence(&self.provider).await?;

        self.time_hub
            .push(
                &self.provider,
                &mut signer,
                Bytes::from(root_cid.to_bytes()),
                Default::default(),
            )
            .await
    }
}

fn parse_ed25519_private_key(key: &str) -> Result<String> {
    // Remove header and footer
    let key = key
        .lines()
        .filter(|line| !line.contains("BEGIN") && !line.contains("END"))
        .collect::<String>();

    // Decode base64
    let decoded = b64_standard.decode(key.as_bytes())?;

    // The secret key starts at index 47 and is 32 bytes long
    if decoded.len() < 79 {
        return Err(anyhow!("Invalid key length"));
    }

    // Return the hex-encoded secret key
    Ok(hex::encode(&decoded[47..79]))
}

// Tests to call the CAS request
#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::time::Duration;

    use expect_test::expect_file;
    use multihash_codetable::{Code, MultihashDigest};
    use ring::signature::Ed25519KeyPair;

    use ceramic_anchor_service::{
        AnchorService, MockAnchorEventService, Store, TransactionManager,
    };
    use ceramic_core::Cid;
    use ceramic_sql::sqlite::SqlitePool;

    // #[tokio::test]
    // #[ignore]
    // async fn test_anchor_batch_with_cas() {
    //     let anchor_client = Arc::new(MockAnchorEventService::new(10));
    //     let anchor_requests = anchor_client
    //         .events_since_high_water_mark(NodeId::random().0, 0, 1_000_000)
    //         .await
    //         .unwrap();
    //     let (node_id, keypair) = node_id_and_private_key();
    //     let remote_cas = Arc::new(HokuRpc::new(
    //         node_id,
    //         keypair,
    //         "https://cas-dev.3boxlabs.com".to_owned(),
    //         Duration::from_secs(1),
    //         1,
    //     ));
    //     let anchor_service = AnchorService::new(
    //         remote_cas,
    //         anchor_client,
    //         SqlitePool::connect_in_memory().await.unwrap(),
    //         NodeId::random().0,
    //         Duration::from_secs(1),
    //         10,
    //     );
    //     let all_blocks = anchor_service
    //         .anchor_batch(anchor_requests.as_slice())
    //         .await
    //         .unwrap();
    //     expect_file!["./test-data/test_anchor_batch_with_cas.test.txt"]
    //         .assert_debug_eq(&all_blocks);
    // }
    //
    // #[tokio::test]
    // #[ignore]
    // async fn test_create_anchor_request_with_cas() {
    //     let mock_root_cid =
    //         Cid::from_str("bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54").unwrap();
    //     let (node_id, keypair) = node_id_and_private_key();
    //
    //     let remote_cas = HokuRpc::new(
    //         node_id,
    //         keypair,
    //         "https://cas-dev.3boxlabs.com".to_owned(),
    //         Duration::from_secs(1),
    //         1,
    //     );
    //     let receipt = remote_cas.anchor_root(mock_root_cid).await;
    //     expect_file!["./test-data/create_anchor_request_on_cas.test.txt"].assert_debug_eq(&receipt);
    // }
    //
    // #[tokio::test]
    // async fn test_anchor_response() {
    //     let anchor_response = include_str!("test-data/anchor_response.json").to_string();
    //     let CasResponseParseResult::Anchored(receipt) =
    //         parse_anchor_response(anchor_response).await.unwrap()
    //     else {
    //         panic!("expected anchored receipt");
    //     };
    //     expect_file!["./test-data/anchor_response.test.txt"].assert_debug_eq(&receipt);
    // }
    //
    // #[tokio::test]
    // async fn test_jwt() {
    //     let mock_data = serde_ipld_dagcbor::to_vec(b"mock root").unwrap();
    //     let mock_hash = MultihashDigest::digest(&Code::Sha2_256, &mock_data);
    //     let (node_id, keypair) = node_id_and_private_key();
    //     let remote_cas = Arc::new(HokuRpc::new(
    //         node_id,
    //         keypair,
    //         "https://cas-dev.3boxlabs.com".to_owned(),
    //         Duration::from_secs(1),
    //         1,
    //     ));
    //     remote_cas
    //         .auth_jwt(hex::encode(mock_hash.digest()))
    //         .await
    //         .unwrap();
    // }
}
