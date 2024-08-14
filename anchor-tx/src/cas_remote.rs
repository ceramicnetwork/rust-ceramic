use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use base64::{
    engine::general_purpose::{STANDARD_NO_PAD as b64_standard, URL_SAFE_NO_PAD as b64},
    Engine as _,
};
use futures::TryStreamExt;
use multihash_codetable::{Code, MultihashDigest};
use ring::signature::Ed25519KeyPair;
use serde::{Deserialize, Serialize};
use tokio::time::interval;
use tracing::{debug, warn};
use uuid::Uuid;

use ceramic_anchor_service::{
    DetachedTimeEvent, MerkleNode, MerkleNodes, Receipt, TransactionManager,
};
use ceramic_car::CarReader;
use ceramic_core::{
    cid_from_ed25519_key_pair, did_key_from_ed25519_key_pair, Cid, StreamId, StreamIdType,
};
use ceramic_event::unvalidated::Proof;

pub const AGENT_VERSION: &str = concat!("ceramic-one/", env!("CARGO_PKG_VERSION"));

#[derive(Serialize, Deserialize, Debug)]
struct CasAuthPayload {
    url: String,
    nonce: String,
    digest: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct CasAnchorRequest {
    stream_id: StreamId,
    cid: String,
    timestamp: String,
    ceramic_one_version: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Header {
    pub alg: String,
    pub kid: String,
    pub typ: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    nonce: String,
    url: String,
    digest: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct AnchorResponse {
    pub message: String,
    pub witness_car: Option<String>,
}

fn cid_to_stream_id(cid: Cid) -> StreamId {
    StreamId {
        r#type: StreamIdType::Unloadable,
        cid,
    }
}

/// Remote CAS transaction manager
pub struct RemoteCas {
    signing_key: Ed25519KeyPair,
    url: String,
    poll_interval: Duration,
    poll_retry_count: u32,
    jws_header_b64: String,
    http_client: reqwest::Client,
    /// The did:key of the node represented as a StreamId
    node_stream_id: StreamId,
}

enum CasResponseParseResult {
    Anchored(Box<Receipt>),
    Unauthorized,
}

#[async_trait]
impl TransactionManager for RemoteCas {
    async fn make_proof(&self, root: Cid) -> Result<Receipt> {
        let mut interval = interval(self.poll_interval);
        for _ in 0..self.poll_retry_count {
            let anchor_response = self.create_anchor_request(root).await?;
            match parse_anchor_response(anchor_response).await {
                Ok(CasResponseParseResult::Anchored(receipt)) => {
                    return Ok(*receipt);
                }
                Ok(CasResponseParseResult::Unauthorized) => {
                    return Err(anyhow!("remote CAS request unauthorized"));
                }
                Err(e) => {
                    debug!("swallowing anchoring result: {}", e);
                }
            }
            interval.tick().await;
        }
        Err(anyhow::anyhow!(
            "{} not anchored after {} attempts {}s apart",
            root.to_string(),
            self.poll_retry_count,
            self.poll_interval.as_secs(),
        ))
    }
}

impl RemoteCas {
    /// Create a new RemoteCas instance
    pub fn new(
        keypair: Ed25519KeyPair,
        remote_anchor_service_url: String,
        anchor_poll_interval: Duration,
    ) -> Self {
        let controller = did_key_from_ed25519_key_pair(&keypair);
        let jws_header = Header {
            kid: format!(
                "{}#{}",
                controller,
                controller
                    .strip_prefix("did:key:")
                    .expect("invalid did:key")
            ),
            alg: "EdDSA".to_string(),
            typ: "JWT".to_string(),
        };
        let jws_header_b64 =
            b64.encode(serde_json::to_vec(&jws_header).expect("invalid jws header"));
        let node_stream_id = cid_to_stream_id(cid_from_ed25519_key_pair(&keypair));
        Self {
            signing_key: keypair,
            url: format!("{}/api/v0/anchor", remote_anchor_service_url),
            poll_interval: anchor_poll_interval,
            poll_retry_count: 12,
            jws_header_b64,
            http_client: reqwest::Client::new(),
            node_stream_id,
        }
    }

    /// Create an anchor request on the remote CAS
    pub async fn create_anchor_request(&self, root_cid: Cid) -> Result<String> {
        let cas_request_body = serde_json::to_string(&CasAnchorRequest {
            stream_id: self.node_stream_id.clone(),
            cid: root_cid.to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            ceramic_one_version: AGENT_VERSION.to_owned(),
        })?;
        let digest = MultihashDigest::digest(&Code::Sha2_256, cas_request_body.as_bytes());
        let digest = hex::encode(digest.digest());
        let auth_jwt = self.auth_jwt(format!("0x{}", digest)).await?;
        let auth_header = format!("Bearer {}", auth_jwt);
        debug!("auth_header {}", &auth_header);
        let res = self
            .http_client
            .post(self.url.clone())
            .header("Authorization", auth_header)
            .header("Content-Type", "application/json")
            .body(cas_request_body)
            .send()
            .await?;
        Ok(res.text().await?)
    }

    async fn auth_jwt(&self, digest: String) -> Result<String> {
        let body = Claims {
            digest,
            nonce: Uuid::new_v4().to_string(),
            url: self.url.clone(),
        };
        let body_b64 = b64.encode(serde_json::to_vec(&body)?);
        let message = [self.jws_header_b64.clone(), body_b64].join(".");
        let sig_b64 = b64.encode(self.signing_key.sign(message.as_bytes()));
        Ok([message.clone(), sig_b64].join("."))
    }
}

async fn parse_anchor_response(anchor_response: String) -> Result<CasResponseParseResult> {
    // Return if we were unable to parse the anchor response
    let anchor_response = serde_json::from_str::<AnchorResponse>(anchor_response.as_str())?;

    // If the response does not contain a witness CAR file, the anchor request is either still pending or it failed
    // because the request was unauthorized.
    let Some(witness_car_b64) = anchor_response.witness_car else {
        return match anchor_response.message.as_str() {
            "Unauthorized" => Ok(CasResponseParseResult::Unauthorized),
            message => {
                return Err(anyhow!("message from remote CAS: {}", message));
            }
        };
    };
    let witness_car_bytes = b64_standard.decode(witness_car_b64)?;
    let car_reader = CarReader::new(witness_car_bytes.as_ref()).await?;
    let mut remote_merkle_nodes = MerkleNodes::default();
    let mut detached_time_event: Option<DetachedTimeEvent> = None;
    let mut proof: Option<Proof> = None;
    for (cid, block) in car_reader
        .stream()
        .into_stream()
        .try_collect::<Vec<(_, _)>>()
        .await?
    {
        if let Ok(block) = serde_ipld_dagcbor::from_slice::<DetachedTimeEvent>(&block) {
            detached_time_event = Some(block);
        } else if let Ok(block) = serde_ipld_dagcbor::from_slice::<Proof>(&block) {
            proof = Some(block);
        } else if let Ok(block) = serde_ipld_dagcbor::from_slice::<MerkleNode>(&block) {
            remote_merkle_nodes.insert(cid, block);
        } else {
            warn!(
                "unknown block type when processing witness CAR: {}, {}, {:?}",
                cid,
                hex::encode(block),
                proof
            );
        }
    }
    if detached_time_event.is_none() || proof.is_none() {
        return Err(anyhow::anyhow!("invalid anchor response"));
    }
    Ok(CasResponseParseResult::Anchored(Box::new(Receipt {
        proof: proof.expect("proof should be present"),
        detached_time_event: detached_time_event.expect("detached time event should be present"),
        remote_merkle_nodes,
    })))
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

    use ceramic_anchor_service::{AnchorService, Store, TransactionManager};
    use ceramic_core::{ed25519_key_pair_from_secret, Cid};

    use crate::cas_mock::MockAnchorClient;

    fn node_private_key() -> Ed25519KeyPair {
        ed25519_key_pair_from_secret(
            std::env::var("NODE_PRIVATE_KEY")
                .unwrap_or(
                    "f80264c02abf947a7bd4f24fc799168a21cdea5b9d3a8ce8f63801785a4dff7299af4"
                        .to_string(),
                )
                .as_str(),
        )
        .unwrap()
    }

    #[tokio::test]
    #[ignore]
    async fn test_anchor_batch_with_cas() {
        let anchor_client = Arc::new(MockAnchorClient::new(10));
        let anchor_requests = anchor_client.local_sourced_data_events().await.unwrap();
        let remote_cas = Arc::new(RemoteCas::new(
            node_private_key(),
            "https://cas-dev.3boxlabs.com".to_owned(),
            Duration::from_secs(1),
        ));
        let anchor_service = AnchorService::new(remote_cas, anchor_client, Duration::from_secs(1));
        let all_blocks = anchor_service
            .anchor_batch(anchor_requests.as_slice())
            .await
            .unwrap();
        expect_file!["./test-data/test_anchor_batch_with_cas.test.txt"]
            .assert_debug_eq(&all_blocks);
    }

    #[tokio::test]
    #[ignore]
    async fn test_create_anchor_request_on_cas() {
        let mock_root_cid =
            Cid::from_str("bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54").unwrap();

        let remote_cas = RemoteCas::new(
            node_private_key(),
            "https://cas-dev.3boxlabs.com".to_owned(),
            Duration::from_secs(1),
        );
        let receipt = remote_cas.make_proof(mock_root_cid).await;
        expect_file!["./test-data/create_anchor_request_on_cas.test.txt"].assert_debug_eq(&receipt);
    }

    #[tokio::test]
    async fn test_anchor_response() {
        let anchor_response = include_str!("test-data/anchor_response.json").to_string();
        let CasResponseParseResult::Anchored(receipt) =
            parse_anchor_response(anchor_response).await.unwrap()
        else {
            panic!("expected anchored receipt");
        };
        expect_file!["./test-data/anchor_response.test.txt"].assert_debug_eq(&receipt);
    }

    #[tokio::test]
    async fn test_jwt() {
        let mock_data = serde_ipld_dagcbor::to_vec(b"mock root").unwrap();
        let mock_hash = MultihashDigest::digest(&Code::Sha2_256, &mock_data);
        let remote_cas = Arc::new(RemoteCas::new(
            node_private_key(),
            "https://cas-dev.3boxlabs.com".to_owned(),
            Duration::from_secs(1),
        ));
        remote_cas
            .auth_jwt(hex::encode(mock_hash.digest()))
            .await
            .unwrap();
    }
}
