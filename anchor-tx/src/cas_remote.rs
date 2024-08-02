use anyhow::Result;
use async_trait::async_trait;
use base64::{
    engine::general_purpose::{STANDARD_NO_PAD as b64_standard, URL_SAFE_NO_PAD as b64},
    Engine as _,
};
use futures::TryStreamExt;
use iroh_car::CarReader;
use multihash_codetable::{Code, MultihashDigest};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use ceramic_core::{
    cid_from_ed25519_key_pair, did_key_from_ed25519_key_pair, Cid, DagCborIpfsBlock, StreamId,
    StreamIdType,
};

use crate::{Receipt, TransactionManager};

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
    pub witness_car: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct DetachedTimeEvent {
    pub path: String,
    pub proof: Cid,
}

fn cid_to_stream_id(cid: Cid) -> StreamId {
    StreamId {
        r#type: StreamIdType::Unloadable,
        cid,
    }
}

async fn auth_jwt(
    request_url: String,
    digest: String,
    signing_key: &Ed25519KeyPair,
) -> Result<String> {
    let public_key_bytes = signing_key.public_key().as_ref();
    let public_key_b58 = multibase::encode(
        multibase::Base::Base58Btc,
        [b"\xed\x01", public_key_bytes].concat(),
    );
    println!("did:key:{}", public_key_b58);
    let controller = did_key_from_ed25519_key_pair(signing_key);
    let header = Header {
        // multibase.encode('base58btc', (b'\xed\x01' + public_key)))
        kid: format!(
            "{}#{}",
            controller,
            controller.strip_prefix("did:key:").unwrap()
        ),
        alg: "EdDSA".to_string(),
    };
    let body = Claims {
        digest,
        nonce: Uuid::new_v4().to_string(),
        url: request_url,
    };

    let header_b64 = b64.encode(serde_json::to_vec(&header).unwrap());
    let body_b64 = b64.encode(serde_json::to_vec(&body).unwrap());
    let message = [header_b64, body_b64].join(".");
    let sig_bytes = signing_key.sign(message.as_bytes());
    let sig_b64 = b64.encode(sig_bytes);
    Ok([message.clone(), sig_b64].join("."))
}

pub struct RemoteCas {
    signing_key: Ed25519KeyPair,
    cas_api_url: String,
}

#[async_trait]
impl TransactionManager for RemoteCas {
    async fn make_proof(&self, root: Cid) -> Result<Receipt> {
        let anchor_response = self.create_anchor_request(root).await?;
        println!("{}", anchor_response);
        // TODO: Poll the CAS asynchronously for the anchor request status
        Ok(parse_anchor_response(anchor_response).await)
    }
}

impl RemoteCas {
    pub fn new(keypair: Ed25519KeyPair, cas_api_url: String) -> Self {
        Self {
            signing_key: keypair,
            cas_api_url,
        }
    }

    pub async fn create_anchor_request(&self, root_cid: Cid) -> Result<String> {
        let cas_create_request_url = format!("{}/api/v0/requests", self.cas_api_url);
        let cas_request_body = serde_json::to_string(&CasAnchorRequest {
            stream_id: cid_to_stream_id(cid_from_ed25519_key_pair(&self.signing_key)),
            cid: root_cid.to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            ceramic_one_version: AGENT_VERSION.to_owned(),
        })?;
        let digest = MultihashDigest::digest(&Code::Sha2_256, cas_request_body.as_bytes());
        let digest = hex::encode(digest.digest());
        println!("digest {}", digest);
        let auth_jwt = auth_jwt(
            cas_create_request_url.clone(),
            format!("0x{}", digest),
            &self.signing_key,
        )
        .await?;
        let auth_header = format!("Bearer {}", auth_jwt);
        println!("auth_header {}", &auth_header);
        let res = reqwest::Client::new()
            .post(cas_create_request_url)
            .header("Authorization", auth_header)
            .header("Content-Type", "application/json")
            .body(cas_request_body)
            .send()
            .await?;
        Ok(res.text().await?)
    }
}

async fn parse_anchor_response(anchor_response: String) -> Receipt {
    let witness_car_b64 = serde_json::from_str::<AnchorResponse>(anchor_response.as_str())
        .unwrap()
        .witness_car;
    let witness_car_bytes = b64_standard.decode(witness_car_b64).unwrap();
    let car_reader = CarReader::new(witness_car_bytes.as_ref()).await.unwrap();
    let header = car_reader.header();
    let root_cid = header.roots()[0];
    let blocks: Vec<(Cid, Vec<u8>)> = car_reader.stream().try_collect().await.unwrap();
    let detached_time_event_bytes = blocks
        .clone()
        .into_iter()
        .find(|(block_cid, _)| block_cid.eq(&root_cid))
        .unwrap()
        .1;
    let blocks: Vec<DagCborIpfsBlock> = blocks
        .into_iter()
        .filter(|(block_cid, _)| !block_cid.eq(&root_cid))
        .map(move |(_, value)| DagCborIpfsBlock::from(value))
        .collect();
    let detached_time_event: DetachedTimeEvent =
        serde_ipld_dagcbor::from_slice(&detached_time_event_bytes).unwrap();
    let proof_block = blocks
        .iter()
        .find(|&block| block.cid.eq(&detached_time_event.proof))
        .unwrap();
    Receipt {
        proof_block: proof_block.clone(),
        path_prefix: Some(detached_time_event.path),
        blocks,
    }
}

// Tests to call the CAS request
#[cfg(test)]
mod tests {
    use super::*;
    use ceramic_core::ed25519_key_pair_from_secret;
    use cid::CidGeneric;
    use expect_test::expect_file;
    use std::str::FromStr;

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
    async fn test_create_anchor_request_on_cas() {
        let mock_root_cid: CidGeneric<64> =
            Cid::from_str("bafyreia776z4jdg5zgycivcpr3q6lcu6llfowkrljkmq3bex2k5hkzat54").unwrap();

        let remote_cas = RemoteCas::new(
            node_private_key(),
            "https://cas-dev.3boxlabs.com".to_owned(),
        );
        let receipt = remote_cas.make_proof(mock_root_cid).await;
        expect_file!["./test-data/anchor-response.test.txt"].assert_debug_eq(&receipt.unwrap());
    }

    #[tokio::test]
    async fn test_anchor_response() {
        let anchor_response = include_str!("./test-data/anchor-response.json").to_string();
        let receipt = parse_anchor_response(anchor_response).await;
        expect_file!["./test-data/anchor-response.test.txt"].assert_debug_eq(&receipt);
    }

    #[tokio::test]
    async fn test_jwt() {
        let mock_data = serde_ipld_dagcbor::to_vec(b"mock root").unwrap();
        let mock_hash = MultihashDigest::digest(&Code::Sha2_256, &mock_data);
        let token = auth_jwt(
            "https://cas-dev.3boxlabs.com".to_owned(),
            hex::encode(mock_hash.digest()),
            &node_private_key(),
        )
        .await
        .unwrap();
        println!("token {}", &token);
    }
}
