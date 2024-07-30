use anyhow::Result;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as b64, Engine as _};
use ceramic_core::{Cid, StreamId, StreamIdType};
use ceramic_p2p::Keypair;
use multihash_codetable::{Code, MultihashDigest};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
fn cid_to_stream_id(cid: Cid) -> StreamId {
    StreamId {
        r#type: StreamIdType::Unloadable,
        cid,
    }
}

async fn auth_jwt(
    request_url: String,
    controller: String,
    digest: String,
    secret: &[u8; 32],
) -> Result<String> {
    let signing_key = Ed25519KeyPair::from_seed_unchecked(secret).unwrap();
    let public_key_bytes = signing_key.public_key().as_ref();
    let public_key_b58 = multibase::encode(
        multibase::Base::Base58Btc,
        [b"\xed\x01", public_key_bytes].concat(),
    );
    println!("did:key:{}", public_key_b58);
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
    keypair: Keypair,
}

impl TransactionManager for RemoteCas {
    async fn make_proof(&self, root: Cid) -> Result<Receipt> {
        let node_controller = std::env::var("NODE_DID").unwrap();
        let signing_key_bytes = hex::decode(std::env::var("NODE_PRIVATE_KEY").unwrap()).unwrap();
        let cas_api_url = "https://cas-dev.3boxlabs.com".to_owned();
        let anchor_response = Self::create_anchor_request(
            cas_api_url,
            node_controller,
            root,
            &signing_key_bytes.try_into().unwrap(),
        )
        .await?;
        println!("{}", anchor_response);
        // Poll the CAS asynchronously for the anchor request status

        Ok(Receipt {
            proof_cid: Default::default(),
            path_prefix: None,
            blocks: vec![],
        })
    }
}

impl RemoteCas {
    // async fn new(p2p_key_dir: PathBuf) -> Result<Self> {
    //     let mut kc = Keychain::<DiskStorage>::new(p2p_key_dir.clone()).await?;
    //     // TOOD: Handle error later
    //     let mut keys = kc.keys().as_mut().unwrap;
    //     let keypair = keys.next().unwrap();
    //     Ok(Self { keypair })
    // }

    pub async fn create_anchor_request(
        cas_api_url: String,
        node_controller: String,
        root_cid: Cid,
        secret: &[u8; 32],
    ) -> Result<String> {
        let cas_create_request_url = format!("{}/api/v0/requests", cas_api_url);
        // let auth_header = auth_header(cas_api_url.clone(), node_controller, root_cid).await?;
        let cas_request_body = serde_json::to_string(&CasAnchorRequest {
            stream_id: cid_to_stream_id(root_cid),
            cid: root_cid.to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            ceramic_one_version: AGENT_VERSION.to_owned(),
        })?;
        // hash = sha256.hash(u8a.fromString(JSON.stringify(requestOpts.body)))
        let digest = MultihashDigest::digest(&Code::Sha2_256, cas_request_body.as_bytes());
        let digest = hex::encode(digest.digest());
        println!("digest {}", digest);
        let auth_jwt = auth_jwt(
            cas_create_request_url.clone(),
            node_controller,
            format!("0x{}", digest),
            secret,
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

// Tests to call the CAS request
#[cfg(test)]
mod tests {
    use super::*;
    use expect_test::expect;

    fn dag_cbor_mock_cid() -> Cid {
        let mock_data = serde_ipld_dagcbor::to_vec(b"mock root").unwrap();
        let mock_hash = MultihashDigest::digest(&Code::Sha2_256, &mock_data);
        Cid::new_v1(0x71, mock_hash)
    }

    #[tokio::test]
    #[ignore]
    async fn test_create_anchor_request_on_cas() {
        let node_controller = std::env::var("NODE_DID").unwrap();
        let signing_key_bytes = hex::decode(std::env::var("NODE_PRIVATE_KEY").unwrap()).unwrap();
        let cas_api_url = "https://cas-dev.3boxlabs.com".to_owned();
        let result = RemoteCas::create_anchor_request(
            cas_api_url,
            node_controller,
            dag_cbor_mock_cid(),
            &signing_key_bytes.try_into().unwrap(),
        )
        .await;
        expect!["Request is pending."].assert_eq(&result.unwrap());
    }

    #[tokio::test]
    async fn test_jwt() {
        let node_controller = std::env::var("NODE_DID").unwrap();
        let signing_key_bytes = hex::decode(std::env::var("NODE_PRIVATE_KEY").unwrap()).unwrap();
        let mock_data = serde_ipld_dagcbor::to_vec(b"mock root").unwrap();
        let mock_hash = MultihashDigest::digest(&Code::Sha2_256, &mock_data);
        let token = auth_jwt(
            "https://cas-dev.3boxlabs.com".to_owned(),
            node_controller,
            hex::encode(mock_hash.digest()),
            &signing_key_bytes.try_into().unwrap(),
        )
        .await
        .unwrap();
        println!("token {}", &token);
    }
}
