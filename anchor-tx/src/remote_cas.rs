use anyhow::Result;
use multihash_codetable::{Code, MultihashDigest};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use ceramic_core::{
    ssi, Base64String, Base64UrlString, Bytes, Cid, DagCborIpfsBlock, DidDocument, StreamId,
    StreamIdType,
};
use ceramic_event::unvalidated::signed::{Envelope, JwkSigner, Signature, Signer};

use crate::transaction_manager::ProofBlock;
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
struct CasAnchorRequestEnvelope {
    payload: CasAuthPayload,
    signatures: Vec<Signature>,
}

fn cid_to_stream_id(cid: Cid) -> StreamId {
    StreamId {
        r#type: StreamIdType::Unloadable,
        cid,
    }
}

async fn auth_header(url: String, controller: String, digest: Cid) -> Result<String> {
    let auth_payload = CasAuthPayload {
        url,
        nonce: Uuid::new_v4().to_string(),
        digest: digest.to_string(),
    };
    let payload = serde_json::to_vec(&auth_payload)?;
    let payload = Base64UrlString::from(payload);
    // Access the private key directory from the environment, and load the private key.
    let node_private_key = std::env::var("CERAMIC_ONE_P2P_KEY_DIR").unwrap();

    let signer = JwkSigner::new(
        DidDocument::new(controller.as_str()),
        node_private_key.as_str(),
    )
    .await
    .unwrap();

    let alg = signer.algorithm();
    let header = ssi::jws::Header {
        algorithm: alg,
        type_: Some("JWT".to_string()),
        key_id: Some(signer.id().id.clone()),
        ..Default::default()
    };
    // creates compact signature of protected.signature
    let header_bytes = serde_json::to_vec(&header)?;
    let header_str = Base64String::from(serde_json::to_vec(&header)?);
    let signing_input = format!("{}.{}", header_str.as_ref(), payload);
    let signed = signer.sign(signing_input.as_bytes())?;

    let envelope = CasAnchorRequestEnvelope {
        payload: auth_payload,
        signatures: vec![Signature {
            header: None,
            protected: Some(header_bytes.into()),
            signature: signed.into(),
        }],
    };

    let (sig, protected) = envelope
        .signatures
        .first()
        .and_then(|sig| sig.protected.as_ref().map(|p| (&sig.signature, p)))
        .unwrap();
    Ok(format!(
        "Bearer {:?}.{:?}.{:?}",
        protected, envelope.payload, sig
    ))
}

pub struct RemoteCas;

impl TransactionManager for RemoteCas {
    async fn make_proof(&self, root: Cid) -> Result<Receipt> {
        let mock_data = b"mock txHash";
        let mock_hash = MultihashDigest::digest(&Code::Sha2_256, mock_data);
        let mock_tx_hash = Cid::new_v1(0x00, mock_hash);
        let mock_proof_block = ProofBlock {
            chain_id: "mock chain id".to_owned(),
            root,
            tx_hash: mock_tx_hash,
            tx_type: "mock tx type".to_owned(),
        };
        let mock_proof: DagCborIpfsBlock = serde_ipld_dagcbor::to_vec(&mock_proof_block)?.into();
        let mock_path = "".to_owned();
        Ok(Receipt {
            proof_cid: mock_proof.cid,
            path_prefix: Some(mock_path),
            blocks: vec![mock_proof],
        })
    }
}

impl RemoteCas {
    pub fn new() -> Self {
        Self
    }
    pub async fn create_anchor_request(
        cas_api_url: String,
        node_controller: String,
        root_cid: Cid,
    ) -> Result<String> {
        let cas_create_request_url = format!("{}/api/v0/requests", cas_api_url);
        // let auth_header = auth_header(cas_api_url.clone(), node_controller, root_cid).await?;
        let cas_request_body = serde_json::to_string(&CasAnchorRequest {
            stream_id: cid_to_stream_id(root_cid),
            cid: root_cid.to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            ceramic_one_version: AGENT_VERSION.to_owned(),
        })?;
        let res = reqwest::Client::new()
            .post(cas_create_request_url)
            // .header("Authorization", auth_header)
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

    #[tokio::test]
    async fn test_create_anchor_request_on_cas() {
        let cas_api_url = "https://cas-dev-direct.3boxlabs.com".to_owned();
        let node_controller = "did:key:z6Mkh3pajt5brscshuDrCCber9nC9Ujpi7EcECveKtJPMEPo".to_owned();
        let mock_data = b"mock root";
        let mock_hash = MultihashDigest::digest(&Code::Sha2_256, mock_data);
        let mock_root_cid = Cid::new_v1(0x55, mock_hash);
        let result =
            RemoteCas::create_anchor_request(cas_api_url, node_controller, mock_root_cid).await;
        expect!["Request is pending."].assert_eq(&result.unwrap());
    }
}
