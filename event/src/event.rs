use crate::args::UnsignedEvent;
use anyhow::Result;
use ceramic_core::{Cid, DagCborEncoded, Jws, Signer};
use multihash::{Code, MultihashDigest};
use serde::Serialize;

// https://github.com/multiformats/multicodec/blob/master/table.csv
const DAG_CBOR_CODEC: u64 = 0x71;

/// A ceramic event
pub struct Event {
    /// Cid of the data for the event
    pub cid: Cid,
    /// The data for the event to be encoded in the block
    pub linked_block: DagCborEncoded,
    /// JWS signature of the event
    pub jws: Jws,
}

impl Event {
    /// Create a new event from an unsigned event, signer, and jwk
    pub async fn new<'a, T: Serialize>(
        unsigned: &'a UnsignedEvent<'a, T>,
        signer: &impl Signer,
    ) -> Result<Self> {
        // encode our event with dag cbor, hashing that to create cid
        let linked_block = DagCborEncoded::new(&unsigned)?;
        let cid = Cid::new_v1(DAG_CBOR_CODEC, Code::Sha2_256.digest(linked_block.as_ref()));
        let jws = Jws::for_cid(signer, &cid).await?;
        Ok(Self {
            cid,
            linked_block,
            jws,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{DidDocument, EventArgs, StreamId};

    use ceramic_core::JwkSigner;
    use expect_test::expect;
    use libipld::{cbor::DagCborCodec, json::DagJsonCodec, prelude::Codec, Ipld};
    use std::str::FromStr;
    use test_log::test;

    fn to_pretty_json(json_data: &[u8]) -> String {
        let json: serde_json::Value = match serde_json::from_slice(json_data) {
            Ok(r) => r,
            Err(_) => {
                panic!(
                    "input data should be valid json: {:?}",
                    String::from_utf8(json_data.to_vec())
                )
            }
        };
        serde_json::to_string_pretty(&json).unwrap()
    }

    async fn signer() -> JwkSigner {
        JwkSigner::new(
            DidDocument::new("did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw"),
            "810d51e02cb63066b7d2d2ec67e05e18c29b938412050bdd3c04d878d8001f3c",
        )
        .await
        .unwrap()
    }

    #[test]
    fn should_roundtrip_json_data() {
        let did_str = "some_did";
        let data = serde_json::json!({
            "creator": did_str,
            "radius": 1,
            "red": 2,
            "green": 3,
            "blue": 4,
        });
        let encoded = DagCborEncoded::new(&data).unwrap();
        let decoded: serde_json::Value = serde_ipld_dagcbor::from_slice(encoded.as_ref()).unwrap();
        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn should_dag_json_init_event() {
        let signer = signer().await;
        let model =
            StreamId::from_str("kjzl6kcym7w8y6of44g27v981fuutovbrnlw2ifbf8n26j2t4g5mmm6zc43nx1u")
                .unwrap();
        let args = EventArgs::new_with_parent(&signer, &model);
        let evt = args.init().unwrap();
        let data: Ipld = DagCborCodec.decode(evt.encoded.as_ref()).unwrap();
        let encoded = DagJsonCodec.encode(&data).unwrap();
        expect![[r#"
            {
              "header": {
                "controllers": [
                  "did:key:z6Mkk3rtfoKDMMG4zyarNGwCQs44GSQ49pcYKQspHJPXSnVw"
                ],
                "model": {
                  "/": {
                    "bytes": "zgEDAYUBEiBICac5WcThoeb40H49X/XNgN0enh/EJNtBhIMsTp36Eg"
                  }
                },
                "sep": "model"
              }
            }"#]]
        .assert_eq(&to_pretty_json(&encoded));
    }

    #[tokio::test]
    async fn should_create_event() {
        let mid =
            StreamId::from_str("kjzl6kcym7w8y7nzgytqayf6aro12zt0mm01n6ydjomyvvklcspx9kr6gpbwd09")
                .unwrap();
        let signer = signer().await;
        let data = serde_json::json!({
            "creator": signer.id().id,
            "radius": 1,
            "red": 2,
            "green": 3,
            "blue": 4,
        });
        let args = EventArgs::new_with_parent(&signer, &mid);
        let evt = args.init_with_data(&data).await.unwrap();

        let protected = evt.jws.signatures[0].protected.as_ref().unwrap();
        let protected = protected.to_vec().unwrap();
        let protected: serde_json::Value = serde_json::from_slice(protected.as_ref()).unwrap();
        assert!(protected.as_object().unwrap().contains_key("kid"));

        let post_data: Ipld = DagCborCodec.decode(evt.linked_block.as_ref()).unwrap();
        let encoded = DagJsonCodec.encode(&post_data).unwrap();
        let post_data: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        let post_data = post_data.as_object().unwrap().get("data").unwrap();
        assert_eq!(post_data, &data);
    }
}
