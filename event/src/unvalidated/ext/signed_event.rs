use crate::unvalidated::Payload;
use anyhow::Result;
use ceramic_core::{Cid, DagCborEncoded, Jws, Signer};
use multihash_codetable::{Code, MultihashDigest};
use serde::Serialize;

// https://github.com/multiformats/multicodec/blob/master/table.csv
const DAG_CBOR_CODEC: u64 = 0x71;

/// A ceramic event
/// TODO: What is the relationship between this and signed::Envelope?
pub struct SignedEvent {
    /// Cid of the data for the event
    pub cid: Cid,
    /// The data for the event to be encoded in the block
    pub linked_block: DagCborEncoded,
    /// JWS signature of the event
    pub jws: Jws,
}

impl SignedEvent {
    /// Create a new event from an unsigned event, signer, and jwk
    pub async fn new<T: Serialize>(
        unsigned: Payload<T>,
        signer: &(impl Signer + Sync),
    ) -> Result<Self> {
        // encode our event with dag cbor, hashing that to create cid
        let linked_block = DagCborEncoded::new(&unsigned)?;
        let cid = Cid::new_v1(DAG_CBOR_CODEC, Code::Sha2_256.digest(linked_block.as_ref()));
        let jws = Jws::builder(signer).build_for_cid(&cid).await?;
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

    use crate::StreamId;

    use crate::unvalidated;
    use expect_test::expect;
    use ipld_core::{codec::Codec, ipld::Ipld};
    use serde_ipld_dagcbor::codec::DagCborCodec;
    use serde_ipld_dagjson::codec::DagJsonCodec;
    use std::str::FromStr;
    use test_log::test;

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
        let signer = crate::tests::signer().await;
        let model =
            StreamId::from_str("kjzl6kcym7w8y6of44g27v981fuutovbrnlw2ifbf8n26j2t4g5mmm6zc43nx1u")
                .unwrap();
        let evt: unvalidated::init::Payload<Ipld> = unvalidated::init::Payload::new(
            unvalidated::init::Header::new(
                vec![signer.id().id.clone()],
                "model".to_string(),
                model.to_vec(),
                None,
                None,
            ),
            None,
        );
        let evt = SignedEvent::new(evt.into(), &signer).await.unwrap();
        let data: Ipld = DagCborCodec::decode_from_slice(&evt.linked_block.as_ref()).unwrap();
        let encoded = DagJsonCodec::encode_to_vec(&data).unwrap();
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
        .assert_eq(&crate::tests::to_pretty_json(&encoded));
    }

    #[tokio::test]
    async fn should_create_event() {
        let mid =
            StreamId::from_str("kjzl6kcym7w8y7nzgytqayf6aro12zt0mm01n6ydjomyvvklcspx9kr6gpbwd09")
                .unwrap();
        let signer = crate::tests::signer().await;
        let data = serde_json::json!({
            "creator": signer.id().id,
            "radius": 1,
            "red": 2,
            "green": 3,
            "blue": 4,
        });
        let evt = unvalidated::init::Payload::new(
            unvalidated::init::Header::new(
                vec![signer.id().id.clone()],
                "model".to_string(),
                mid.to_vec(),
                None,
                None,
            ),
            Some(data.clone()),
        );
        let evt = SignedEvent::new(evt.into(), &signer).await.unwrap();
        let protected = evt.jws.signatures[0].protected.as_ref().unwrap();
        let protected = protected.to_vec().unwrap();
        let protected: serde_json::Value = serde_json::from_slice(protected.as_ref()).unwrap();
        assert!(protected.as_object().unwrap().contains_key("kid"));

        let post_data: Ipld = serde_ipld_dagcbor::from_slice(evt.linked_block.as_ref()).unwrap();
        let encoded = serde_ipld_dagjson::to_vec(&post_data).unwrap();
        let post_data: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        let post_data = post_data.as_object().unwrap().get("data").unwrap();
        assert_eq!(post_data, &data);
    }
}
