mod signed_event;
mod unsigned_event;

use ceramic_core::Signer;
use serde::Serialize;
use signed_event::SignedEvent;
use unsigned_event::UnsignedEvent;

/// Extension trait to pull fields from payloads
pub trait CeramicExt {
    /// Obtain the model of the payload
    fn model(&self) -> anyhow::Result<&[u8]>;
}

/// Extension trait to convert payloads into ceramic compatible events
#[async_trait::async_trait]
pub trait IntoUnsignedCeramicEvent {
    /// Convert this event into a ceramic compatible event
    async fn unsigned(self) -> anyhow::Result<UnsignedEvent>;
}

/// Extension trait to convert payloads into ceramic compatible events
#[async_trait::async_trait]
pub trait IntoSignedCeramicEvent {
    /// Convert this event into a ceramic compatible event
    async fn signed(self, signer: &(impl Signer + Sync)) -> anyhow::Result<SignedEvent>;
}

const MODEL_KEY: &str = "model";

#[async_trait::async_trait]
impl<D> CeramicExt for crate::unvalidated::payload::init::Payload<D> {
    fn model(&self) -> anyhow::Result<&[u8]> {
        let value = self
            .header
            .model()
            .ok_or_else(|| anyhow::anyhow!(format!("{MODEL_KEY} not found")))?;
        Ok(value)
    }
}

#[async_trait::async_trait]
impl IntoUnsignedCeramicEvent for crate::unvalidated::init::Payload<()> {
    async fn unsigned(self) -> anyhow::Result<UnsignedEvent> {
        UnsignedEvent::new(self)
    }
}

#[async_trait::async_trait]
impl<D: Serialize + Send> IntoSignedCeramicEvent for crate::unvalidated::Payload<D> {
    async fn signed(self, signer: &(impl Signer + Sync)) -> anyhow::Result<SignedEvent> {
        SignedEvent::new(self, signer).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::StreamId;

    use crate::unvalidated;
    use ceramic_core::DagCborEncoded;
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
        let evt = unvalidated::init::Payload::new(
            unvalidated::init::Header::new(
                vec![signer.id().id.clone()],
                "model".to_string(),
                Some(model.to_vec()),
                None,
                None,
            ),
            None,
        );
        let evt = evt.unsigned().await.unwrap();
        let data: Ipld = DagCborCodec::decode_from_slice(evt.encoded.as_ref()).unwrap();
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
                Some(mid.to_vec()),
                None,
                None,
            ),
            Some(data.clone()),
        );
        let evt: crate::unvalidated::payload::Payload<_> = evt.into();
        let evt = evt.signed(&signer).await.unwrap();
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
