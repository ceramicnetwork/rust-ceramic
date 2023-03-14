use crate::args::UnsignedEvent;
use crate::DidDocument;
use anyhow::Result;
use ceramic_core::{Cid, DagCborEncoded, Jwk, Jws};
use multihash::{Code, MultihashDigest};
use serde::Serialize;

// https://github.com/multiformats/multicodec/blob/master/table.csv
const DAG_CBOR_CODEC: u64 = 0x71;

/// A ceramic event, which is a signed commit
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
        signer: &'a DidDocument,
        jwk: &'a Jwk,
    ) -> Result<Self> {
        // encode our commit with dag cbor, hashing that to create cid
        let linked_block = DagCborEncoded::new(&unsigned)?;
        let cid = Cid::new_v1(DAG_CBOR_CODEC, Code::Sha2_256.digest(linked_block.as_ref()));
        let jws = Jws::new(jwk, signer, &cid)?;
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
    use std::str::FromStr;

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
    async fn should_roundtrip_init_event() {
        let did = DidDocument::new("did:key:blah");
        let model =
            StreamId::from_str("kjzl6kcym7w8y6of44g27v981fuutovbrnlw2ifbf8n26j2t4g5mmm6zc43nx1u")
                .unwrap();
        let args = EventArgs::new_with_parent(&did, &model);
        let evt = args.init().await.unwrap();
        let _: serde_json::Value = serde_ipld_dagcbor::from_slice(evt.encoded.as_ref()).unwrap();
    }

    #[tokio::test]
    async fn should_create_event() {
        let mid =
            StreamId::from_str("kjzl6kcym7w8y7nzgytqayf6aro12zt0mm01n6ydjomyvvklcspx9kr6gpbwd09")
                .unwrap();
        let did = DidDocument::new("did:key:z6MkeqMVHDo67GE1CDMDXGvFK2eG98Ta2c2WB18m7SVXDb6f");
        let did_str = &did.id;
        let data = serde_json::json!({
            "creator": did_str,
            "radius": 1,
            "red": 2,
            "green": 3,
            "blue": 4,
        });
        let args = EventArgs::new_with_parent(&did, &mid);
        let evt = args
            .init_with_data(
                &data,
                "3224d39677c03d4c3d83d6ede051db0f2c1df16f422ed509731dd6592a906d9c",
            )
            .await
            .unwrap();

        let protected = evt.jws.signatures[0].protected.as_ref().unwrap();
        let protected = protected.to_vec().unwrap();
        let protected: serde_json::Value = serde_json::from_slice(protected.as_ref()).unwrap();
        assert!(protected.as_object().unwrap().contains_key("kid"));

        let post_data: serde_json::Value =
            serde_ipld_dagcbor::from_slice(evt.linked_block.as_ref()).unwrap();
        let post_data = post_data.as_object().unwrap().get("data").unwrap();
        assert_eq!(post_data, &data);
    }
}
