use cid::Cid;
use serde::Serialize;
use std::collections::TryReserveError;

/// SerializeExt extends [`Serialize`] with methods specific to dag-json and dag-cbor
/// serialization.
pub trait SerializeExt: Serialize {
    /// Serialize to dag-json string
    fn to_json(&self) -> Result<String, serde_ipld_dagjson::EncodeError> {
        let json_bytes = self.to_json_bytes()?;
        Ok(String::from_utf8(json_bytes).expect("serde_ipld_dagjson::to_vec returns valid UTF-8"))
    }

    /// Serialize to dag-json bytes
    fn to_json_bytes(&self) -> Result<Vec<u8>, serde_ipld_dagjson::EncodeError> {
        serde_ipld_dagjson::to_vec(self)
    }

    /// serde serialize to dag-cbor
    fn to_cbor(&self) -> Result<Vec<u8>, serde_ipld_dagcbor::EncodeError<TryReserveError>> {
        serde_ipld_dagcbor::to_vec(self)
    }

    /// CID of serde serialize to dag-cbor
    fn to_cid(&self) -> Result<Cid, serde_ipld_dagcbor::EncodeError<TryReserveError>> {
        let (cid, _) = self.to_dag_cbor_block()?;
        Ok(cid)
    }

    /// serde serialize to dag-cbor and return the CID and bytes
    fn to_dag_cbor_block(
        &self,
    ) -> Result<(Cid, Vec<u8>), serde_ipld_dagcbor::EncodeError<TryReserveError>> {
        let cbor_bytes = self.to_cbor()?;
        let hash = multihash_codetable::MultihashDigest::digest(
            &multihash_codetable::Code::Sha2_256,
            &cbor_bytes,
        );
        Ok((Cid::new_v1(0x71, hash), cbor_bytes))
    }
}

impl<T: Serialize> SerializeExt for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use expect_test::expect;

    #[derive(Serialize)]
    struct TestStruct {
        a: u32,
        b: String,
    }

    #[test]
    fn thing() {
        let test = TestStruct {
            a: 100u32,
            b: "hello".to_string(),
        };
        expect!["bafyreib2ompyhdp4gpbqcfjmbktgyramh3kquc4lwcrgc722fo3r7rq2di"]
            .assert_eq(&test.to_cid().unwrap().to_string());
        expect!["a26161186461626568656c6c6f"].assert_eq(&hex::encode(test.to_cbor().unwrap()));
        expect![[r#"{"a":100,"b":"hello"}"#]].assert_eq(&test.to_json().unwrap());

        let (t_cid, t_cbor) = test.to_dag_cbor_block().unwrap();
        expect!["bafyreib2ompyhdp4gpbqcfjmbktgyramh3kquc4lwcrgc722fo3r7rq2di"]
            .assert_eq(&t_cid.to_string());
        expect!["a26161186461626568656c6c6f"].assert_eq(&hex::encode(t_cbor));
    }
}
