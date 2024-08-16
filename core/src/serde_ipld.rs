use anyhow::Result;
use cid::Cid;
use serde::Serialize;

/// SerdeIpld is a trait for serialization, deserialization, CID calculation assuming dagcbor.
pub trait SerdeIpld: Serialize {
    /// serde serialize to dag-json String.
    fn to_json(&self) -> Result<String> {
        let json_bytes = serde_ipld_dagjson::to_vec(self)?;
        String::from_utf8(json_bytes).map_err(Into::into)
    }

    /// serde serialize to dag-cbor Vec u8
    fn to_cbor(&self) -> Result<Vec<u8>> {
        serde_ipld_dagcbor::to_vec(self).map_err(Into::into)
    }

    /// CID of serde serialize to dag-cbor
    fn to_cid(&self) -> Result<Cid> {
        let (cid, _) = self.to_dag_cbor_block()?;
        Ok(cid)
    }

    /// serde serialize to dag-cbor and return the CID and bytes
    fn to_dag_cbor_block(&self) -> Result<(Cid, Vec<u8>)> {
        let cbor_bytes = self.to_cbor()?;
        let hash = multihash_codetable::MultihashDigest::digest(
            &multihash_codetable::Code::Sha2_256,
            &cbor_bytes,
        );
        Ok((Cid::new_v1(0x71, hash), cbor_bytes))
    }
}

impl<T: Serialize> SerdeIpld for T {}

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
        expect!["a26161186461626568656c6c6f"].assert_eq(&hex::encode(&test.to_cbor().unwrap()));
        expect![[r#"{"a":100,"b":"hello"}"#]].assert_eq(&test.to_json().unwrap());

        let (t_cid, t_cbor) = test.to_dag_cbor_block().unwrap();
        expect!["bafyreib2ompyhdp4gpbqcfjmbktgyramh3kquc4lwcrgc722fo3r7rq2di"]
            .assert_eq(&t_cid.to_string());
        expect!["a26161186461626568656c6c6f"].assert_eq(&hex::encode(t_cbor));
    }
}
