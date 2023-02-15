use std::io::Cursor;

use cid::multihash::{Code, MultihashDigest};
use cid::Cid;
use libipld::{
    cbor::DagCborCodec,
    json::DagJsonCodec,
    prelude::{Codec, Decode},
    Ipld,
};
use reqwest::{
    multipart::{self, Part},
    Client,
};

const RAW: u64 = 0x55;

pub fn compute_cid(data: &[u8]) -> Cid {
    let hash = Code::Sha2_256.digest(data);
    Cid::new_v1(RAW, hash)
}

pub async fn store(data: &[u8]) -> Cid {
    let cbor_data = DagCborCodec.encode(data).unwrap();
    let form = multipart::Form::new()
        // And a file...
        .part("file", Part::bytes(cbor_data));

    // And finally, send the form
    let client = Client::new();
    let resp = client
        .post("http://localhost:5001/api/v0/dag/put")
        .query(&[("input-codec", "dag-cbor")])
        .multipart(form)
        .send()
        .await
        .unwrap();
    let cid: Ipld =
        Ipld::decode(DagJsonCodec, &mut Cursor::new(resp.bytes().await.unwrap())).unwrap();
    match cid {
        Ipld::Map(m) => match m.get("Cid") {
            Some(Ipld::Link(cid)) => cid.to_owned(),
            _ => panic!("expected CID {:#?}", m),
        },
        _ => panic!("expected map {:#?}", cid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cid::multibase::Base;

    #[test]
    fn test_compute_cid() {
        let cid = compute_cid(b"hello world");
        assert_eq!(
            cid.to_string_of_base(Base::Base32Lower).unwrap(),
            "bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e"
        );
    }
}
