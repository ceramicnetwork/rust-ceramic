pub mod cas;
pub mod eth;

#[cfg(test)]
pub(crate) mod tests {
    use ceramic_event::ssi::did::{DIDMethod, Source};
    use ceramic_event::ssi::jwk::Params;
    use ceramic_event::{ssi, Cid, DidDocument, JwkSigner};

    pub async fn mock_server<Fn, Fut>(func: Fn) -> (wiremock::MockServer, url::Url)
    where
        Fut: std::future::Future<Output = wiremock::MockServer>,
        Fn: FnOnce(wiremock::MockServer) -> Fut,
    {
        let server = wiremock::MockServer::start().await;

        let server = func(server).await;

        let url = url::Url::parse(&server.uri()).unwrap();

        (server, url)
    }

    pub async fn test_signer() -> JwkSigner {
        let key = ssi::jwk::JWK::generate_ed25519().unwrap();
        let private_key = if let Params::OKP(params) = &key.params {
            let pk = params.private_key.as_ref().unwrap();
            hex::encode(pk.0.as_slice())
        } else {
            panic!("Invalid key generated");
        };
        let did = did_method_key::DIDKey.generate(&Source::Key(&key)).unwrap();
        let did = DidDocument::new(&did);
        let signer = JwkSigner::new(did.clone(), &private_key).await.unwrap();
        signer
    }

    pub const CID_STR: &str = "bagcqceraplay4erv6l32qrki522uhiz7rf46xccwniw7ypmvs3cvu2b3oulq";

    pub async fn write_car(root: Cid, data: Vec<u8>) -> Vec<u8> {
        let header: iroh_car::CarHeaderV1 = vec![root].into();
        let mut buffer = Vec::new();
        let mut car = iroh_car::CarWriter::new(iroh_car::CarHeader::V1(header), &mut buffer);
        car.write(root, data).await.unwrap();
        buffer
    }
}
