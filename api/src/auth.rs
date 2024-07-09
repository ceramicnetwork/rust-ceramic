use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use biscuit_auth::macros::*;
use biscuit_auth::{Biscuit, KeyPair};
use ceramic_core::ssi::did::DIDMethods;
use ceramic_core::ssi::did_resolve::{DIDResolver, ResolutionInputMetadata};
use ceramic_core::{Cid, Jwk};
use ceramic_event::unvalidated::signed::cacao::{Capability, MetadataValue, SignatureMetadata};
use futures::StreamExt;
use iroh_car::CarReader;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::ser::SerializeMap;
use serde::Serialize;
use ssi::jws::verify_bytes_warnable;
use std::collections::HashMap;
use std::io::Cursor;
use std::str::FromStr;

static PREV_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"prev:(.+)"#).unwrap());
static BISCUIT_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"biscuit:(.+)"#).unwrap());

#[derive(Clone, Debug)]
pub enum Operation {
    Read,
    #[allow(dead_code)]
    Write,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read => write!(f, "read"),
            Self::Write => write!(f, "write"),
        }
    }
}

fn authenticate_biscuit(
    biscuit: &Biscuit,
    operation: &Operation,
    resource: &str,
    _allowed_resources: &[String],
) -> Result<(), String> {
    let mut auth = authorizer!(
        r#"
        operation({operation});
        resource({resource});

        is_allowed($user, $res) <-
            user($user),
            resource($res),
            right($user, $res);

        allow if is_allowed($user, $resource);
    "#,
        operation = operation.to_string(),
        resource = resource,
    );
    auth.set_time();
    auth.add_token(biscuit)
        .map_err(|e| format!("Failed to authorize biscuit: {e}"))?;
    auth.authorize()
        .map_err(|e| format!("Failed to authorize: {e}"))?;
    Ok(())
}

struct Capabilities {
    biscuit: Option<Vec<u8>>,
    resources: Vec<String>,
}

struct Authentication {
    root: Cid,
    capabilities: HashMap<Cid, Capabilities>,
}

static DID_METHODS: Lazy<DIDMethods> = Lazy::new(|| {
    let mut m = DIDMethods::default();
    m.insert(Box::new(did_method_key::DIDKey));
    m.insert(Box::new(did_pkh::DIDPKH));
    m
});

#[derive(Debug)]
struct Sorted<'a> {
    header_data: Vec<(&'a str, &'a MetadataValue)>,
    alg: MetadataValue,
    kid: MetadataValue,
}

impl<'a> Sorted<'a> {
    fn new(metadata: &'a SignatureMetadata) -> Self {
        let header_data: Vec<_> = metadata.rest.iter().map(|(k, v)| (k.as_str(), v)).collect();
        Self {
            header_data,
            alg: MetadataValue::String(metadata.alg.clone()),
            kid: MetadataValue::String(metadata.kid.clone()),
        }
    }
}

impl<'a> Serialize for Sorted<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        let mut header_data: Vec<_> = self
            .header_data
            .iter()
            .map(|(k, v)| (*k, *v))
            .chain(vec![("alg", &self.alg), ("kid", &self.kid)])
            .collect();
        header_data.sort_by(|a, b| a.0.cmp(b.0));
        let mut s = serializer.serialize_map(Some(header_data.len()))?;
        for (k, v) in &header_data {
            s.serialize_entry(k, v)?;
        }
        s.end()
    }
}

async fn verify_capability(cacao: &Capability) -> Result<(), String> {
    let did = if let Some((did, _)) = cacao.signature.metadata.kid.split_once('#') {
        did
    } else {
        &cacao.signature.metadata.kid
    };
    let meta = ResolutionInputMetadata::default();
    let (meta, opt_doc, _opt_doc_meta) = DID_METHODS.resolve(did, &meta).await;
    if let Some(s) = meta.error {
        return Err(s);
    }
    let doc = opt_doc.ok_or_else(|| format!("No document found for {did}"))?;
    let jwk = Jwk::new(&doc).await.map_err(|e| e.to_string())?;

    //reconstruct header and payload
    let payload = URL_SAFE_NO_PAD.encode(
        serde_json::to_vec(&cacao.payload)
            .map_err(|e| e.to_string())?
            .as_slice(),
    );
    let header = Sorted::new(&cacao.signature.metadata);
    let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).map_err(|e| e.to_string())?);
    let input = format!("{header}.{payload}");
    let sig = URL_SAFE_NO_PAD
        .decode(cacao.signature.signature.as_bytes())
        .map_err(|_| "Invalid signature")?;
    if let Err(e) = verify_bytes_warnable(
        cacao.signature.r#type.algorithm(),
        input.as_bytes(),
        &jwk,
        &sig,
    ) {
        tracing::warn!(
            "Validation failed: {}\n    Sig={}",
            e,
            cacao.signature.signature
        );
    }

    Ok(())
}

async fn read_authentication(data: &str) -> Result<Authentication, String> {
    let car_data = URL_SAFE_NO_PAD
        .decode(data.as_bytes())
        .map_err(|_| format!("Data was not Base64 URL: {data}"))?;
    let reader = Cursor::new(&car_data);
    let car_reader = CarReader::new(reader)
        .await
        .map_err(|e| format!("Failed to read CAR: {e}"))?;

    let root = *car_reader
        .header()
        .roots()
        .first()
        .ok_or_else(|| "No roots present".to_string())?;
    let mut blocks = Box::pin(car_reader.stream());
    let mut cacaos: HashMap<Cid, Capability> = HashMap::default();
    while let Some(block) = blocks.next().await {
        let (cid, data) = block.map_err(|e| format!("Failed to read block from CAR: {e}"))?;
        match serde_ipld_dagcbor::from_slice::<Capability>(&data) {
            Ok(cacao) => {
                verify_capability(&cacao).await?;
                cacaos.insert(cid, cacao);
            }
            Err(e) => {
                tracing::trace!("Failed to decode data for {cid}: {e}");
            }
        }
    }
    let blocks = cacaos.iter().map(|(cid, sig)| {
        let mut resources = vec![];
        let mut prev = vec![];
        let mut biscuit = None;
        let sig_resources = if let Some(res) = &sig.payload.resources {
            res
        } else {
            &vec![]
        };
        for res in sig_resources {
            if let Some(res) = PREV_REGEX.captures_iter(res).next() {
                let (_, [cid]) = res.extract();
                let cid = Cid::from_str(cid).map_err(|_| format!("Invalid previous CID: {cid}"))?;
                if !cacaos.contains_key(&cid) {
                    //return Err(format!("No signature found for {cid}"));
                    tracing::warn!("No signature found for {cid}");
                }
                prev.push(cid);
            } else if let Some(res) = BISCUIT_REGEX.captures_iter(res).next() {
                let (_, [biscuit_data]) = res.extract();
                let biscuit_data = URL_SAFE_NO_PAD
                    .decode(biscuit_data)
                    .map_err(|_| format!("Invalid biscuit: {biscuit_data}"))?;

                tracing::trace!("Biscuit: {}", String::from_utf8_lossy(&biscuit_data));

                if biscuit.is_none() {
                    biscuit = Some(biscuit_data);
                } else {
                    return Err("Multiple biscuits specified".to_string());
                }
            } else {
                resources.push(res.clone());
            }
        }
        Ok((*cid, Capabilities { resources, biscuit }))
    });
    let blocks: Result<_, String> = blocks.collect();
    Ok(Authentication {
        root,
        capabilities: blocks?,
    })
}

pub async fn authenticate(data: &str, operation: Operation, resource: &str) -> Result<(), String> {
    let auth = read_authentication(data).await?;

    let mut validated_root = false;
    for (cid, cap) in auth.capabilities.iter() {
        if let Some(biscuit) = &cap.biscuit {
            let mut bldr = biscuit_auth::builder::BiscuitBuilder::new();
            bldr.add_code(String::from_utf8_lossy(biscuit))
                .map_err(|e| format!("Invalid code: {e}"))?;
            let kp = KeyPair::new();
            let biscuit = bldr
                .build(&kp)
                .map_err(|e| format!("Invalid biscuit: {e}"))?;
            authenticate_biscuit(&biscuit, &operation, resource, &cap.resources)?;
        }
        if cid == &auth.root {
            validated_root = true;
        }
    }

    if validated_root {
        tracing::debug!("Token validated for {}", auth.root);
        Ok(())
    } else {
        Err("Failed to validate CAR".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ceramic_core::{DidDocument, StreamId};
    use ceramic_event::cid_from_dag_cbor;
    use ceramic_event::unvalidated::signed::cacao::{
        Header, HeaderType, Payload, Signature, SignatureType,
    };
    use ceramic_event::unvalidated::signed::{JwkSigner, Signer};
    use iroh_car::{CarHeader, CarHeaderV1, CarWriter};
    use ssi::jwk::Params;
    use ssi_dids::{DIDMethod, DocumentBuilder, Source};
    use std::time::Duration;

    #[tokio::test]
    async fn should_authenticate_biscuit() {
        let now = chrono::Utc::now();
        let expiry = now + Duration::from_secs(60);
        let mut bldr = biscuit_auth::builder::BiscuitBuilder::new();
        bldr.add_code(&format!(
            r#"
                user("did:pkh:eip155:1:0xfa3F54AE9C4287CA09a486dfaFaCe7d1d4095d93");
                right("did:pkh:eip155:1:0xfa3F54AE9C4287CA09a486dfaFaCe7d1d4095d93", "ceramic://*?model=kjzl6hvfrbw6cadyci5lvsff4jxl1idffrp2ld3i0k1znz0b3k67abkmtf7p7q3");
                check if time($time), $time < {};
            "#,
            expiry.to_rfc3339()
        )).unwrap();
        let kp = KeyPair::new();
        let biscuit = bldr.build(&kp).unwrap();

        authenticate_biscuit(
            &biscuit,
            &Operation::Read,
            "ceramic://*?model=kjzl6hvfrbw6cadyci5lvsff4jxl1idffrp2ld3i0k1znz0b3k67abkmtf7p7q3",
            &vec![],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn should_authenticate() {
        let header = r#"Y6Jlcm9vdHOC2CpYJQABcRIgCrnpZxbxpRk4zP9p18ohYhLqnaBWELv-hNX7vpsEbi3YKlglAAFxEiATGjqMmNzLPBmtmlSsL4czjuBbdgYZHQ6lE4nwK544d2d2ZXJzaW9uAdkEAXESIHgQFit07BJc13FxaE03BZkURgb47hnQc7bIjR7RvV7Xo2FooWF0Z2VpcDQzNjFhcKljYXVkeDlkaWQ6a2V5OnpEbmFlaXp0ZkJqMm5lTmpDMnpTQ0hxbVF0QVBWczE4Y3JOelA0V3pKdENVUFc0UDVjZXhweBgyMDI0LTA3LTE1VDA3OjU5OjM0LjEwNlpjaWF0eBgyMDI0LTA3LTA4VDA3OjU5OjM0LjEwNlpjaXNzeD1kaWQ6cGtoOmVpcDE1NToxMzc6MHg0MzllNjZkYjViODViMDY1Yjk2MmRiMGIzYjIxZTYwNzY3NGMxZDBiZW5vbmNlallXTmNHODNKWjdmZG9tYWluaWxvY2FsaG9zdGd2ZXJzaW9uYTFpcmVzb3VyY2VzgXhRY2VyYW1pYzovLyo_bW9kZWw9a2p6bDZodmZyYnc2Y2FkeWNpNWx2c2ZmNGp4bDFpZGZmcnAybGQzaTBrMXpuejBiM2s2N2Fia210ZjdwN3EzaXN0YXRlbWVudHg8R2l2ZSB0aGlzIGFwcGxpY2F0aW9uIGFjY2VzcyB0byBzb21lIG9mIHlvdXIgZGF0YSBvbiBDZXJhbWljYXOiYXN4hDB4ZjliNjhkNGYzYTBkMDdkYTE5YzZkZmM0MzA5YTQ3YmE1NmQ5MGRlY2QyNTI2ZGE3OGJmZWFjODM2OTExZDMwMTVhZjIwYTFmMDEwYWVmYmEwNmQ2NTdhZDdmMmM5ZGEwY2FhZjJlNmI4NGY4MTE1NDc5NTdkZmRiYjBmMWZmZDMxY2F0ZmVpcDE5MccJAXESIAq56WcW8aUZOMz_adfKIWIS6p2gVhC7_oTV-76bBG4to2FooWF0Z2NhaXAxMjJhcKljYXVkeDtkaWQ6cGtoOmVpcDE1NToxOjB4ZmEzRjU0QUU5QzQyODdDQTA5YTQ4NmRmYUZhQ2U3ZDFkNDA5NWQ5M2NleHB4GDIwMjQtMDgtMTBUMTc6MDY6NTkuOTcyWmNpYXR4GDIwMjQtMDctMDhUMDc6NTk6MzQuMTA2WmNpc3N4OWRpZDprZXk6ekRuYWVpenRmQmoybmVOakMyelNDSHFtUXRBUFZzMThjck56UDRXekp0Q1VQVzRQNWVub25jZWpZV05jRzgzSlo3ZmRvbWFpbmlsb2NhbGhvc3RndmVyc2lvbmExaXJlc291cmNlc4N4UWNlcmFtaWM6Ly8qP21vZGVsPWtqemw2aHZmcmJ3NmNhZHljaTVsdnNmZjRqeGwxaWRmZnJwMmxkM2kwazF6bnowYjNrNjdhYmttdGY3cDdxM3hAcHJldjpiYWZ5cmVpZHljYWxjdzVobWNqb25vNGxybmJndG9ibXpjcmRhbjZob2RoaWhobndpcnVwbmRwazYyNHkBlmJpc2N1aXQ6THk4Z2JtOGdjbTl2ZENCclpYa2dhV1FnYzJWMENuVnpaWElvSW1ScFpEcHdhMmc2Wldsd01UVTFPakU2TUhobVlUTkdOVFJCUlRsRE5ESTROME5CTURsaE5EZzJaR1poUm1GRFpUZGtNV1EwTURrMVpEa3pJaWs3Q25KcFoyaDBLQ0prYVdRNmNHdG9PbVZwY0RFMU5Ub3hPakI0Wm1FelJqVTBRVVU1UXpReU9EZERRVEE1WVRRNE5tUm1ZVVpoUTJVM1pERmtOREE1TldRNU15SXNJQ0pqWlhKaGJXbGpPaTh2S2o5dGIyUmxiRDFyYW5wc05taDJabkppZHpaallXUjVZMmsxYkhaelptWTBhbmhzTVdsa1ptWnljREpzWkROcE1Hc3hlbTU2TUdJemF6WTNZV0pyYlhSbU4zQTNjVE1pS1RzS1kyaGxZMnNnYVdZZ2RHbHRaU2drZEdsdFpTa3NJQ1IwYVcxbElEd2dNakF5TkMwd09DMHhNRlF4Tnpvd05qbzFPVm83Q2dpc3RhdGVtZW50eDxHaXZlIHRoaXMgYXBwbGljYXRpb24gYWNjZXNzIHRvIHNvbWUgb2YgeW91ciBkYXRhIG9uIENlcmFtaWNhc6NhbaNjYWxnZUVTMjU2Y2NhcHhCaXBmczovL2JhZnlyZWlkeWNhbGN3NWhtY2pvbm80bHJuYmd0b2JtemNyZGFuNmhvZGhpaGhud2lydXBuZHBrNjI0Y2tpZHhrZGlkOmtleTp6RG5hZWl6dGZCajJuZU5qQzJ6U0NIcW1RdEFQVnMxOGNyTnpQNFd6SnRDVVBXNFA1I3pEbmFlaXp0ZkJqMm5lTmpDMnpTQ0hxbVF0QVBWczE4Y3JOelA0V3pKdENVUFc0UDVhc3hWa1dsUUoxRDYta2pxWHpZQTZsSE1SN0VoT1paZFM3QTJkUUlIZHFPV0NxZEhEWGc2QTJvekF2RTVLWC1kOFRlMmV5VzNMR016REdzSmMtX2VObDRDNEFhdGNqd3PXBAFxEiARavXbIEU6OeTVxMhWIPI_Kvg_6WZAZBiAECpS6JZ4MqNhaKFhdGdlaXA0MzYxYXCpY2F1ZHg5ZGlkOmtleTp6RG5hZXdHdUtmMjVFNlFEaGgxcUFqSnl2d0dSWG5VSkgyc0hvVnJlS1huUmt0QldmY2V4cHgYMjAyNC0wNy0xOFQxNzowNjo0NC43NjJaY2lhdHgYMjAyNC0wNy0xMVQxNzowNjo0NC43NjJaY2lzc3g7ZGlkOnBraDplaXAxNTU6MToweGZhM2Y1NGFlOWM0Mjg3Y2EwOWE0ODZkZmFmYWNlN2QxZDQwOTVkOTNlbm9uY2VqOWVHNW1FY09XdWZkb21haW5pbG9jYWxob3N0Z3ZlcnNpb25hMWlyZXNvdXJjZXOBeFFjZXJhbWljOi8vKj9tb2RlbD1ranpsNmh2ZnJidzZjYWR5Y2k1bHZzZmY0anhsMWlkZmZycDJsZDNpMGsxem56MGIzazY3YWJrbXRmN3A3cTNpc3RhdGVtZW50eDxHaXZlIHRoaXMgYXBwbGljYXRpb24gYWNjZXNzIHRvIHNvbWUgb2YgeW91ciBkYXRhIG9uIENlcmFtaWNhc6Jhc3iEMHhiNDE0ZTgzYmZmOTFkMjZkZTEwODYxNWQwZDdiNmU1NzhlODJhZTQ4MDQwZDE3N2M2NWExZDdkMjhiOGU0MWU1NmExNzM0MTRlZDNkMWU2Y2Q0YmY4NDRlMzA3MzBlZTU4ODkxZTc3MzcwNjhkMDJiNTgzYTNhYWU5YmM2NDI1ZjFjYXRmZWlwMTkx7gYBcRIgExo6jJjcyzwZrZpUrC-HM47gW3YGGR0OpROJ8CueOHejYWihYXRnY2FpcDEyMmFwqWNhdWR4OWRpZDprZXk6ekRuYWV3R3VLZjI1RTZRRGhoMXFBakp5dndHUlhuVUpIMnNIb1ZyZUtYblJrdEJXZmNleHB4GDIwMjQtMDctMThUMTc6MDY6NDQuNzYyWmNpYXR4GDIwMjQtMDctMTFUMTc6MDY6NDQuNzYyWmNpc3N4OWRpZDprZXk6ekRuYWV3R3VLZjI1RTZRRGhoMXFBakp5dndHUlhuVUpIMnNIb1ZyZUtYblJrdEJXZmVub25jZWo5ZUc1bUVjT1d1ZmRvbWFpbmlsb2NhbGhvc3RndmVyc2lvbmExaXJlc291cmNlc4N4UWNlcmFtaWM6Ly8qP21vZGVsPWtqemw2aHZmcmJ3NmNhZHljaTVsdnNmZjRqeGwxaWRmZnJwMmxkM2kwazF6bnowYjNrNjdhYmttdGY3cDdxM3hAcHJldjpiYWZ5cmVpYXJubDI1d2ljZmhpNDZqdm9lemJsY2I0cjdmbDRkNzJsZ2lic2JyYWFxZmpqb3JmdHlnaXhAcHJldjpiYWZ5cmVpYWt4aHV3b2Z4cnV1bXRydGg3bmhsNHVpbGNjbHZqM2ljd2NjNTc1Ymd2N283andiZG9mdWlzdGF0ZW1lbnR4PEdpdmUgdGhpcyBhcHBsaWNhdGlvbiBhY2Nlc3MgdG8gc29tZSBvZiB5b3VyIGRhdGEgb24gQ2VyYW1pY2Fzo2Fto2NhbGdlRVMyNTZjY2FweEJpcGZzOi8vYmFmeXJlaWFybmwyNXdpY2ZoaTQ2anZvZXpibGNiNHI3Zmw0ZDcybGdpYnNicmFhcWZqam9yZnR5Z2lja2lkeGtkaWQ6a2V5OnpEbmFld0d1S2YyNUU2UURoaDFxQWpKeXZ3R1JYblVKSDJzSG9WcmVLWG5Sa3RCV2YjekRuYWV3R3VLZjI1RTZRRGhoMXFBakp5dndHUlhuVUpIMnNIb1ZyZUtYblJrdEJXZmFzeFZLVzhMd0R4YkQtVHAtdWYzTUh6RXZ2UE96ZF9nWEVfWlpyZmNvd1Jyd1F0ODd4M2NqUlpTamVHbU1CWUVpRWdXYm84VXlQUEotS2lNanBCOGZtQWJxQWF0Y2p3cw"#;
        let resource =
            r#"ceramic://*?model=kjzl6hvfrbw6cadyci5lvsff4jxl1idffrp2ld3i0k1znz0b3k67abkmtf7p7q3"#;

        authenticate(header, Operation::Read, resource)
            .await
            .unwrap();
    }

    fn generate_did_and_private_key() -> (DidDocument, String) {
        let key = ssi::jwk::JWK::generate_ed25519().unwrap();
        let private_key = if let Params::OKP(params) = &key.params {
            let pk = params.private_key.as_ref().unwrap();
            hex::encode(pk.0.as_slice())
        } else {
            panic!("Failed to generate private key");
        };
        let did = did_method_key::DIDKey.generate(&Source::Key(&key)).unwrap();
        let mut builder = DocumentBuilder::default();
        builder.id(did);
        let doc = builder.build().unwrap();
        (doc, private_key)
    }

    async fn create_capability(
        signer: &impl Signer,
        parent: Option<&DidDocument>,
        resources: Vec<String>,
    ) -> Capability {
        let did = signer.id();
        let aud = parent
            .map(|d| d.id.clone())
            .unwrap_or_else(|| did.id.clone());
        let payload = Payload {
            issuer: did.id.clone(),
            audience: aud,
            issued_at: chrono::Utc::now(),
            domain: "ceramic".to_string(),
            version: "1.0.0".to_string(),
            expiration: None,
            statement: None,
            not_before: None,
            request_id: None,
            resources: Some(resources),
            nonce: "a".to_string(),
        };
        let metadata = SignatureMetadata {
            kid: did.id.clone(),
            alg: format!("{:?}", signer.algorithm()),
            rest: HashMap::default(),
        };
        let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&metadata).unwrap());
        let encoded_payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap());
        let signing_input = format!("{header}.{encoded_payload}");
        let signed = signer.sign(signing_input.as_bytes()).unwrap();
        let signature = Signature {
            signature: URL_SAFE_NO_PAD.encode(&signed),
            metadata,
            r#type: SignatureType::JWS,
        };
        Capability {
            payload: payload,
            signature: signature,
            header: Header {
                r#type: HeaderType::CAIP122,
            },
        }
    }

    #[tokio::test]
    async fn should_authenticate_car() {
        // create our root block
        let (owner_did, owner_key) = generate_did_and_private_key();
        let owner_signer = JwkSigner::new(owner_did.clone(), &owner_key).await.unwrap();
        let (delegated_did, delegated_key) = generate_did_and_private_key();
        let delegated_signer = JwkSigner::new(delegated_did, &delegated_key).await.unwrap();

        let stream_id =
            StreamId::from_str("kjzl6hvfrbw6cadyci5lvsff4jxl1idffrp2ld3i0k1znz0b3k67abkmtf7p7q3")
                .unwrap();
        let resource = format!("ceramic://*?model={stream_id}");
        let mut resources = vec![resource.clone()];
        let owner_cap = create_capability(&owner_signer, None, resources.clone()).await;
        let owner_car = serde_ipld_dagcbor::to_vec(&owner_cap).unwrap();
        let owner_cid = cid_from_dag_cbor(&owner_car);

        let biscuit = biscuit!(
            r#"
            user({user});
            right({user}, "model", {stream_id});
            right({user}, {resource});
        "#,
            user = delegated_signer.id().id.clone(),
            stream_id = stream_id.to_string(),
            resource = resource.clone(),
        );
        let biscuit = URL_SAFE_NO_PAD.encode(biscuit.dump_code().as_bytes());
        resources.push(format!("prev:{}", owner_cid));
        resources.push(format!("biscuit:{}", biscuit));
        let delegated_cap = create_capability(&delegated_signer, Some(&owner_did), resources).await;
        let delegated_car = serde_ipld_dagcbor::to_vec(&delegated_cap).unwrap();
        let delegated_cid = cid_from_dag_cbor(&delegated_car);

        let header = CarHeader::V1(CarHeaderV1::from(vec![delegated_cid]));
        let mut buffer = Vec::new();
        let mut writer = CarWriter::new(header, &mut buffer);
        writer.write(owner_cid, owner_car).await.unwrap();
        writer.write(delegated_cid, delegated_car).await.unwrap();
        let car = writer.finish().await.unwrap();

        let bearer = URL_SAFE_NO_PAD.encode(&car);

        authenticate(&bearer, Operation::Read, &resource)
            .await
            .unwrap();
    }
}
