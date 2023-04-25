use std::{collections::HashSet, str::FromStr};

use actix_web::{http::header::ContentType, web, HttpResponse, Scope};
use anyhow::anyhow;
use multiaddr::Protocol;
use serde::{Deserialize, Serialize};

use crate::{error::Error, http::AppState, swarm, IpfsDep, Multiaddr, PeerId};

pub fn scope<T>() -> Scope
where
    T: IpfsDep + 'static,
{
    web::scope("/swarm")
        .service(web::resource("/peers").route(web::post().to(peers::<T>)))
        .service(web::resource("/connect").route(web::post().to(connect::<T>)))
}

#[tracing::instrument(skip(data))]
async fn peers<T>(data: web::Data<AppState<T>>) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    let peers: Vec<Peer> = swarm::peers(data.api.clone())
        .await?
        .into_iter()
        .map(|(k, v)| Peer {
            addr: v
                .get(0)
                .map(|a| a.to_string())
                .unwrap_or_else(|| "".to_string()),
            direction: 0,
            latency: "".to_string(),
            muxer: "".to_string(),
            peer: k.to_string(),
        })
        .collect();

    #[derive(Serialize)]
    struct PeersResponse {
        #[serde(rename = "Peers")]
        peers: Vec<Peer>,
    }

    #[derive(Serialize)]
    struct Peer {
        #[serde(rename = "Addr")]
        addr: String,
        #[serde(rename = "Direction")]
        direction: i32,
        #[serde(rename = "Latency")]
        latency: String,
        #[serde(rename = "Muxer")]
        muxer: String,
        #[serde(rename = "Peer")]
        peer: String,
    }

    let peers = PeersResponse { peers };
    let body = serde_json::to_vec(&peers).map_err(|e| Error::Internal(e.into()))?;
    Ok(HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(body))
}

#[derive(Debug)]
struct ConnectQuery {
    arg: Vec<String>,
}

// Custom deserialize impl for ConnectQuery to support multiple `arg` query parameters
impl<'de> Deserialize<'de> for ConnectQuery {
    fn deserialize<D>(deserializer: D) -> Result<ConnectQuery, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FieldVisitor;

        impl<'de> serde::de::Visitor<'de> for FieldVisitor {
            type Value = ConnectQuery;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("`arg`")
            }

            fn visit_map<V>(self, mut map: V) -> Result<ConnectQuery, V::Error>
            where
                V: serde::de::MapAccess<'de>,
            {
                let mut args: Vec<String> = Vec::default();
                while let Some(key) = map.next_key()? {
                    match key {
                        "arg" => args.push(map.next_value::<String>()?),
                        _ => unreachable!(),
                    }
                }
                Ok(ConnectQuery { arg: args })
            }
        }
        deserializer.deserialize_identifier(FieldVisitor)
    }
}

async fn connect<T>(
    data: web::Data<AppState<T>>,
    query: web::Query<ConnectQuery>,
) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    // Iterate over each arg and parse it as a multiaddr and search for peer ids.
    let iter = query
        .arg
        .iter()
        .map(|addr| -> Result<(Multiaddr, Option<PeerId>), Error> {
            let addr = Multiaddr::from_str(addr).map_err(|e| Error::Invalid(e.into()))?;
            let peer_id = addr
                .iter()
                .flat_map(|proto| match proto {
                    Protocol::P2p(mh) => vec![mh],
                    _ => Vec::new(),
                })
                .next()
                .map(|mh| -> Result<PeerId, Error> {
                    PeerId::from_multihash(mh)
                        .map_err(|_e| Error::Invalid(anyhow!("invalid peer Id")))
                })
                .transpose()?;
            Ok((addr, peer_id))
        });

    let (addrs, peer_ids) =
        itertools::process_results(iter, |iter| iter.unzip::<_, _, Vec<_>, Vec<_>>())?;

    // Check we found exactly one unique peer id.
    let peer_ids: HashSet<PeerId> = peer_ids
        .into_iter()
        .flat_map(|p| if let Some(p) = p { vec![p] } else { Vec::new() })
        .collect();
    let peer_id = match peer_ids.len() {
        0 => {
            return Err(Error::Invalid(anyhow!(
                "no peer id specificed in multiaddrs"
            )))
        }
        1 => peer_ids.into_iter().next().unwrap(),
        _ => return Err(Error::Invalid(anyhow!("found multiple distinct peer ids"))),
    };

    // Connect to the peer for all its addrs
    swarm::connect(data.api.clone(), peer_id, addrs).await?;

    #[derive(Serialize)]
    struct ConnectResponse {
        #[serde(rename = "Strings")]
        strings: Vec<String>,
    }

    let connect_resp = ConnectResponse {
        strings: vec![format!("connect {} success", peer_id)],
    };
    let body = serde_json::to_vec(&connect_resp).map_err(|e| Error::Internal(e.into()))?;
    Ok(HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(body))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    use crate::http::tests::{assert_body_json, build_server};

    use actix_web::test;
    use expect_test::expect;
    use unimock::MockFn;
    use unimock::{matching, Unimock};

    use crate::IpfsDepMock;
    #[actix_web::test]
    async fn test_swarm_connect() {
        let mock = Unimock::new(
            IpfsDepMock::connect
                .next_call(matching!((p,_) if *p == PeerId::from_str("12D3KooWFtPWZ1uHShnbvmxYJGmygUfTVmcb6iSQfiAm4XnmsQ8t").unwrap()))
                .returns(Ok(())),
        );
        let server = build_server(mock).await;
        let req = test::TestRequest::post()
            .uri("/swarm/connect?arg=/ip4/1.1.1.1/tcp/4001/p2p/12D3KooWFtPWZ1uHShnbvmxYJGmygUfTVmcb6iSQfiAm4XnmsQ8t")
            .to_request();
        let resp = test::call_service(&server, req).await;
        assert!(resp.status().is_success());
        assert_eq!(
            "application/json",
            resp.headers().get("Content-Type").unwrap()
        );
        assert_body_json(
            resp.into_body(),
            expect![[r#"
                {
                  "Strings": [
                    "connect 12D3KooWFtPWZ1uHShnbvmxYJGmygUfTVmcb6iSQfiAm4XnmsQ8t success"
                  ]
                }"#]],
        )
        .await;
    }
    #[actix_web::test]
    async fn test_swarm_peers() {
        let mock = Unimock::new(
            IpfsDepMock::peers
                .next_call(matching!(()))
                .returns(Ok(HashMap::from([
                    (
                        PeerId::from_str("12D3KooWRyGSRzzEBpHbHyRkGTgCpXuoRMQgYrqk7tFQzM3AFEWp")
                            .unwrap(),
                        vec![Multiaddr::from_str("/ip4/98.165.227.74/udp/15685/quic").unwrap()],
                    ),
                    (
                        PeerId::from_str("12D3KooWBSyp3QZQBFakvXT2uqT2L5ZmTNnpYNXgyVZq5YB3P7DU")
                            .unwrap(),
                        vec![Multiaddr::from_str("/ip4/95.211.198.178/udp/4001/quic").unwrap()],
                    ),
                ]))),
        );
        let server = build_server(mock).await;
        let req = test::TestRequest::post().uri("/swarm/peers").to_request();
        let resp = test::call_service(&server, req).await;
        assert!(resp.status().is_success());
        assert_eq!(
            "application/json",
            resp.headers().get("Content-Type").unwrap()
        );
        assert_body_json(
            resp.into_body(),
            expect![[r#"
                {
                  "Peers": [
                    {
                      "Addr": "/ip4/95.211.198.178/udp/4001/quic",
                      "Direction": 0,
                      "Latency": "",
                      "Muxer": "",
                      "Peer": "12D3KooWBSyp3QZQBFakvXT2uqT2L5ZmTNnpYNXgyVZq5YB3P7DU"
                    },
                    {
                      "Addr": "/ip4/98.165.227.74/udp/15685/quic",
                      "Direction": 0,
                      "Latency": "",
                      "Muxer": "",
                      "Peer": "12D3KooWRyGSRzzEBpHbHyRkGTgCpXuoRMQgYrqk7tFQzM3AFEWp"
                    }
                  ]
                }"#]],
        )
        .await;
    }
}
