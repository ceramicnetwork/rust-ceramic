use std::str::FromStr;

use actix_web::{http::header::ContentType, web, HttpResponse, Scope};
use anyhow::anyhow;
use iroh_api::PeerId;
use serde::{Deserialize, Serialize};

use crate::{error::Error, http::AppState, id, IpfsDep};
pub fn scope<T>() -> Scope
where
    T: IpfsDep + 'static,
{
    web::scope("/id").route("", web::post().to(id::<T>))
}
#[derive(Debug, Deserialize)]
struct IdQuery {
    arg: Option<String>,
    format: Option<String>,
    #[serde(rename = "peerid-base")]
    peer_id_base: Option<String>,
}

#[tracing::instrument(skip(data))]
async fn id<T>(
    data: web::Data<AppState<T>>,
    query: web::Query<IdQuery>,
) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    if query.format.is_some() {
        return Err(Error::Invalid(anyhow!("format is not supported.")));
    }
    if query.peer_id_base.is_some() {
        return Err(Error::Invalid(anyhow!("peerid-base is not supported.")));
    }
    let info = if let Some(id) = &query.arg {
        let peer_id = PeerId::from_str(id).map_err(|e| Error::Invalid(e.into()))?;
        id::lookup(data.api.clone(), peer_id).await?
    } else {
        id::lookup_local(data.api.clone()).await?
    };
    #[derive(Serialize)]
    struct IdResponse {
        #[serde(rename = "ID")]
        id: String,
        #[serde(rename = "Addresses")]
        addresses: Vec<String>,
        #[serde(rename = "AgentVersion")]
        agent_version: String,
        #[serde(rename = "ProtocolVersion")]
        protocol_version: String,
        #[serde(rename = "Protocols")]
        protocols: Vec<String>,
    }
    let id = IdResponse {
        id: info.peer_id.to_string(),
        addresses: info
            .listen_addrs
            .into_iter()
            .map(|a| a.to_string())
            .collect(),
        agent_version: info.agent_version,
        protocol_version: info.protocol_version,
        protocols: info.protocols,
    };
    let body = serde_json::to_vec(&id).map_err(|e| Error::Internal(e.into()))?;
    Ok(HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(body))
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::http::tests::{assert_body_json, build_server};
    use crate::PeerInfo;

    use actix_web::test;
    use expect_test::expect;
    use unimock::MockFn;
    use unimock::{matching, Unimock};

    use crate::IpfsDepMock;

    #[actix_web::test]
    async fn test_id() {
        let info = PeerInfo {
            peer_id: "12D3KooWQuKj4A11GNZ4MmcAmJzCNGZjArjyRTgkLhSutqeqVypv"
                .parse()
                .unwrap(),
            protocol_version: "ipfs/0.1.0".to_string(),
            agent_version: "iroh/0.2.0".to_string(),
            listen_addrs: vec![
                "/ip4/127.0.0.1/udp/35826/quic-v1".parse().unwrap(),
                "/ip4/192.168.12.189/tcp/43113".parse().unwrap(),
            ],
            protocols: vec![
                "/ipfs/ping/1.0.0".parse().unwrap(),
                "/ipfs/id/1.0.0".parse().unwrap(),
                "/ipfs/id/push/1.0.0".parse().unwrap(),
                "/ipfs/bitswap/1.2.0".parse().unwrap(),
                "/ipfs/bitswap/1.1.0".parse().unwrap(),
                "/ipfs/bitswap/1.0.0".parse().unwrap(),
                "/ipfs/bitswap".parse().unwrap(),
                "/ipfs/kad/1.0.0".parse().unwrap(),
                "/libp2p/autonat/1.0.0".parse().unwrap(),
                "/libp2p/circuit/relay/0.2.0/hop".parse().unwrap(),
                "/libp2p/circuit/relay/0.2.0/stop".parse().unwrap(),
                "/libp2p/dcutr".parse().unwrap(),
                "/meshsub/1.1.0".parse().unwrap(),
                "/meshsub/1.0.0".parse().unwrap(),
            ],
        };
        let mock = Unimock::new(
            IpfsDepMock::lookup
                .some_call(matching!((p) if p == &PeerId::from_str("12D3KooWQuKj4A11GNZ4MmcAmJzCNGZjArjyRTgkLhSutqeqVypv").unwrap()))
                .returns(Ok(info)),
        );
        let server = build_server(mock).await;
        let req = test::TestRequest::post()
            .uri("/id?arg=12D3KooWQuKj4A11GNZ4MmcAmJzCNGZjArjyRTgkLhSutqeqVypv")
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
                  "Addresses": [
                    "/ip4/127.0.0.1/udp/35826/quic-v1",
                    "/ip4/192.168.12.189/tcp/43113"
                  ],
                  "AgentVersion": "iroh/0.2.0",
                  "ID": "12D3KooWQuKj4A11GNZ4MmcAmJzCNGZjArjyRTgkLhSutqeqVypv",
                  "ProtocolVersion": "ipfs/0.1.0",
                  "Protocols": [
                    "/ipfs/ping/1.0.0",
                    "/ipfs/id/1.0.0",
                    "/ipfs/id/push/1.0.0",
                    "/ipfs/bitswap/1.2.0",
                    "/ipfs/bitswap/1.1.0",
                    "/ipfs/bitswap/1.0.0",
                    "/ipfs/bitswap",
                    "/ipfs/kad/1.0.0",
                    "/libp2p/autonat/1.0.0",
                    "/libp2p/circuit/relay/0.2.0/hop",
                    "/libp2p/circuit/relay/0.2.0/stop",
                    "/libp2p/dcutr",
                    "/meshsub/1.1.0",
                    "/meshsub/1.0.0"
                  ]
                }"#]],
        )
        .await;
    }

    #[actix_web::test]
    async fn test_local_id() {
        let info = PeerInfo {
            peer_id: "12D3KooWQuKj4A11GNZ4MmcAmJzCNGZjArjyRTgkLhSutqeqVypv"
                .parse()
                .unwrap(),
            protocol_version: "ipfs/0.1.0".to_string(),
            agent_version: "iroh/0.2.0".to_string(),
            listen_addrs: vec![
                "/ip4/127.0.0.1/udp/35826/quic-v1".parse().unwrap(),
                "/ip4/192.168.12.189/tcp/43113".parse().unwrap(),
            ],
            protocols: vec![
                "/ipfs/ping/1.0.0".parse().unwrap(),
                "/ipfs/id/1.0.0".parse().unwrap(),
                "/ipfs/id/push/1.0.0".parse().unwrap(),
                "/ipfs/bitswap/1.2.0".parse().unwrap(),
                "/ipfs/bitswap/1.1.0".parse().unwrap(),
                "/ipfs/bitswap/1.0.0".parse().unwrap(),
                "/ipfs/bitswap".parse().unwrap(),
                "/ipfs/kad/1.0.0".parse().unwrap(),
                "/libp2p/autonat/1.0.0".parse().unwrap(),
                "/libp2p/circuit/relay/0.2.0/hop".parse().unwrap(),
                "/libp2p/circuit/relay/0.2.0/stop".parse().unwrap(),
                "/libp2p/dcutr".parse().unwrap(),
                "/meshsub/1.1.0".parse().unwrap(),
                "/meshsub/1.0.0".parse().unwrap(),
            ],
        };
        let mock = Unimock::new(
            IpfsDepMock::lookup_local
                .some_call(matching!(_))
                .returns(Ok(info)),
        );
        let server = build_server(mock).await;
        let req = test::TestRequest::post().uri("/id").to_request();
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
                  "Addresses": [
                    "/ip4/127.0.0.1/udp/35826/quic-v1",
                    "/ip4/192.168.12.189/tcp/43113"
                  ],
                  "AgentVersion": "iroh/0.2.0",
                  "ID": "12D3KooWQuKj4A11GNZ4MmcAmJzCNGZjArjyRTgkLhSutqeqVypv",
                  "ProtocolVersion": "ipfs/0.1.0",
                  "Protocols": [
                    "/ipfs/ping/1.0.0",
                    "/ipfs/id/1.0.0",
                    "/ipfs/id/push/1.0.0",
                    "/ipfs/bitswap/1.2.0",
                    "/ipfs/bitswap/1.1.0",
                    "/ipfs/bitswap/1.0.0",
                    "/ipfs/bitswap",
                    "/ipfs/kad/1.0.0",
                    "/libp2p/autonat/1.0.0",
                    "/libp2p/circuit/relay/0.2.0/hop",
                    "/libp2p/circuit/relay/0.2.0/stop",
                    "/libp2p/dcutr",
                    "/meshsub/1.1.0",
                    "/meshsub/1.0.0"
                  ]
                }"#]],
        )
        .await;
    }
}
