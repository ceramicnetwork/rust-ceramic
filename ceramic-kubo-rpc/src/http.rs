//! Implements the /dag/* endpoints.
use std::{io::Cursor, net, str::FromStr};

use actix_multipart::Multipart;
use actix_web::{
    error,
    http::{header::ContentType, StatusCode},
    web, App, HttpResponse, HttpServer, Scope,
};
use anyhow::anyhow;
use futures_util::StreamExt;
use iroh_api::{Cid, IpfsPath, Multiaddr, PeerId};
use libipld::{cbor::DagCborCodec, ipld, json::DagJsonCodec, prelude::Encode};
use multiaddr::Protocol;
use serde::Deserialize;
use serde::Serialize;
use tracing_actix_web::TracingLogger;

use crate::{dag, error::Error, swarm, IpfsDep};

#[derive(Clone)]
struct AppState<T>
where
    T: IpfsDep,
{
    api: T,
}

/// Start the Kubo RPC mimic server.
///
/// Block until shutdown.
/// Automatically registers shutdown listeners for interrupt and kill signals.
/// See https://actix.rs/docs/server/#graceful-shutdown
pub async fn serve<T, A>(api: T, addrs: A) -> std::io::Result<()>
where
    T: IpfsDep + Send + Clone + 'static,
    A: net::ToSocketAddrs,
{
    HttpServer::new(move || {
        App::new()
            .wrap(TracingLogger::default())
            .app_data(web::Data::new(AppState { api: api.clone() }))
            .service(
                web::scope("/api/v0")
                    .service(dag_scope::<T>())
                    .service(swarm_scope::<T>()),
            )
    })
    .bind(addrs)?
    .run()
    .await
}

fn dag_scope<T>() -> Scope
where
    T: IpfsDep + 'static,
{
    web::scope("/dag")
        .service(web::resource("/get").route(web::post().to(dag_get::<T>)))
        .service(web::resource("/put").route(web::post().to(dag_put::<T>)))
        .service(web::resource("/resolve").route(web::post().to(resolve::<T>)))
}

const DAG_CBOR: &str = "dag-cbor";
const DAG_JSON: &str = "dag-json";

fn dag_cbor() -> String {
    DAG_CBOR.to_string()
}
// used to provide default to query structs
fn dag_json() -> String {
    DAG_JSON.to_string()
}

#[derive(Debug, Deserialize)]
struct GetQuery {
    arg: String,
    #[serde(rename = "output-codec", default = "dag_json")]
    output_codec: String,
}

#[tracing::instrument(skip(data))]
async fn dag_get<T>(
    data: web::Data<AppState<T>>,
    query: web::Query<GetQuery>,
) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    let cid = Cid::from_str(query.arg.as_str()).map_err(|e| Error::Invalid(e.into()))?;
    match query.output_codec.as_str() {
        DAG_JSON => Ok(HttpResponse::Ok()
            .content_type(ContentType::json())
            .body(dag::get(data.api.clone(), cid, DagJsonCodec).await?)),
        DAG_CBOR => Ok(HttpResponse::Ok()
            .content_type(ContentType::octet_stream())
            .body(dag::get(data.api.clone(), cid, DagCborCodec).await?)),
        _ => Err(Error::Invalid(anyhow!(
            "unsupported output-codec \"{}\"",
            query.output_codec
        ))),
    }
}

#[derive(Debug, Deserialize)]
struct PutQuery {
    #[serde(rename = "store-codec", default = "dag_cbor")]
    store_codec: String,
    #[serde(rename = "input-codec", default = "dag_json")]
    input_codec: String,
}

#[tracing::instrument(skip(data, payload))]
async fn dag_put<T>(
    data: web::Data<AppState<T>>,
    query: web::Query<PutQuery>,
    mut payload: Multipart,
) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    while let Some(item) = payload.next().await {
        let mut field = item.map_err(|e| {
            Error::Internal(Into::<anyhow::Error>::into(e).context("reading multipart field"))
        })?;
        if field.name() == "file" {
            let mut input_bytes: Vec<u8> = Vec::new();
            while let Some(chunk) = field.next().await {
                input_bytes.extend(
                    &chunk
                        .map_err(|e| {
                            Error::Internal(
                                Into::<anyhow::Error>::into(e).context("reading multipart chunk"),
                            )
                        })?
                        .to_vec(),
                )
            }

            let cid = match (query.input_codec.as_str(), query.store_codec.as_str()) {
                (DAG_JSON, DAG_CBOR) => {
                    dag::put(
                        data.api.clone(),
                        DagJsonCodec,
                        DagCborCodec,
                        &mut Cursor::new(input_bytes),
                    )
                    .await?
                }
                _ => {
                    return Err(Error::Invalid(anyhow!(
                        "unsupported input-codec, store-codec combination \"{}\", \"{}\"",
                        query.input_codec,
                        query.store_codec,
                    )));
                }
            };

            let response = ipld!({
                "Cid": cid,
            });

            let mut data = Vec::new();
            response.encode(DagJsonCodec, &mut data).unwrap();
            return Ok(HttpResponse::Ok()
                .content_type(ContentType::json())
                .body(data));
        }
    }
    Err(Error::Invalid(anyhow!("missing multipart field 'file'")))
}

#[derive(Debug, Deserialize)]
struct ResolveQuery {
    arg: String,
}

#[tracing::instrument(skip(data))]
async fn resolve<T>(
    data: web::Data<AppState<T>>,
    query: web::Query<ResolveQuery>,
) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    let path: IpfsPath = query.arg.parse().map_err(Error::Invalid)?;
    let cid = dag::resolve(data.api.clone(), &path).await?;
    let resolved = ipld!({
        "Cid": cid,
        // TODO(nathanielc): What is this? Do we use it?
        "RemPath": "",
    });

    let mut data = Vec::new();
    resolved.encode(DagJsonCodec, &mut data).unwrap();
    Ok(HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(data))
}

fn swarm_scope<T>() -> Scope
where
    T: IpfsDep + 'static,
{
    web::scope("/swarm")
        .service(web::resource("/peers").route(web::post().to(swarm_peers::<T>)))
        .service(web::resource("/connect").route(web::post().to(swarm_connect::<T>)))
}

#[tracing::instrument(skip(data))]
async fn swarm_peers<T>(data: web::Data<AppState<T>>) -> Result<HttpResponse, Error>
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

#[derive(Debug, Deserialize)]
struct ConnectQuery {
    arg: String,
}

#[tracing::instrument(skip(data))]
async fn swarm_connect<T>(
    data: web::Data<AppState<T>>,
    query: web::Query<ConnectQuery>,
) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    let ma = Multiaddr::from_str(query.arg.as_str()).map_err(|e| Error::Invalid(e.into()))?;
    let mh = ma
        .iter()
        .flat_map(|proto| {
            if let Protocol::P2p(mh) = proto {
                vec![mh]
            } else {
                vec![]
            }
        })
        .next()
        .ok_or_else(|| Error::Invalid(anyhow!("multiaddr does not contain p2p peer Id")))?;
    let peer_id =
        PeerId::from_multihash(mh).map_err(|_e| Error::Invalid(anyhow!("invalid peer Id")))?;

    swarm::connect(data.api.clone(), peer_id, vec![ma]).await?;

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

#[derive(Serialize)]
struct ErrorJson<'a> {
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "Code")]
    pub code: i32,
    #[serde(rename = "Type")]
    pub typ: &'a str,
}

impl error::ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        let err = ErrorJson {
            message: self.to_string(),
            code: 0,
            typ: "error",
        };
        let data = serde_json::to_string(&err).unwrap();
        HttpResponse::build(self.status_code())
            .content_type(ContentType::json())
            .body(data)
    }

    fn status_code(&self) -> StatusCode {
        match *self {
            Error::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Invalid(_) => StatusCode::BAD_REQUEST,
            Error::NotFound => StatusCode::NOT_FOUND,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    use actix_multipart_rfc7578::client::multipart;
    use actix_web::{
        body::{self, MessageBody},
        dev::ServiceResponse,
        test, web, App,
    };
    use expect_test::{expect, Expect};
    use iroh_api::Bytes;
    use unimock::MockFn;
    use unimock::{matching, Unimock};

    use crate::IpfsDepMock;

    async fn build_server(
        mock: impl IpfsDep + 'static,
    ) -> impl actix_web::dev::Service<
        actix_http::Request,
        Response = ServiceResponse,
        Error = actix_web::Error,
    > {
        test::init_service(
            App::new()
                .app_data(web::Data::new(AppState { api: mock }))
                .service(dag_scope::<Unimock>())
                .service(swarm_scope::<Unimock>()),
        )
        .await
    }

    async fn assert_body_json<B>(body: B, expect: Expect)
    where
        B: MessageBody,
        <B as MessageBody>::Error: std::fmt::Debug,
    {
        let body_json: serde_json::Value =
            serde_json::from_slice(body::to_bytes(body).await.unwrap().as_ref())
                .expect("response body should be valid json");
        let pretty_json = serde_json::to_string_pretty(&body_json).unwrap();
        expect.assert_eq(&pretty_json);
    }

    async fn assert_body_binary<B>(body: B, expect: Expect)
    where
        B: MessageBody,
        <B as MessageBody>::Error: std::fmt::Debug,
    {
        let bytes = hex::encode(&body::to_bytes(body).await.unwrap());
        expect.assert_eq(&bytes);
    }

    #[actix_web::test]
    async fn test_dag_get_json() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-pb/fixtures/cross-codec/#dagpb_data_some
        let bytes: Bytes = hex::decode("0a050001020304")
            .expect("should be valid hex data")
            .into();
        let mock = Unimock::new(
            IpfsDepMock::get
                .some_call(matching!(_))
                .returns(Ok(Some(bytes))),
        );
        let server = build_server(mock).await;
        let req = test::TestRequest::post()
            .uri("/dag/get?arg=bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom")
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
                  "Data": {
                    "/": {
                      "bytes": "AAECAwQ"
                    }
                  },
                  "Links": []
                }"#]],
        )
        .await;
    }

    #[actix_web::test]
    async fn test_dag_get_cbor() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-pb/fixtures/cross-codec/#dagpb_data_some
        let bytes: Bytes = hex::decode("0a050001020304")
            .expect("should be valid hex data")
            .into();
        let mock = Unimock::new(
            IpfsDepMock::get
                .some_call(matching!(_))
                .returns(Ok(Some(bytes))),
        );
        let server = build_server(mock).await;
        let req = test::TestRequest::post()
            .uri("/dag/get?arg=bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom&output-codec=dag-cbor")
            .to_request();
        let resp = test::call_service(&server, req).await;
        assert!(resp.status().is_success());
        assert_eq!(
            "application/octet-stream",
            resp.headers().get("Content-Type").unwrap()
        );
        assert_body_binary(
            resp.into_body(),
            expect!["a26444617461450001020304654c696e6b7380"],
        )
        .await;
    }

    #[actix_web::test]
    async fn test_dag_put() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-json/fixtures/cross-codec/#array-mixed
        let mock = Unimock::new(
            // Expect call to put with dag-cbor cid
            IpfsDepMock::put
                .next_call(matching!((c, _, _) if *c == Cid::from_str("bafyreidufmzzejc3p7gmh6ivp4fjvca5jfazk57nu6vdkvki4c4vpja724").unwrap()))
                .returns(Ok(())),
        );
        let server = build_server(mock).await;

        let mut form = multipart::Form::default();

        let file_bytes = Cursor::new(
            r#"[6433713753386423,65536,500,2,0,-1,-3,-256,-2784428724,-6433713753386424,{"/":{"bytes":"YTE"}},"Čaues ßvěte!"]"#,
        );
        form.add_reader_file("file", file_bytes, "");

        let ct = form.content_type();
        let body = body::to_bytes(multipart::Body::from(form)).await.unwrap();

        let req = test::TestRequest::post()
            .uri("/dag/put")
            .insert_header(("Content-Type", ct))
            .set_payload(body)
            .to_request();
        let resp = test::call_service(&server, req).await;
        assert!(resp.status().is_success());
        assert_eq!(
            "application/json",
            resp.headers().get("Content-Type").unwrap()
        );
        assert_body_json(
            resp.into_body(),
            // Expect response with dag-cbor cid
            expect![[r#"
                {
                  "Cid": {
                    "/": "bafyreidufmzzejc3p7gmh6ivp4fjvca5jfazk57nu6vdkvki4c4vpja724"
                  }
                }"#]],
        )
        .await;
    }
    #[actix_web::test]
    async fn test_dag_resolve() {
        // Test data uses getting started guide for IPFS:
        // ipfs://QmQPeNsJPyVWPFDVHb77w8G42Fvo15z4bG2X8D2GhfbSXc
        let mock = Unimock::new(
            IpfsDepMock::resolve
                .next_call(matching!((p) if **p == IpfsPath::from_str("QmQPeNsJPyVWPFDVHb77w8G42Fvo15z4bG2X8D2GhfbSXc/ping").unwrap()))
                .returns(Ok(vec![Cid::from_str("QmejvEPop4D7YUadeGqYWmZxHhLc4JBUCzJJHWMzdcMe2y").unwrap()])),
        );
        let server = build_server(mock).await;
        let req = test::TestRequest::post()
            .uri("/dag/resolve?arg=QmQPeNsJPyVWPFDVHb77w8G42Fvo15z4bG2X8D2GhfbSXc/ping")
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
                  "Cid": {
                    "/": "QmejvEPop4D7YUadeGqYWmZxHhLc4JBUCzJJHWMzdcMe2y"
                  },
                  "RemPath": ""
                }"#]],
        )
        .await;
    }

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
