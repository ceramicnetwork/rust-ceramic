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

use crate::{dag, error::Error, swarm, IrohClient};

#[derive(Clone)]
struct AppState<T>
where
    T: IrohClient,
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
    T: IrohClient + Send + Clone + 'static,
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
    T: IrohClient + 'static,
{
    web::scope("/dag")
        .service(web::resource("/get").route(web::post().to(dag_get::<T>)))
        .service(web::resource("/put").route(web::post().to(dag_put::<T>)))
        .service(web::resource("/resolve").route(web::post().to(resolve::<T>)))
}

const DAG_CBOR: &'static str = "dag-cbor";
const DAG_JSON: &'static str = "dag-json";

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
    T: IrohClient,
{
    let cid = Cid::from_str(query.arg.as_str()).map_err(|e| Error::BadRequest(e.into()))?;
    let body = match query.output_codec.as_str() {
        DAG_JSON => dag::get(data.api.clone(), cid, DagJsonCodec).await?,
        DAG_CBOR => dag::get(data.api.clone(), cid, DagCborCodec).await?,
        _ => {
            return Err(Error::BadRequest(anyhow!(
                "unsupported output-codec \"{}\"",
                query.output_codec
            )));
        }
    };

    Ok(HttpResponse::Ok().body(body))
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
    T: IrohClient,
{
    while let Some(item) = payload.next().await {
        let mut field = item.map_err(|e| Error::InternalError(e.into()))?;
        if field.name() == "file" {
            let mut input_bytes: Vec<u8> = Vec::new();
            while let Some(chunk) = field.next().await {
                input_bytes.extend(&chunk.map_err(|e| Error::InternalError(e.into()))?.to_vec())
            }

            match (query.input_codec.as_str(), query.store_codec.as_str()) {
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
                    return Err(Error::BadRequest(anyhow!(
                        "unsupported input-codec, store-codec combination \"{}\", \"{}\"",
                        query.input_codec,
                        query.store_codec,
                    )));
                }
            };

            return Ok(HttpResponse::Ok().into());
        }
    }
    Err(Error::BadRequest(anyhow!("missing multipart field 'file'")))
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
    T: IrohClient,
{
    let path: IpfsPath = query.arg.parse().map_err(|e| Error::BadRequest(e))?;
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

pub fn swarm_scope<T>() -> Scope
where
    T: IrohClient + 'static,
{
    web::scope("/swarm")
        .service(web::resource("/peers").route(web::post().to(peers::<T>)))
        .service(web::resource("/connect").route(web::post().to(connect::<T>)))
}

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

#[tracing::instrument(skip(data))]
async fn peers<T>(data: web::Data<AppState<T>>) -> Result<HttpResponse, Error>
where
    T: IrohClient,
{
    let peers: Vec<Peer> = swarm::peers(data.api.clone())
        .await?
        .into_iter()
        .map(|(k, v)| Peer {
            addr: v
                .iter()
                .nth(0)
                .map(|a| a.to_string())
                .unwrap_or_else(|| "".to_string()),
            direction: 0,
            latency: "".to_string(),
            muxer: "".to_string(),
            peer: k.to_string(),
        })
        .collect();

    let peers = PeersResponse { peers };
    let body = serde_json::to_vec(&peers).map_err(|e| Error::InternalError(e.into()))?;
    Ok(HttpResponse::Ok().body(body))
}

#[derive(Debug, Deserialize)]
struct ConnectQuery {
    arg: String,
}

#[derive(Serialize)]
struct ConnectResponse {
    #[serde(rename = "Strings")]
    strings: Vec<String>,
}

#[tracing::instrument(skip(data))]
async fn connect<T>(
    data: web::Data<AppState<T>>,
    query: web::Query<ConnectQuery>,
) -> Result<HttpResponse, Error>
where
    T: IrohClient,
{
    let ma = Multiaddr::from_str(query.arg.as_str()).map_err(|e| Error::BadRequest(e.into()))?;
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
        .ok_or_else(|| Error::BadRequest(anyhow!("multiaddr does not contain p2p peer Id")))?;
    let peer_id =
        PeerId::from_multihash(mh).map_err(|_e| Error::BadRequest(anyhow!("invalid peer Id")))?;

    swarm::connect(data.api.clone(), peer_id.clone(), vec![ma]).await?;

    let connect_resp = ConnectResponse {
        strings: vec![format!("connect {} success", peer_id.to_string())],
    };
    let body = serde_json::to_vec(&connect_resp).map_err(|e| Error::InternalError(e.into()))?;
    Ok(HttpResponse::Ok().body(body))
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
            .insert_header(ContentType::json())
            .body(data)
    }

    fn status_code(&self) -> StatusCode {
        match *self {
            Error::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::BadRequest(_) => StatusCode::BAD_REQUEST,
            Error::NotFound => StatusCode::NOT_FOUND,
        }
    }
}
