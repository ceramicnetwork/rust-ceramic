//! Implements the /swarm/* endpoints.
use anyhow::anyhow;
use multiaddr::Protocol;
use std::str::FromStr;

use actix_web::{web, HttpResponse, Scope};
use iroh_api::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};

use crate::{error::Error, AppState, IrohClient, P2pClient};

pub fn scope<T>() -> Scope
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
    let p2p = data.api.try_p2p().map_err(|e| Error::InternalError(e))?;
    let peers: Vec<Peer> = p2p
        .peers()
        .await
        .map_err(|e| Error::InternalError(e))?
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
    let p2p = data.api.try_p2p().map_err(|e| Error::InternalError(e))?;
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
    p2p.connect(peer_id.clone(), vec![ma])
        .await
        .map_err(|e| Error::InternalError(e))?;

    let connect_resp = ConnectResponse {
        strings: vec![format!("connect {} success", peer_id.to_string())],
    };
    let body = serde_json::to_vec(&connect_resp).map_err(|e| Error::InternalError(e.into()))?;
    Ok(HttpResponse::Ok().body(body))
}
