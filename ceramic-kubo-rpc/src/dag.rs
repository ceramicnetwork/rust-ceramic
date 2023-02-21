//! Implements the /dag/* endpoints.
use std::{io::Cursor, str::FromStr};

use actix_multipart::Multipart;
use actix_web::{http::header::ContentType, web, HttpResponse, Scope};
use anyhow::anyhow;
use futures_util::StreamExt as _;
use iroh_api::{Cid, IpfsPath};
use libipld::{
    cbor::DagCborCodec,
    ipld,
    json::DagJsonCodec,
    multihash::{Code, MultihashDigest},
    pb::DagPbCodec,
    prelude::{Decode, Encode},
    Ipld,
};
use serde::Deserialize;

use crate::{error::Error, AppState, IrohClient, StoreClient};

const DAG_CBOR: &'static str = "dag-cbor";
const DAG_JSON: &'static str = "dag-json";

// used to provide default to query structs
fn dag_cbor() -> String {
    DAG_CBOR.to_string()
}
// used to provide default to query structs
fn dag_json() -> String {
    DAG_JSON.to_string()
}

pub fn scope<T>() -> Scope
where
    T: IrohClient + 'static,
{
    web::scope("/dag")
        .service(web::resource("/get").route(web::post().to(get::<T>)))
        .service(web::resource("/put").route(web::post().to(put::<T>)))
        .service(web::resource("/resolve").route(web::post().to(resolve::<T>)))
}

#[derive(Debug, Deserialize)]
struct GetQuery {
    arg: String,
    #[serde(rename = "output-codec", default = "dag_json")]
    output_codec: String,
}

#[tracing::instrument(skip(data))]
async fn get<T>(
    data: web::Data<AppState<T>>,
    query: web::Query<GetQuery>,
) -> Result<HttpResponse, Error>
where
    T: IrohClient,
{
    let store = data.api.try_store().map_err(|e| Error::InternalError(e))?;
    let cid = Cid::from_str(query.arg.as_str()).map_err(|e| Error::BadRequest(e.into()))?;
    let bytes = store
        .get(cid)
        .await
        .map_err(|e| Error::InternalError(e))?
        .ok_or(Error::NotFound)?;
    let dag_data = match cid.codec() {
        // dag-pb
        0x70 => Ipld::decode(DagPbCodec, &mut Cursor::new(&bytes))
            .map_err(|e| Error::InternalError(e))?,
        // dag-cbor
        0x71 => Ipld::decode(DagCborCodec, &mut Cursor::new(&bytes))
            .map_err(|e| Error::InternalError(e))?,
        _ => {
            return Err(Error::BadRequest(anyhow!(
                "unsupported codec {}",
                cid.codec()
            )));
        }
    };
    let mut body: Vec<u8> = Vec::with_capacity(bytes.len());
    match query.output_codec.as_str() {
        DAG_JSON => dag_data
            .encode(DagJsonCodec, &mut body)
            .map_err(|e| Error::InternalError(e.into()))?,
        DAG_CBOR => dag_data
            .encode(DagCborCodec, &mut body)
            .map_err(|e| Error::InternalError(e.into()))?,

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
async fn put<T>(
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

            let dag_data: Ipld = match query.input_codec.as_str() {
                DAG_JSON => Ipld::decode(DagJsonCodec, &mut Cursor::new(&input_bytes))
                    .map_err(|e| Error::BadRequest(e.into()))?,
                _ => {
                    return Err(Error::BadRequest(anyhow!(
                        "unsupported input-codec \"{}\"",
                        query.input_codec
                    )));
                }
            };

            let mut blob: Vec<u8> = Vec::with_capacity(input_bytes.len());
            let codec: u64 = match query.store_codec.as_str() {
                DAG_CBOR => {
                    dag_data
                        .encode(DagCborCodec, &mut blob)
                        .map_err(|e| Error::InternalError(e.into()))?;
                    DagCborCodec.into()
                }

                _ => {
                    return Err(Error::BadRequest(anyhow!(
                        "unsupported store-codec \"{}\"",
                        query.store_codec
                    )));
                }
            };

            let store = data.api.try_store().map_err(|e| Error::InternalError(e))?;
            let hash = Code::Sha2_256.digest(&blob);
            let cid = Cid::new_v1(codec, hash);
            store
                .put(cid, blob.into(), vec![])
                .await
                .map_err(|e| Error::InternalError(e))?;
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
    let resolved_path = data
        .api
        .resolve(&path)
        .await
        .map_err(|e| Error::InternalError(e))?;

    let cid = resolved_path
        .iter()
        .last()
        .ok_or(Error::InternalError(anyhow!(
            "resolved path should have at least one element"
        )))?
        .to_owned();
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

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use actix_web::{body::to_bytes, dev::Service, http, test, App, Error};
    use anyhow::Result;
    use async_trait::async_trait;
    use iroh_api::{Bytes, Multiaddr, PeerId};
    use tracing_test::traced_test;

    use crate::{P2pClient, StoreClient};

    use super::*;

    #[derive(Clone)]
    struct TestClient {}

    #[async_trait]
    impl IrohClient for Arc<TestClient> {
        type StoreClient = Self;

        type P2pClient = Self;

        fn try_store(&self) -> anyhow::Result<Self::StoreClient> {
            Ok(self.clone())
        }

        fn try_p2p(&self) -> anyhow::Result<Self::P2pClient> {
            Ok(self.clone())
        }

        async fn resolve(&self, ipfs_path: &IpfsPath) -> Result<Vec<Cid>> {
            todo!()
        }
    }

    #[async_trait]
    impl StoreClient for Arc<TestClient> {
        async fn get(&self, cid: Cid) -> Result<Option<Bytes>> {
            todo!()
        }
    }

    #[async_trait]
    impl P2pClient for Arc<TestClient> {
        async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>> {
            todo!()
        }
    }

    #[actix_web::test]
    #[traced_test]
    async fn test_get() -> Result<(), Error> {
        debug!("test_get");
        // Note by default this is configured with an indexer, but not with http resolvers.
        let api = Arc::new(TestClient {});
        let app = App::new()
            .app_data(web::Data::new(AppState { api }))
            .service(web::scope("").route("get", web::get().to(get::<Arc<TestClient>>)));
        let app = test::init_service(app).await;

        let req = test::TestRequest::get()
            .uri("/get")
            //.param("arg", "QmQPeNsJPyVWPFDVHb77w8G42Fvo15z4bG2X8D2GhfbSXc")
            .to_request();
        let resp = app.call(req).await?;

        assert_eq!(resp.status(), http::StatusCode::OK);

        let response_body = resp.into_body();
        assert_eq!(to_bytes(response_body).await?, r#"{}"#);

        Ok(())
    }
}
