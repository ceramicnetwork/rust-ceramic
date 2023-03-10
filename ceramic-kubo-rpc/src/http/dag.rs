use std::{io::Cursor, str::FromStr};

use actix_multipart::Multipart;
use actix_web::{http::header::ContentType, web, HttpResponse, Scope};
use anyhow::anyhow;
use futures_util::StreamExt;
use iroh_api::IpfsPath;
use libipld::{cbor::DagCborCodec, ipld, json::DagJsonCodec, prelude::Encode};
use serde::Deserialize;

use crate::{dag, error::Error, http::AppState, IpfsDep};
pub fn scope<T>() -> Scope
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
    let ipfs_path = IpfsPath::from_str(query.arg.as_str()).map_err(Error::Invalid)?;
    match query.output_codec.as_str() {
        DAG_JSON => Ok(HttpResponse::Ok()
            .content_type(ContentType::json())
            .body(dag::get(data.api.clone(), &ipfs_path, DagJsonCodec).await?)),
        DAG_CBOR => Ok(HttpResponse::Ok()
            .content_type(ContentType::octet_stream())
            .body(dag::get(data.api.clone(), &ipfs_path, DagCborCodec).await?)),
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

#[cfg(test)]
mod tests {

    use super::*;

    use crate::http::tests::{assert_body_binary, assert_body_json, build_server};

    use actix_multipart_rfc7578::client::multipart;
    use actix_web::{body, test};
    use expect_test::expect;
    use iroh_api::{Bytes, Cid};
    use unimock::MockFn;
    use unimock::{matching, Unimock};

    use crate::IpfsDepMock;

    #[actix_web::test]
    async fn test_dag_get_json() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-pb/fixtures/cross-codec/#dagpb_data_some
        let bytes: Bytes = hex::decode("0a050001020304")
            .expect("should be valid hex data")
            .into();
        let mock = Unimock::new(IpfsDepMock::get.some_call(matching!(_)).returns(Ok((
            Cid::try_from("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(),
            bytes,
        ))));
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
        let mock = Unimock::new(IpfsDepMock::get.some_call(matching!(_)).returns(Ok((
            Cid::try_from("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap(),
            bytes,
        ))));
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
}
