use std::str::FromStr;

use actix_multipart::Multipart;
use actix_web::{http::header::ContentType, web, HttpResponse, Scope};
use anyhow::anyhow;
use dag_jose::DagJoseCodec;
use futures_util::StreamExt;
use iroh_api::Cid;
use libipld::{cbor::DagCborCodec, json::DagJsonCodec};
use serde::{Deserialize, Serialize};

use crate::{
    block,
    error::Error,
    http::{AppState, DAG_CBOR, DAG_JOSE, DAG_JSON},
    IpfsDep,
};
pub fn scope<T>() -> Scope
where
    T: IpfsDep + 'static,
{
    web::scope("/block")
        .service(web::resource("/get").route(web::post().to(get::<T>)))
        .service(web::resource("/put").route(web::post().to(put::<T>)))
        .service(web::resource("/stat").route(web::post().to(stat::<T>)))
}

const RAW: &str = "raw";
const SHA2_256: &str = "sha2-256";

// used to provide default to query structs
fn raw() -> String {
    RAW.to_string()
}
fn sha2_256() -> String {
    SHA2_256.to_string()
}

#[derive(Debug, Deserialize)]
struct GetQuery {
    arg: String,
}

#[tracing::instrument(skip(data))]
async fn get<T>(
    data: web::Data<AppState<T>>,
    query: web::Query<GetQuery>,
) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    let cid = Cid::from_str(query.arg.as_str()).map_err(|e| Error::Invalid(e.into()))?;

    Ok(HttpResponse::Ok()
        // Note the content is not really text/plain, however the Kubo RPC
        // explicitly specifies the text/plain content type.
        .content_type(ContentType(mime::TEXT_PLAIN))
        .body(block::get(data.api.clone(), cid).await?))
}

#[derive(Debug, Deserialize)]
struct PutQuery {
    #[serde(rename = "cid-codec", default = "raw")]
    cid_codec: String,
    #[serde(default = "sha2_256")]
    mhtype: String,
    #[serde(default)]
    pin: bool,
    format: Option<String>,
}

#[tracing::instrument(skip(data, payload))]
async fn put<T>(
    data: web::Data<AppState<T>>,
    query: web::Query<PutQuery>,
    mut payload: Multipart,
) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    if query.pin {
        return Err(Error::Invalid(anyhow!(
            "recursive pinning is not supported."
        )));
    }
    if query.mhtype != SHA2_256 {
        return Err(Error::Invalid(anyhow!(
            "unsupported multihash \"{}\".",
            query.mhtype,
        )));
    }

    while let Some(item) = payload.next().await {
        let mut field = item.map_err(|e| {
            Error::Internal(Into::<anyhow::Error>::into(e).context("reading multipart field"))
        })?;
        if field.name() == "data" {
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
            let codec = if let Some(codec) = query.format.as_ref() {
                codec
            } else {
                &query.cid_codec
            };

            let size = input_bytes.len() as u64;
            let cid = match codec.as_str() {
                DAG_CBOR => block::put(data.api.clone(), DagCborCodec, input_bytes).await?,
                DAG_JSON => block::put(data.api.clone(), DagJsonCodec, input_bytes).await?,
                DAG_JOSE => block::put(data.api.clone(), DagJoseCodec, input_bytes).await?,
                _ => {
                    return Err(Error::Invalid(anyhow!(
                        "unsupported cid-codec \"{}\"",
                        codec,
                    )));
                }
            };

            #[derive(Serialize)]
            struct PutResponse {
                #[serde(rename = "Key")]
                key: String,
                #[serde(rename = "Size")]
                size: u64,
            }
            let put = PutResponse {
                key: cid.to_string(),
                size,
            };
            let body = serde_json::to_vec(&put).map_err(|e| Error::Internal(e.into()))?;
            return Ok(HttpResponse::Ok()
                .content_type(ContentType::json())
                .body(body));
        }
    }
    Err(Error::Invalid(anyhow!("missing multipart field 'file'")))
}

#[derive(Debug, Deserialize)]
struct StatQuery {
    arg: String,
}

#[tracing::instrument(skip(data))]
async fn stat<T>(
    data: web::Data<AppState<T>>,
    query: web::Query<StatQuery>,
) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    let cid = Cid::from_str(query.arg.as_str()).map_err(|e| Error::Invalid(e.into()))?;
    let size = block::stat(data.api.clone(), cid).await?;

    #[derive(Serialize)]
    struct StatResponse {
        #[serde(rename = "Key")]
        key: String,
        #[serde(rename = "Size")]
        size: u64,
    }
    let stat = StatResponse {
        key: cid.to_string(),
        size,
    };
    let body = serde_json::to_vec(&stat).map_err(|e| Error::Internal(e.into()))?;
    return Ok(HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(body));
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;

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
    async fn test_get() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-pb/fixtures/cross-codec/#dagpb_data_some
        let bytes: Bytes = hex::decode("0a050001020304")
            .expect("should be valid hex data")
            .into();
        let mock = Unimock::new(IpfsDepMock::block_get
            .some_call(matching!((cid) if cid == &Cid::try_from("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap()))
            .returns(Ok( bytes)));
        let server = build_server(mock).await;
        let req = test::TestRequest::post()
            .uri("/block/get?arg=bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom")
            .to_request();
        let resp = test::call_service(&server, req).await;
        assert!(resp.status().is_success());
        assert_eq!("text/plain", resp.headers().get("Content-Type").unwrap());
        assert_body_binary(resp.into_body(), expect!["0a050001020304"]).await;
    }

    #[actix_web::test]
    async fn test_put() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-json/fixtures/cross-codec/#array-mixed
        let mock = Unimock::new(
            // Expect call to put with dag-cbor cid
            IpfsDepMock::put
                .next_call(matching!((c, _, _) if *c == Cid::from_str("baguqeera4iuxsgqusw3ctry362niptivjyio6dxnsn5afctijsahacub2eza").unwrap()))
                .returns(Ok(())),
        );
        let server = build_server(mock).await;

        let mut form = multipart::Form::default();

        let file_bytes = Cursor::new(
            r#"[6433713753386423,65536,500,2,0,-1,-3,-256,-2784428724,-6433713753386424,{"/":{"bytes":"YTE"}},"Čaues ßvěte!"]"#,
        );
        form.add_reader_file("data", file_bytes, "");

        let ct = form.content_type();
        let body = body::to_bytes(multipart::Body::from(form)).await.unwrap();

        let req = test::TestRequest::post()
            .uri("/block/put?format=dag-json")
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
            // Expect response with dag-json cid
            expect![[r#"
                {
                  "Key": "baguqeera4iuxsgqusw3ctry362niptivjyio6dxnsn5afctijsahacub2eza",
                  "Size": 113
                }"#]],
        )
        .await;
    }
    #[actix_web::test]
    async fn test_stat() {
        // Test data from:
        // https://ipld.io/specs/codecs/dag-pb/fixtures/cross-codec/#dagpb_data_some
        let mock = Unimock::new(IpfsDepMock::block_size
            .some_call(matching!((cid) if cid == &Cid::try_from("bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom").unwrap()))
            .returns(Ok(7)));
        let server = build_server(mock).await;
        let req = test::TestRequest::post()
            .uri("/block/stat?arg=bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom")
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
              "Key": "bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom",
              "Size": 7
            }"#]],
        )
        .await;
    }
}
