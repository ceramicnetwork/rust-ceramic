use std::str::FromStr;

use actix_web::{http::header::ContentType, web, HttpResponse, Scope};
use anyhow::anyhow;
use iroh_api::IpfsPath;
use serde::{Deserialize, Serialize};

use crate::{error::Error, http::AppState, pin, IpfsDep};
pub fn scope<T>() -> Scope
where
    T: IpfsDep + 'static,
{
    web::scope("/pin")
        .service(web::resource("/add").route(web::post().to(add::<T>)))
        .service(web::resource("/rm").route(web::post().to(remove::<T>)))
        .service(web::resource("/ls").route(web::post().to(list::<T>)))
}
#[derive(Debug, Deserialize)]
struct AddQuery {
    arg: String,
    recursive: Option<bool>,
    progress: Option<bool>,
}

#[tracing::instrument(skip(data))]
async fn add<T>(
    data: web::Data<AppState<T>>,
    query: web::Query<AddQuery>,
) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    if let Some(recursive) = query.recursive {
        if recursive {
            return Err(Error::Invalid(anyhow!(
                "recursive pinning is not supported."
            )));
        }
    }
    if let Some(progress) = query.progress {
        if progress {
            return Err(Error::Invalid(anyhow!("pin progress is not supported.")));
        }
    }
    let ipfs_path = IpfsPath::from_str(query.arg.as_str()).map_err(Error::Invalid)?;
    let cid = pin::add(data.api.clone(), &ipfs_path).await?;

    #[derive(Serialize)]
    struct AddResponse {
        #[serde(rename = "Pins")]
        pins: Vec<String>,
    }
    let pins = AddResponse {
        pins: vec![cid.to_string()],
    };
    let body = serde_json::to_vec(&pins).map_err(|e| Error::Internal(e.into()))?;
    Ok(HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(body))
}

#[derive(Debug, Deserialize)]
struct RemoveQuery {
    arg: String,
}
#[tracing::instrument(skip(data))]
async fn remove<T>(
    data: web::Data<AppState<T>>,
    query: web::Query<RemoveQuery>,
) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    let ipfs_path = IpfsPath::from_str(query.arg.as_str()).map_err(Error::Invalid)?;
    let cid = pin::remove(data.api.clone(), &ipfs_path).await?;

    #[derive(Serialize)]
    struct RemoveResponse {
        #[serde(rename = "Pins")]
        pins: Vec<String>,
    }
    let pins = RemoveResponse {
        pins: vec![cid.to_string()],
    };
    let body = serde_json::to_vec(&pins).map_err(|e| Error::Internal(e.into()))?;
    Ok(HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(body))
}

#[tracing::instrument(skip(_data))]
async fn list<T>(_data: web::Data<AppState<T>>) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    Err(Error::Invalid(anyhow!("listing pins is not supported.")))
}

#[cfg(test)]
mod tests {

    use crate::http::tests::{assert_body_json, build_server};

    use actix_web::test;
    use expect_test::expect;
    use iroh_api::{Bytes, Cid};
    use unimock::MockFn;
    use unimock::{matching, Unimock};

    use crate::IpfsDepMock;

    #[actix_web::test]
    async fn test_add() {
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
            .uri("/pin/add?arg=bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom")
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
                  "Pins": [
                    "bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom"
                  ]
                }"#]],
        )
        .await;
    }
    #[actix_web::test]
    async fn test_remove() {
        // Setup empty mock
        let mock = Unimock::new(());
        let server = build_server(mock).await;
        let req = test::TestRequest::post()
            .uri("/pin/rm?arg=bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom")
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
                  "Pins": [
                    "bafybeibazl2z4vqp2tmwcfag6wirmtpnomxknqcgrauj7m2yisrz3qjbom"
                  ]
                }"#]],
        )
        .await;
    }
}
