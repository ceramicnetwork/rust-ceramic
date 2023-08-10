use actix_web::{http::header::ContentType, web, HttpResponse, Scope};
use serde::Serialize;

use crate::{error::Error, http::AppState, version, IpfsDep};

pub fn scope<T>() -> Scope
where
    T: IpfsDep + 'static,
{
    web::scope("/version").route("", web::post().to(version::<T>))
}

#[derive(Serialize)]
struct VersionResponse {
    #[serde(rename = "Commit")]
    commit: String,
    #[serde(rename = "System")]
    system: String,
    #[serde(rename = "Version")]
    version: String,
}

#[tracing::instrument(skip(data))]
async fn version<T>(data: web::Data<AppState<T>>) -> Result<HttpResponse, Error>
where
    T: IpfsDep,
{
    let metadata = version::version(data.api.clone()).await?;
    let version = VersionResponse {
        commit: metadata.commit,
        system: format!("{}/{}", metadata.arch, metadata.os),
        version: metadata.version,
    };
    let body = serde_json::to_vec(&version).map_err(|e| Error::Internal(e.into()))?;
    Ok(HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(body))
}

#[cfg(test)]
mod tests {

    use actix_web::test;
    use expect_test::expect;
    use unimock::MockFn;
    use unimock::{matching, Unimock};

    use crate::http::tests::{assert_body_json, build_server};
    use crate::IpfsDepMock;

    #[actix_web::test]
    async fn test_version() {
        let mock = Unimock::new(IpfsDepMock::version.some_call(matching!(_)).returns(Ok(
            ceramic_metadata::Version {
                version: "0.1.0".to_owned(),
                arch: "aarch64".to_owned(),
                os: "macos".to_owned(),
                commit: "284f164".to_owned(),
            },
        )));
        let server = build_server(mock).await;
        let req = test::TestRequest::post().uri("/version").to_request();
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
                  "Commit": "284f164",
                  "System": "aarch64/macos",
                  "Version": "0.1.0"
                }"#]],
        )
        .await;
    }
}
