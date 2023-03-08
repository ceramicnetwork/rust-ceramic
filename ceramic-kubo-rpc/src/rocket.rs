use std::str::FromStr;

use anyhow::anyhow;
use iroh_api::Cid;
use libipld::{cbor::DagCborCodec, json::DagJsonCodec};
use rocket::{
    http::{ContentType, Status},
    launch, post,
    response::Responder,
    routes, Build, FromForm, Response, Rocket, State,
};
use serde::Serialize;

use crate::{dag, error::Error, IrohClient};

#[derive(Clone)]
struct AppState<T>
where
    T: IrohClient,
{
    api: T,
}

#[launch]
fn rocket() -> Rocket<Build> {
    rocket::build().mount("/api/v0/dag", routes![dag_get])
}
#[derive(Debug, FromForm)]
struct GetQuery {
    arg: String,
    #[field(name = "output-codec", default = "dag-json")]
    output_codec: String,
}

//#[tracing::instrument(skip(state))]
#[post("/get?<query..>")]
async fn dag_get<T>(
    state: &State<AppState<T>>,
    query: GetQuery,
) -> Result<(ContentType, Vec<u8>), Error>
where
    T: IrohClient + Send + Sync,
{
    let cid = Cid::from_str(query.arg.as_str()).map_err(|e| Error::Invalid(e.into()))?;
    match query.output_codec.as_str() {
        DAG_JSON => Ok((
            ContentType::JSON,
            dag::get(state.api.clone(), cid, DagJsonCodec).await?,
        )),
        DAG_CBOR => Ok((
            ContentType::Binary,
            dag::get(state.api.clone(), cid, DagCborCodec).await?,
        )),
        _ => {
            return Err(Error::Invalid(anyhow!(
                "unsupported output-codec \"{}\"",
                query.output_codec
            )));
        }
    }
}

// Implement consistent error response
impl<'r, 'o: 'r> Responder<'r, 'o> for Error {
    fn respond_to(self, request: &'r rocket::Request<'_>) -> rocket::response::Result<'o> {
        ///         let string = format!("{}:{}", self.name, self.age);
        ///         Response::build_from(string.respond_to(req)?)
        ///             .raw_header("X-Person-Name", self.name)
        ///             .raw_header("X-Person-Age", self.age.to_string())
        ///             .header(ContentType::new("application", "x-person"))
        ///             .ok()
        #[derive(Serialize)]
        struct ErrorJson<'a> {
            #[serde(rename = "Message")]
            pub message: String,
            #[serde(rename = "Code")]
            pub code: i32,
            #[serde(rename = "Type")]
            pub typ: &'a str,
        }
        let err = ErrorJson {
            message: self.to_string(),
            code: 0,
            typ: "error",
        };
        let data = serde_json::to_string(&err).expect("should serialize to JSON");

        Response::build_from(data.respond_to(request)?)
            .header(ContentType::JSON)
            .status(self.status_code())
            .ok()
    }
}

trait StatusCode {
    fn status_code(&self) -> Status;
}

impl StatusCode for Error {
    fn status_code(&self) -> Status {
        match self {
            Error::NotFound => Status::NotFound,
            Error::Invalid(_) => Status::BadRequest,
            Error::Internal(_) => Status::InternalServerError,
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;
    use crate::{P2pClient, StoreClient};

    use anyhow::Result;
    use async_trait::async_trait;
    use iroh_api::{Bytes, Cid, IpfsPath, Multiaddr, PeerId};
    use rocket::local::blocking::Client;
    use rocket::{http::Status, uri};

    #[derive(Clone)]
    struct TestIrohClient {}

    #[async_trait]
    impl IrohClient for TestIrohClient {
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
    impl StoreClient for TestIrohClient {
        async fn get(&self, cid: Cid) -> Result<Option<Bytes>> {
            todo!()
        }
        async fn put(&self, cid: Cid, blob: Bytes, links: Vec<Cid>) -> Result<()> {
            todo!()
        }
    }
    #[async_trait]
    impl P2pClient for TestIrohClient {
        async fn peers(&self) -> Result<HashMap<PeerId, Vec<Multiaddr>>> {
            todo!()
        }
        async fn connect(&self, peer_id: PeerId, addrs: Vec<Multiaddr>) -> Result<()> {
            todo!()
        }
    }
    #[test]
    fn dag_get() {
        let client = Client::tracked(rocket()).expect("valid rocket instance");
        let response = client.get(uri!("/api/v0/dag/get")).dispatch();
        assert_eq!(response.status(), Status::Ok);
        assert_eq!(response.into_string().unwrap(), "Hello, world!");
    }
}
