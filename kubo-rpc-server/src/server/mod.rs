use futures::{future, future::BoxFuture, future::FutureExt, stream, stream::TryStreamExt, Stream};
use hyper::header::{HeaderName, HeaderValue, CONTENT_TYPE};
use hyper::{Body, HeaderMap, Request, Response, StatusCode};
use log::warn;
use multipart::server::save::SaveResult;
use multipart::server::Multipart;
#[allow(unused_imports)]
use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::future::Future;
use std::marker::PhantomData;
use std::task::{Context, Poll};
pub use swagger::auth::Authorization;
use swagger::auth::Scopes;
use swagger::{ApiError, BodyExt, Has, RequestParser, XSpanIdString};
use url::form_urlencoded;

use crate::header;
#[allow(unused_imports)]
use crate::models;

pub use crate::context;

type ServiceFuture = BoxFuture<'static, Result<Response<Body>, crate::ServiceError>>;

use crate::{
    Api, BlockGetPostResponse, BlockPutPostResponse, BlockStatPostResponse, DagGetPostResponse,
    DagImportPostResponse, DagPutPostResponse, DagResolvePostResponse, IdPostResponse,
    PinAddPostResponse, PinRmPostResponse, PubsubLsPostResponse, PubsubPubPostResponse,
    PubsubSubPostResponse, SwarmConnectPostResponse, SwarmPeersPostResponse, VersionPostResponse,
};

mod paths {
    use lazy_static::lazy_static;

    lazy_static! {
        pub static ref GLOBAL_REGEX_SET: regex::RegexSet = regex::RegexSet::new(vec![
            r"^/api/v0/block/get$",
            r"^/api/v0/block/put$",
            r"^/api/v0/block/stat$",
            r"^/api/v0/dag/get$",
            r"^/api/v0/dag/import$",
            r"^/api/v0/dag/put$",
            r"^/api/v0/dag/resolve$",
            r"^/api/v0/id$",
            r"^/api/v0/pin/add$",
            r"^/api/v0/pin/rm$",
            r"^/api/v0/pubsub/ls$",
            r"^/api/v0/pubsub/pub$",
            r"^/api/v0/pubsub/sub$",
            r"^/api/v0/swarm/connect$",
            r"^/api/v0/swarm/peers$",
            r"^/api/v0/version$"
        ])
        .expect("Unable to create global regex set");
    }
    pub(crate) static ID_BLOCK_GET: usize = 0;
    pub(crate) static ID_BLOCK_PUT: usize = 1;
    pub(crate) static ID_BLOCK_STAT: usize = 2;
    pub(crate) static ID_DAG_GET: usize = 3;
    pub(crate) static ID_DAG_IMPORT: usize = 4;
    pub(crate) static ID_DAG_PUT: usize = 5;
    pub(crate) static ID_DAG_RESOLVE: usize = 6;
    pub(crate) static ID_ID: usize = 7;
    pub(crate) static ID_PIN_ADD: usize = 8;
    pub(crate) static ID_PIN_RM: usize = 9;
    pub(crate) static ID_PUBSUB_LS: usize = 10;
    pub(crate) static ID_PUBSUB_PUB: usize = 11;
    pub(crate) static ID_PUBSUB_SUB: usize = 12;
    pub(crate) static ID_SWARM_CONNECT: usize = 13;
    pub(crate) static ID_SWARM_PEERS: usize = 14;
    pub(crate) static ID_VERSION: usize = 15;
}

pub struct MakeService<T, C>
where
    T: Api<C> + Clone + Send + 'static,
    C: Has<XSpanIdString> + Send + Sync + 'static,
{
    api_impl: T,
    marker: PhantomData<C>,
}

impl<T, C> MakeService<T, C>
where
    T: Api<C> + Clone + Send + 'static,
    C: Has<XSpanIdString> + Send + Sync + 'static,
{
    pub fn new(api_impl: T) -> Self {
        MakeService {
            api_impl,
            marker: PhantomData,
        }
    }
}

impl<T, C, Target> hyper::service::Service<Target> for MakeService<T, C>
where
    T: Api<C> + Clone + Send + 'static,
    C: Has<XSpanIdString> + Send + Sync + 'static,
{
    type Response = Service<T, C>;
    type Error = crate::ServiceError;
    type Future = future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, target: Target) -> Self::Future {
        futures::future::ok(Service::new(self.api_impl.clone()))
    }
}

fn method_not_allowed() -> Result<Response<Body>, crate::ServiceError> {
    Ok(Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .body(Body::empty())
        .expect("Unable to create Method Not Allowed response"))
}

pub struct Service<T, C>
where
    T: Api<C> + Clone + Send + 'static,
    C: Has<XSpanIdString> + Send + Sync + 'static,
{
    api_impl: T,
    marker: PhantomData<C>,
}

impl<T, C> Service<T, C>
where
    T: Api<C> + Clone + Send + 'static,
    C: Has<XSpanIdString> + Send + Sync + 'static,
{
    pub fn new(api_impl: T) -> Self {
        Service {
            api_impl,
            marker: PhantomData,
        }
    }
}

impl<T, C> Clone for Service<T, C>
where
    T: Api<C> + Clone + Send + 'static,
    C: Has<XSpanIdString> + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Service {
            api_impl: self.api_impl.clone(),
            marker: self.marker,
        }
    }
}

impl<T, C> hyper::service::Service<(Request<Body>, C)> for Service<T, C>
where
    T: Api<C> + Clone + Send + Sync + 'static,
    C: Has<XSpanIdString> + Send + Sync + 'static,
{
    type Response = Response<Body>;
    type Error = crate::ServiceError;
    type Future = ServiceFuture;

    fn poll_ready(&mut self, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.api_impl.poll_ready(cx)
    }

    fn call(&mut self, req: (Request<Body>, C)) -> Self::Future {
        async fn run<T, C>(
            mut api_impl: T,
            req: (Request<Body>, C),
        ) -> Result<Response<Body>, crate::ServiceError>
        where
            T: Api<C> + Clone + Send + 'static,
            C: Has<XSpanIdString> + Send + Sync + 'static,
        {
            let (request, context) = req;
            let (parts, body) = request.into_parts();
            let (method, uri, headers) = (parts.method, parts.uri, parts.headers);
            let path = paths::GLOBAL_REGEX_SET.matches(uri.path());

            match method {
                // BlockGetPost - POST /block/get
                hyper::Method::POST if path.matched(paths::ID_BLOCK_GET) => {
                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_arg = query_params
                        .iter()
                        .filter(|e| e.0 == "arg")
                        .map(|e| e.1.clone())
                        .next();
                    let param_arg = match param_arg {
                        Some(param_arg) => {
                            let param_arg = <String as std::str::FromStr>::from_str(&param_arg);
                            match param_arg {
                            Ok(param_arg) => Some(param_arg),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter arg - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter arg")),
                        }
                        }
                        None => None,
                    };
                    let param_arg = match param_arg {
                    Some(param_arg) => param_arg,
                    None => return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Missing required query parameter arg"))
                        .expect("Unable to create Bad Request response for missing query parameter arg")),
                };
                    let param_timeout = query_params
                        .iter()
                        .filter(|e| e.0 == "timeout")
                        .map(|e| e.1.clone())
                        .next();
                    let param_timeout = match param_timeout {
                        Some(param_timeout) => {
                            let param_timeout =
                                <String as std::str::FromStr>::from_str(&param_timeout);
                            match param_timeout {
                            Ok(param_timeout) => Some(param_timeout),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter timeout - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter timeout")),
                        }
                        }
                        None => None,
                    };
                    let param_offline = query_params
                        .iter()
                        .filter(|e| e.0 == "offline")
                        .map(|e| e.1.clone())
                        .next();
                    let param_offline = match param_offline {
                        Some(param_offline) => {
                            let param_offline =
                                <bool as std::str::FromStr>::from_str(&param_offline);
                            match param_offline {
                            Ok(param_offline) => Some(param_offline),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter offline - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter offline")),
                        }
                        }
                        None => None,
                    };

                    let result = api_impl
                        .block_get_post(param_arg, param_timeout, param_offline, &context)
                        .await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            BlockGetPostResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("text/plain")
                                                            .expect("Unable to create Content-Type header for BLOCK_GET_POST_SUCCESS"));
                                let body = body.0;
                                *response.body_mut() = Body::from(body);
                            }
                            BlockGetPostResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for BLOCK_GET_POST_BAD_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            BlockGetPostResponse::InternalError(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 500 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for BLOCK_GET_POST_INTERNAL_ERROR"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // BlockPutPost - POST /block/put
                hyper::Method::POST if path.matched(paths::ID_BLOCK_PUT) => {
                    let boundary =
                        match swagger::multipart::form::boundary(&headers) {
                            Some(boundary) => boundary.to_string(),
                            None => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from("Couldn't find valid multipart body".to_string()))
                                .expect(
                                    "Unable to create Bad Request response for incorrect boundary",
                                )),
                        };

                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_cid_codec = query_params
                        .iter()
                        .filter(|e| e.0 == "cid-codec")
                        .map(|e| e.1.clone())
                        .next();
                    let param_cid_codec = match param_cid_codec {
                        Some(param_cid_codec) => {
                            let param_cid_codec =
                                <models::Codecs as std::str::FromStr>::from_str(&param_cid_codec);
                            match param_cid_codec {
                            Ok(param_cid_codec) => Some(param_cid_codec),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter cid-codec - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter cid-codec")),
                        }
                        }
                        None => None,
                    };
                    let param_mhtype = query_params
                        .iter()
                        .filter(|e| e.0 == "mhtype")
                        .map(|e| e.1.clone())
                        .next();
                    let param_mhtype = match param_mhtype {
                        Some(param_mhtype) => {
                            let param_mhtype =
                                <models::Multihash as std::str::FromStr>::from_str(&param_mhtype);
                            match param_mhtype {
                            Ok(param_mhtype) => Some(param_mhtype),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter mhtype - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter mhtype")),
                        }
                        }
                        None => None,
                    };
                    let param_pin = query_params
                        .iter()
                        .filter(|e| e.0 == "pin")
                        .map(|e| e.1.clone())
                        .next();
                    let param_pin = match param_pin {
                        Some(param_pin) => {
                            let param_pin = <bool as std::str::FromStr>::from_str(&param_pin);
                            match param_pin {
                            Ok(param_pin) => Some(param_pin),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter pin - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter pin")),
                        }
                        }
                        None => None,
                    };

                    // Form Body parameters (note that non-required body parameters will ignore garbage
                    // values, rather than causing a 400 response). Produce warning header and logs for
                    // any unused fields.
                    let result = body.into_raw();
                    match result.await {
                            Ok(body) => {
                                use std::io::Read;

                                // Read Form Parameters from body
                                let mut entries = match Multipart::with_body(&body.to_vec()[..], boundary).save().temp() {
                                    SaveResult::Full(entries) => {
                                        entries
                                    },
                                    _ => {
                                        return Ok(Response::builder()
                                                        .status(StatusCode::BAD_REQUEST)
                                                        .body(Body::from("Unable to process all message parts".to_string()))
                                                        .expect("Unable to create Bad Request response due to failure to process all message"))
                                    },
                                };
                                let field_file = entries.fields.remove("file");
                                let param_file = match field_file {
                                    Some(field) => {
                                        let mut reader = field[0].data.readable().expect("Unable to read field for file");
                                        let mut data = vec![];
                                        reader.read_to_end(&mut data).expect("Reading saved binary data should never fail");
                                        swagger::ByteArray(data)
                                    },
                                    None => {
                                        return Ok(
                                            Response::builder()
                                            .status(StatusCode::BAD_REQUEST)
                                            .body(Body::from("Missing required form parameter file".to_string()))
                                            .expect("Unable to create Bad Request due to missing required form parameter file"))
                                    }
                                };
                                let result = api_impl.block_put_post(
                                            param_file,
                                            param_cid_codec,
                                            param_mhtype,
                                            param_pin,
                                        &context
                                    ).await;
                                let mut response = Response::new(Body::empty());
                                response.headers_mut().insert(
                                            HeaderName::from_static("x-span-id"),
                                            HeaderValue::from_str((&context as &dyn Has<XSpanIdString>).get().0.clone().as_str())
                                                .expect("Unable to create X-Span-ID header value"));

                                        match result {
                                            Ok(rsp) => match rsp {
                                                BlockPutPostResponse::Success
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(200).expect("Unable to turn 200 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for BLOCK_PUT_POST_SUCCESS"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                BlockPutPostResponse::BadRequest
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(400).expect("Unable to turn 400 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for BLOCK_PUT_POST_BAD_REQUEST"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                            },
                                            Err(_) => {
                                                // Application code returned an error. This should not happen, as the implementation should
                                                // return a valid response.
                                                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                                                *response.body_mut() = Body::from("An internal error occurred");
                                            },
                                        }

                                        Ok(response)
                            },
                            Err(e) => Ok(Response::builder()
                                                .status(StatusCode::BAD_REQUEST)
                                                .body(Body::from("Couldn't read multipart body".to_string()))
                                                .expect("Unable to create Bad Request response due to unable read multipart body")),
                        }
                }

                // BlockStatPost - POST /block/stat
                hyper::Method::POST if path.matched(paths::ID_BLOCK_STAT) => {
                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_arg = query_params
                        .iter()
                        .filter(|e| e.0 == "arg")
                        .map(|e| e.1.clone())
                        .next();
                    let param_arg = match param_arg {
                        Some(param_arg) => {
                            let param_arg = <String as std::str::FromStr>::from_str(&param_arg);
                            match param_arg {
                            Ok(param_arg) => Some(param_arg),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter arg - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter arg")),
                        }
                        }
                        None => None,
                    };
                    let param_arg = match param_arg {
                    Some(param_arg) => param_arg,
                    None => return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Missing required query parameter arg"))
                        .expect("Unable to create Bad Request response for missing query parameter arg")),
                };

                    let result = api_impl.block_stat_post(param_arg, &context).await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            BlockStatPostResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for BLOCK_STAT_POST_SUCCESS"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            BlockStatPostResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for BLOCK_STAT_POST_BAD_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // DagGetPost - POST /dag/get
                hyper::Method::POST if path.matched(paths::ID_DAG_GET) => {
                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_arg = query_params
                        .iter()
                        .filter(|e| e.0 == "arg")
                        .map(|e| e.1.clone())
                        .next();
                    let param_arg = match param_arg {
                        Some(param_arg) => {
                            let param_arg = <String as std::str::FromStr>::from_str(&param_arg);
                            match param_arg {
                            Ok(param_arg) => Some(param_arg),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter arg - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter arg")),
                        }
                        }
                        None => None,
                    };
                    let param_arg = match param_arg {
                    Some(param_arg) => param_arg,
                    None => return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Missing required query parameter arg"))
                        .expect("Unable to create Bad Request response for missing query parameter arg")),
                };
                    let param_output_codec = query_params
                        .iter()
                        .filter(|e| e.0 == "output-codec")
                        .map(|e| e.1.clone())
                        .next();
                    let param_output_codec = match param_output_codec {
                        Some(param_output_codec) => {
                            let param_output_codec =
                                <models::Codecs as std::str::FromStr>::from_str(
                                    &param_output_codec,
                                );
                            match param_output_codec {
                            Ok(param_output_codec) => Some(param_output_codec),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter output-codec - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter output-codec")),
                        }
                        }
                        None => None,
                    };

                    let result = api_impl
                        .dag_get_post(param_arg, param_output_codec, &context)
                        .await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            DagGetPostResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/octet-stream")
                                                            .expect("Unable to create Content-Type header for DAG_GET_POST_SUCCESS"));
                                let body = body.0;
                                *response.body_mut() = Body::from(body);
                            }
                            DagGetPostResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DAG_GET_POST_BAD_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // DagImportPost - POST /dag/import
                hyper::Method::POST if path.matched(paths::ID_DAG_IMPORT) => {
                    let boundary =
                        match swagger::multipart::form::boundary(&headers) {
                            Some(boundary) => boundary.to_string(),
                            None => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from("Couldn't find valid multipart body".to_string()))
                                .expect(
                                    "Unable to create Bad Request response for incorrect boundary",
                                )),
                        };

                    // Form Body parameters (note that non-required body parameters will ignore garbage
                    // values, rather than causing a 400 response). Produce warning header and logs for
                    // any unused fields.
                    let result = body.into_raw();
                    match result.await {
                            Ok(body) => {
                                use std::io::Read;

                                // Read Form Parameters from body
                                let mut entries = match Multipart::with_body(&body.to_vec()[..], boundary).save().temp() {
                                    SaveResult::Full(entries) => {
                                        entries
                                    },
                                    _ => {
                                        return Ok(Response::builder()
                                                        .status(StatusCode::BAD_REQUEST)
                                                        .body(Body::from("Unable to process all message parts".to_string()))
                                                        .expect("Unable to create Bad Request response due to failure to process all message"))
                                    },
                                };
                                let field_file = entries.fields.remove("file");
                                let param_file = match field_file {
                                    Some(field) => {
                                        let mut reader = field[0].data.readable().expect("Unable to read field for file");
                                        let mut data = vec![];
                                        reader.read_to_end(&mut data).expect("Reading saved binary data should never fail");
                                        swagger::ByteArray(data)
                                    },
                                    None => {
                                        return Ok(
                                            Response::builder()
                                            .status(StatusCode::BAD_REQUEST)
                                            .body(Body::from("Missing required form parameter file".to_string()))
                                            .expect("Unable to create Bad Request due to missing required form parameter file"))
                                    }
                                };
                                let result = api_impl.dag_import_post(
                                            param_file,
                                        &context
                                    ).await;
                                let mut response = Response::new(Body::empty());
                                response.headers_mut().insert(
                                            HeaderName::from_static("x-span-id"),
                                            HeaderValue::from_str((&context as &dyn Has<XSpanIdString>).get().0.clone().as_str())
                                                .expect("Unable to create X-Span-ID header value"));

                                        match result {
                                            Ok(rsp) => match rsp {
                                                DagImportPostResponse::Success
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(200).expect("Unable to turn 200 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DAG_IMPORT_POST_SUCCESS"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                DagImportPostResponse::BadRequest
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(400).expect("Unable to turn 400 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DAG_IMPORT_POST_BAD_REQUEST"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                            },
                                            Err(_) => {
                                                // Application code returned an error. This should not happen, as the implementation should
                                                // return a valid response.
                                                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                                                *response.body_mut() = Body::from("An internal error occurred");
                                            },
                                        }

                                        Ok(response)
                            },
                            Err(e) => Ok(Response::builder()
                                                .status(StatusCode::BAD_REQUEST)
                                                .body(Body::from("Couldn't read multipart body".to_string()))
                                                .expect("Unable to create Bad Request response due to unable read multipart body")),
                        }
                }

                // DagPutPost - POST /dag/put
                hyper::Method::POST if path.matched(paths::ID_DAG_PUT) => {
                    let boundary =
                        match swagger::multipart::form::boundary(&headers) {
                            Some(boundary) => boundary.to_string(),
                            None => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from("Couldn't find valid multipart body".to_string()))
                                .expect(
                                    "Unable to create Bad Request response for incorrect boundary",
                                )),
                        };

                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_store_codec = query_params
                        .iter()
                        .filter(|e| e.0 == "store-codec")
                        .map(|e| e.1.clone())
                        .next();
                    let param_store_codec = match param_store_codec {
                        Some(param_store_codec) => {
                            let param_store_codec =
                                <models::Codecs as std::str::FromStr>::from_str(&param_store_codec);
                            match param_store_codec {
                            Ok(param_store_codec) => Some(param_store_codec),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter store-codec - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter store-codec")),
                        }
                        }
                        None => None,
                    };
                    let param_input_codec = query_params
                        .iter()
                        .filter(|e| e.0 == "input-codec")
                        .map(|e| e.1.clone())
                        .next();
                    let param_input_codec = match param_input_codec {
                        Some(param_input_codec) => {
                            let param_input_codec =
                                <models::Codecs as std::str::FromStr>::from_str(&param_input_codec);
                            match param_input_codec {
                            Ok(param_input_codec) => Some(param_input_codec),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter input-codec - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter input-codec")),
                        }
                        }
                        None => None,
                    };

                    // Form Body parameters (note that non-required body parameters will ignore garbage
                    // values, rather than causing a 400 response). Produce warning header and logs for
                    // any unused fields.
                    let result = body.into_raw();
                    match result.await {
                            Ok(body) => {
                                use std::io::Read;

                                // Read Form Parameters from body
                                let mut entries = match Multipart::with_body(&body.to_vec()[..], boundary).save().temp() {
                                    SaveResult::Full(entries) => {
                                        entries
                                    },
                                    _ => {
                                        return Ok(Response::builder()
                                                        .status(StatusCode::BAD_REQUEST)
                                                        .body(Body::from("Unable to process all message parts".to_string()))
                                                        .expect("Unable to create Bad Request response due to failure to process all message"))
                                    },
                                };
                                let field_file = entries.fields.remove("file");
                                let param_file = match field_file {
                                    Some(field) => {
                                        let mut reader = field[0].data.readable().expect("Unable to read field for file");
                                        let mut data = vec![];
                                        reader.read_to_end(&mut data).expect("Reading saved binary data should never fail");
                                        swagger::ByteArray(data)
                                    },
                                    None => {
                                        return Ok(
                                            Response::builder()
                                            .status(StatusCode::BAD_REQUEST)
                                            .body(Body::from("Missing required form parameter file".to_string()))
                                            .expect("Unable to create Bad Request due to missing required form parameter file"))
                                    }
                                };
                                let result = api_impl.dag_put_post(
                                            param_file,
                                            param_store_codec,
                                            param_input_codec,
                                        &context
                                    ).await;
                                let mut response = Response::new(Body::empty());
                                response.headers_mut().insert(
                                            HeaderName::from_static("x-span-id"),
                                            HeaderValue::from_str((&context as &dyn Has<XSpanIdString>).get().0.clone().as_str())
                                                .expect("Unable to create X-Span-ID header value"));

                                        match result {
                                            Ok(rsp) => match rsp {
                                                DagPutPostResponse::Success
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(200).expect("Unable to turn 200 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DAG_PUT_POST_SUCCESS"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                                DagPutPostResponse::BadRequest
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(400).expect("Unable to turn 400 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DAG_PUT_POST_BAD_REQUEST"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                            },
                                            Err(_) => {
                                                // Application code returned an error. This should not happen, as the implementation should
                                                // return a valid response.
                                                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                                                *response.body_mut() = Body::from("An internal error occurred");
                                            },
                                        }

                                        Ok(response)
                            },
                            Err(e) => Ok(Response::builder()
                                                .status(StatusCode::BAD_REQUEST)
                                                .body(Body::from("Couldn't read multipart body".to_string()))
                                                .expect("Unable to create Bad Request response due to unable read multipart body")),
                        }
                }

                // DagResolvePost - POST /dag/resolve
                hyper::Method::POST if path.matched(paths::ID_DAG_RESOLVE) => {
                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_arg = query_params
                        .iter()
                        .filter(|e| e.0 == "arg")
                        .map(|e| e.1.clone())
                        .next();
                    let param_arg = match param_arg {
                        Some(param_arg) => {
                            let param_arg = <String as std::str::FromStr>::from_str(&param_arg);
                            match param_arg {
                            Ok(param_arg) => Some(param_arg),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter arg - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter arg")),
                        }
                        }
                        None => None,
                    };
                    let param_arg = match param_arg {
                    Some(param_arg) => param_arg,
                    None => return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Missing required query parameter arg"))
                        .expect("Unable to create Bad Request response for missing query parameter arg")),
                };

                    let result = api_impl.dag_resolve_post(param_arg, &context).await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            DagResolvePostResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DAG_RESOLVE_POST_SUCCESS"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            DagResolvePostResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DAG_RESOLVE_POST_BAD_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // IdPost - POST /id
                hyper::Method::POST if path.matched(paths::ID_ID) => {
                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_arg = query_params
                        .iter()
                        .filter(|e| e.0 == "arg")
                        .map(|e| e.1.clone())
                        .next();
                    let param_arg = match param_arg {
                        Some(param_arg) => {
                            let param_arg = <String as std::str::FromStr>::from_str(&param_arg);
                            match param_arg {
                            Ok(param_arg) => Some(param_arg),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter arg - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter arg")),
                        }
                        }
                        None => None,
                    };

                    let result = api_impl.id_post(param_arg, &context).await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => {
                            match rsp {
                                IdPostResponse::Success(body) => {
                                    *response.status_mut() = StatusCode::from_u16(200)
                                        .expect("Unable to turn 200 into a StatusCode");
                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for ID_POST_SUCCESS"));
                                    let body = serde_json::to_string(&body)
                                        .expect("impossible to fail to serialize");
                                    *response.body_mut() = Body::from(body);
                                }
                                IdPostResponse::BadRequest(body) => {
                                    *response.status_mut() = StatusCode::from_u16(400)
                                        .expect("Unable to turn 400 into a StatusCode");
                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for ID_POST_BAD_REQUEST"));
                                    let body = serde_json::to_string(&body)
                                        .expect("impossible to fail to serialize");
                                    *response.body_mut() = Body::from(body);
                                }
                            }
                        }
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // PinAddPost - POST /pin/add
                hyper::Method::POST if path.matched(paths::ID_PIN_ADD) => {
                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_arg = query_params
                        .iter()
                        .filter(|e| e.0 == "arg")
                        .map(|e| e.1.clone())
                        .next();
                    let param_arg = match param_arg {
                        Some(param_arg) => {
                            let param_arg = <String as std::str::FromStr>::from_str(&param_arg);
                            match param_arg {
                            Ok(param_arg) => Some(param_arg),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter arg - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter arg")),
                        }
                        }
                        None => None,
                    };
                    let param_arg = match param_arg {
                    Some(param_arg) => param_arg,
                    None => return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Missing required query parameter arg"))
                        .expect("Unable to create Bad Request response for missing query parameter arg")),
                };
                    let param_recursive = query_params
                        .iter()
                        .filter(|e| e.0 == "recursive")
                        .map(|e| e.1.clone())
                        .next();
                    let param_recursive = match param_recursive {
                        Some(param_recursive) => {
                            let param_recursive =
                                <bool as std::str::FromStr>::from_str(&param_recursive);
                            match param_recursive {
                            Ok(param_recursive) => Some(param_recursive),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter recursive - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter recursive")),
                        }
                        }
                        None => None,
                    };
                    let param_progress = query_params
                        .iter()
                        .filter(|e| e.0 == "progress")
                        .map(|e| e.1.clone())
                        .next();
                    let param_progress = match param_progress {
                        Some(param_progress) => {
                            let param_progress =
                                <bool as std::str::FromStr>::from_str(&param_progress);
                            match param_progress {
                            Ok(param_progress) => Some(param_progress),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter progress - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter progress")),
                        }
                        }
                        None => None,
                    };

                    let result = api_impl
                        .pin_add_post(param_arg, param_recursive, param_progress, &context)
                        .await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            PinAddPostResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for PIN_ADD_POST_SUCCESS"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            PinAddPostResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for PIN_ADD_POST_BAD_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // PinRmPost - POST /pin/rm
                hyper::Method::POST if path.matched(paths::ID_PIN_RM) => {
                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_arg = query_params
                        .iter()
                        .filter(|e| e.0 == "arg")
                        .map(|e| e.1.clone())
                        .next();
                    let param_arg = match param_arg {
                        Some(param_arg) => {
                            let param_arg = <String as std::str::FromStr>::from_str(&param_arg);
                            match param_arg {
                            Ok(param_arg) => Some(param_arg),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter arg - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter arg")),
                        }
                        }
                        None => None,
                    };
                    let param_arg = match param_arg {
                    Some(param_arg) => param_arg,
                    None => return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Missing required query parameter arg"))
                        .expect("Unable to create Bad Request response for missing query parameter arg")),
                };

                    let result = api_impl.pin_rm_post(param_arg, &context).await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            PinRmPostResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for PIN_RM_POST_SUCCESS"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            PinRmPostResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for PIN_RM_POST_BAD_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // PubsubLsPost - POST /pubsub/ls
                hyper::Method::POST if path.matched(paths::ID_PUBSUB_LS) => {
                    let result = api_impl.pubsub_ls_post(&context).await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            PubsubLsPostResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for PUBSUB_LS_POST_SUCCESS"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            PubsubLsPostResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for PUBSUB_LS_POST_BAD_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // PubsubPubPost - POST /pubsub/pub
                hyper::Method::POST if path.matched(paths::ID_PUBSUB_PUB) => {
                    let boundary =
                        match swagger::multipart::form::boundary(&headers) {
                            Some(boundary) => boundary.to_string(),
                            None => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from("Couldn't find valid multipart body".to_string()))
                                .expect(
                                    "Unable to create Bad Request response for incorrect boundary",
                                )),
                        };

                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_arg = query_params
                        .iter()
                        .filter(|e| e.0 == "arg")
                        .map(|e| e.1.clone())
                        .next();
                    let param_arg = match param_arg {
                        Some(param_arg) => {
                            let param_arg = <String as std::str::FromStr>::from_str(&param_arg);
                            match param_arg {
                            Ok(param_arg) => Some(param_arg),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter arg - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter arg")),
                        }
                        }
                        None => None,
                    };
                    let param_arg = match param_arg {
                    Some(param_arg) => param_arg,
                    None => return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Missing required query parameter arg"))
                        .expect("Unable to create Bad Request response for missing query parameter arg")),
                };

                    // Form Body parameters (note that non-required body parameters will ignore garbage
                    // values, rather than causing a 400 response). Produce warning header and logs for
                    // any unused fields.
                    let result = body.into_raw();
                    match result.await {
                            Ok(body) => {
                                use std::io::Read;

                                // Read Form Parameters from body
                                let mut entries = match Multipart::with_body(&body.to_vec()[..], boundary).save().temp() {
                                    SaveResult::Full(entries) => {
                                        entries
                                    },
                                    _ => {
                                        return Ok(Response::builder()
                                                        .status(StatusCode::BAD_REQUEST)
                                                        .body(Body::from("Unable to process all message parts".to_string()))
                                                        .expect("Unable to create Bad Request response due to failure to process all message"))
                                    },
                                };
                                let field_file = entries.fields.remove("file");
                                let param_file = match field_file {
                                    Some(field) => {
                                        let mut reader = field[0].data.readable().expect("Unable to read field for file");
                                        let mut data = vec![];
                                        reader.read_to_end(&mut data).expect("Reading saved binary data should never fail");
                                        swagger::ByteArray(data)
                                    },
                                    None => {
                                        return Ok(
                                            Response::builder()
                                            .status(StatusCode::BAD_REQUEST)
                                            .body(Body::from("Missing required form parameter file".to_string()))
                                            .expect("Unable to create Bad Request due to missing required form parameter file"))
                                    }
                                };
                                let result = api_impl.pubsub_pub_post(
                                            param_arg,
                                            param_file,
                                        &context
                                    ).await;
                                let mut response = Response::new(Body::empty());
                                response.headers_mut().insert(
                                            HeaderName::from_static("x-span-id"),
                                            HeaderValue::from_str((&context as &dyn Has<XSpanIdString>).get().0.clone().as_str())
                                                .expect("Unable to create X-Span-ID header value"));

                                        match result {
                                            Ok(rsp) => match rsp {
                                                PubsubPubPostResponse::Success
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(200).expect("Unable to turn 200 into a StatusCode");
                                                },
                                                PubsubPubPostResponse::BadRequest
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(400).expect("Unable to turn 400 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for PUBSUB_PUB_POST_BAD_REQUEST"));
                                                    let body = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body);
                                                },
                                            },
                                            Err(_) => {
                                                // Application code returned an error. This should not happen, as the implementation should
                                                // return a valid response.
                                                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                                                *response.body_mut() = Body::from("An internal error occurred");
                                            },
                                        }

                                        Ok(response)
                            },
                            Err(e) => Ok(Response::builder()
                                                .status(StatusCode::BAD_REQUEST)
                                                .body(Body::from("Couldn't read multipart body".to_string()))
                                                .expect("Unable to create Bad Request response due to unable read multipart body")),
                        }
                }

                // PubsubSubPost - POST /pubsub/sub
                hyper::Method::POST if path.matched(paths::ID_PUBSUB_SUB) => {
                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_arg = query_params
                        .iter()
                        .filter(|e| e.0 == "arg")
                        .map(|e| e.1.clone())
                        .next();
                    let param_arg = match param_arg {
                        Some(param_arg) => {
                            let param_arg = <String as std::str::FromStr>::from_str(&param_arg);
                            match param_arg {
                            Ok(param_arg) => Some(param_arg),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter arg - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter arg")),
                        }
                        }
                        None => None,
                    };
                    let param_arg = match param_arg {
                    Some(param_arg) => param_arg,
                    None => return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from("Missing required query parameter arg"))
                        .expect("Unable to create Bad Request response for missing query parameter arg")),
                };

                    let result = api_impl.pubsub_sub_post(param_arg, &context).await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            PubsubSubPostResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/octet-stream")
                                                            .expect("Unable to create Content-Type header for PUBSUB_SUB_POST_SUCCESS"));

                                *response.body_mut() = Body::wrap_stream(body);
                            }
                            PubsubSubPostResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for PUBSUB_SUB_POST_BAD_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // SwarmConnectPost - POST /swarm/connect
                hyper::Method::POST if path.matched(paths::ID_SWARM_CONNECT) => {
                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_arg = query_params
                        .iter()
                        .filter(|e| e.0 == "arg")
                        .map(|e| e.1.clone())
                        .filter_map(|param_arg| param_arg.parse().ok())
                        .collect::<Vec<_>>();

                    let result = api_impl
                        .swarm_connect_post(param_arg.as_ref(), &context)
                        .await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            SwarmConnectPostResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for SWARM_CONNECT_POST_SUCCESS"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            SwarmConnectPostResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for SWARM_CONNECT_POST_BAD_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // SwarmPeersPost - POST /swarm/peers
                hyper::Method::POST if path.matched(paths::ID_SWARM_PEERS) => {
                    let result = api_impl.swarm_peers_post(&context).await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            SwarmPeersPostResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for SWARM_PEERS_POST_SUCCESS"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            SwarmPeersPostResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for SWARM_PEERS_POST_BAD_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                // VersionPost - POST /version
                hyper::Method::POST if path.matched(paths::ID_VERSION) => {
                    let result = api_impl.version_post(&context).await;
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        HeaderName::from_static("x-span-id"),
                        HeaderValue::from_str(
                            (&context as &dyn Has<XSpanIdString>)
                                .get()
                                .0
                                .clone()
                                .as_str(),
                        )
                        .expect("Unable to create X-Span-ID header value"),
                    );

                    match result {
                        Ok(rsp) => match rsp {
                            VersionPostResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for VERSION_POST_SUCCESS"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                            VersionPostResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for VERSION_POST_BAD_REQUEST"));
                                let body = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body);
                            }
                        },
                        Err(_) => {
                            // Application code returned an error. This should not happen, as the implementation should
                            // return a valid response.
                            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            *response.body_mut() = Body::from("An internal error occurred");
                        }
                    }

                    Ok(response)
                }

                _ if path.matched(paths::ID_BLOCK_GET) => method_not_allowed(),
                _ if path.matched(paths::ID_BLOCK_PUT) => method_not_allowed(),
                _ if path.matched(paths::ID_BLOCK_STAT) => method_not_allowed(),
                _ if path.matched(paths::ID_DAG_GET) => method_not_allowed(),
                _ if path.matched(paths::ID_DAG_IMPORT) => method_not_allowed(),
                _ if path.matched(paths::ID_DAG_PUT) => method_not_allowed(),
                _ if path.matched(paths::ID_DAG_RESOLVE) => method_not_allowed(),
                _ if path.matched(paths::ID_ID) => method_not_allowed(),
                _ if path.matched(paths::ID_PIN_ADD) => method_not_allowed(),
                _ if path.matched(paths::ID_PIN_RM) => method_not_allowed(),
                _ if path.matched(paths::ID_PUBSUB_LS) => method_not_allowed(),
                _ if path.matched(paths::ID_PUBSUB_PUB) => method_not_allowed(),
                _ if path.matched(paths::ID_PUBSUB_SUB) => method_not_allowed(),
                _ if path.matched(paths::ID_SWARM_CONNECT) => method_not_allowed(),
                _ if path.matched(paths::ID_SWARM_PEERS) => method_not_allowed(),
                _ if path.matched(paths::ID_VERSION) => method_not_allowed(),
                _ => Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .expect("Unable to create Not Found response")),
            }
        }
        Box::pin(run(self.api_impl.clone(), req))
    }
}

/// Request parser for `Api`.
pub struct ApiRequestParser;
impl<T> RequestParser<T> for ApiRequestParser {
    fn parse_operation_id(request: &Request<T>) -> Option<&'static str> {
        let path = paths::GLOBAL_REGEX_SET.matches(request.uri().path());
        match *request.method() {
            // BlockGetPost - POST /block/get
            hyper::Method::POST if path.matched(paths::ID_BLOCK_GET) => Some("BlockGetPost"),
            // BlockPutPost - POST /block/put
            hyper::Method::POST if path.matched(paths::ID_BLOCK_PUT) => Some("BlockPutPost"),
            // BlockStatPost - POST /block/stat
            hyper::Method::POST if path.matched(paths::ID_BLOCK_STAT) => Some("BlockStatPost"),
            // DagGetPost - POST /dag/get
            hyper::Method::POST if path.matched(paths::ID_DAG_GET) => Some("DagGetPost"),
            // DagImportPost - POST /dag/import
            hyper::Method::POST if path.matched(paths::ID_DAG_IMPORT) => Some("DagImportPost"),
            // DagPutPost - POST /dag/put
            hyper::Method::POST if path.matched(paths::ID_DAG_PUT) => Some("DagPutPost"),
            // DagResolvePost - POST /dag/resolve
            hyper::Method::POST if path.matched(paths::ID_DAG_RESOLVE) => Some("DagResolvePost"),
            // IdPost - POST /id
            hyper::Method::POST if path.matched(paths::ID_ID) => Some("IdPost"),
            // PinAddPost - POST /pin/add
            hyper::Method::POST if path.matched(paths::ID_PIN_ADD) => Some("PinAddPost"),
            // PinRmPost - POST /pin/rm
            hyper::Method::POST if path.matched(paths::ID_PIN_RM) => Some("PinRmPost"),
            // PubsubLsPost - POST /pubsub/ls
            hyper::Method::POST if path.matched(paths::ID_PUBSUB_LS) => Some("PubsubLsPost"),
            // PubsubPubPost - POST /pubsub/pub
            hyper::Method::POST if path.matched(paths::ID_PUBSUB_PUB) => Some("PubsubPubPost"),
            // PubsubSubPost - POST /pubsub/sub
            hyper::Method::POST if path.matched(paths::ID_PUBSUB_SUB) => Some("PubsubSubPost"),
            // SwarmConnectPost - POST /swarm/connect
            hyper::Method::POST if path.matched(paths::ID_SWARM_CONNECT) => {
                Some("SwarmConnectPost")
            }
            // SwarmPeersPost - POST /swarm/peers
            hyper::Method::POST if path.matched(paths::ID_SWARM_PEERS) => Some("SwarmPeersPost"),
            // VersionPost - POST /version
            hyper::Method::POST if path.matched(paths::ID_VERSION) => Some("VersionPost"),
            _ => None,
        }
    }
}
