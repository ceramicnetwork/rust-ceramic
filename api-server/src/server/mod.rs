use futures::{future, future::BoxFuture, future::FutureExt, stream, stream::TryStreamExt, Stream};
use hyper::header::{HeaderName, HeaderValue, CONTENT_TYPE};
use hyper::{Body, HeaderMap, Request, Response, StatusCode};
use log::warn;
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

use crate::{Api, CeramicEventsPostResponse, CeramicSubscribeSortKeySortValueGetResponse};

mod paths {
    use lazy_static::lazy_static;

    lazy_static! {
        pub static ref GLOBAL_REGEX_SET: regex::RegexSet = regex::RegexSet::new(vec![
            r"^/ceramic/events$",
            r"^/ceramic/subscribe/(?P<sort-key>[^/?#]*)/(?P<sort-value>[^/?#]*)$"
        ])
        .expect("Unable to create global regex set");
    }
    pub(crate) static ID_CERAMIC_EVENTS: usize = 0;
    pub(crate) static ID_CERAMIC_SUBSCRIBE_SORT_KEY_SORT_VALUE: usize = 1;
    lazy_static! {
        pub static ref REGEX_CERAMIC_SUBSCRIBE_SORT_KEY_SORT_VALUE: regex::Regex =
            #[allow(clippy::invalid_regex)]
            regex::Regex::new(
                r"^/ceramic/subscribe/(?P<sort-key>[^/?#]*)/(?P<sort-value>[^/?#]*)$"
            )
            .expect("Unable to create regex for CERAMIC_SUBSCRIBE_SORT_KEY_SORT_VALUE");
    }
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
                // CeramicEventsPost - POST /ceramic/events
                hyper::Method::POST if path.matched(paths::ID_CERAMIC_EVENTS) => {
                    // Body parameters (note that non-required body parameters will ignore garbage
                    // values, rather than causing a 400 response). Produce warning header and logs for
                    // any unused fields.
                    let result = body.into_raw().await;
                    match result {
                            Ok(body) => {
                                let mut unused_elements = Vec::new();
                                let param_event: Option<models::Event> = if !body.is_empty() {
                                    let deserializer = &mut serde_json::Deserializer::from_slice(&body);
                                    match serde_ignored::deserialize(deserializer, |path| {
                                            warn!("Ignoring unknown field in body: {}", path);
                                            unused_elements.push(path.to_string());
                                    }) {
                                        Ok(param_event) => param_event,
                                        Err(e) => return Ok(Response::builder()
                                                        .status(StatusCode::BAD_REQUEST)
                                                        .body(Body::from(format!("Couldn't parse body parameter Event - doesn't match schema: {}", e)))
                                                        .expect("Unable to create Bad Request response for invalid body parameter Event due to schema")),
                                    }
                                } else {
                                    None
                                };
                                let param_event = match param_event {
                                    Some(param_event) => param_event,
                                    None => return Ok(Response::builder()
                                                        .status(StatusCode::BAD_REQUEST)
                                                        .body(Body::from("Missing required body parameter Event"))
                                                        .expect("Unable to create Bad Request response for missing body parameter Event")),
                                };

                                let result = api_impl.ceramic_events_post(
                                            param_event,
                                        &context
                                    ).await;
                                let mut response = Response::new(Body::empty());
                                response.headers_mut().insert(
                                            HeaderName::from_static("x-span-id"),
                                            HeaderValue::from_str((&context as &dyn Has<XSpanIdString>).get().0.clone().as_str())
                                                .expect("Unable to create X-Span-ID header value"));

                                        if !unused_elements.is_empty() {
                                            response.headers_mut().insert(
                                                HeaderName::from_static("warning"),
                                                HeaderValue::from_str(format!("Ignoring unknown fields in body: {:?}", unused_elements).as_str())
                                                    .expect("Unable to create Warning header value"));
                                        }

                                        match result {
                                            Ok(rsp) => match rsp {
                                                CeramicEventsPostResponse::Success
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(204).expect("Unable to turn 204 into a StatusCode");
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
                                                .body(Body::from(format!("Couldn't read body parameter Event: {}", e)))
                                                .expect("Unable to create Bad Request response due to unable to read body parameter Event")),
                        }
                }

                // CeramicSubscribeSortKeySortValueGet - GET /ceramic/subscribe/{sort-key}/{sort-value}
                hyper::Method::GET
                    if path.matched(paths::ID_CERAMIC_SUBSCRIBE_SORT_KEY_SORT_VALUE) =>
                {
                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_CERAMIC_SUBSCRIBE_SORT_KEY_SORT_VALUE
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE CERAMIC_SUBSCRIBE_SORT_KEY_SORT_VALUE in set but failed match against \"{}\"", path, paths::REGEX_CERAMIC_SUBSCRIBE_SORT_KEY_SORT_VALUE.as_str())
                    );

                    let param_sort_key = match percent_encoding::percent_decode(path_params["sort-key"].as_bytes()).decode_utf8() {
                    Ok(param_sort_key) => match param_sort_key.parse::<String>() {
                        Ok(param_sort_key) => param_sort_key,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter sort-key: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["sort-key"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_sort_value = match percent_encoding::percent_decode(path_params["sort-value"].as_bytes()).decode_utf8() {
                    Ok(param_sort_value) => match param_sort_value.parse::<String>() {
                        Ok(param_sort_value) => param_sort_value,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter sort-value: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["sort-value"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_controller = query_params
                        .iter()
                        .filter(|e| e.0 == "controller")
                        .map(|e| e.1.clone())
                        .next();
                    let param_controller = match param_controller {
                        Some(param_controller) => {
                            let param_controller =
                                <String as std::str::FromStr>::from_str(&param_controller);
                            match param_controller {
                            Ok(param_controller) => Some(param_controller),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter controller - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter controller")),
                        }
                        }
                        None => None,
                    };
                    let param_stream_id = query_params
                        .iter()
                        .filter(|e| e.0 == "streamId")
                        .map(|e| e.1.clone())
                        .next();
                    let param_stream_id = match param_stream_id {
                        Some(param_stream_id) => {
                            let param_stream_id =
                                <String as std::str::FromStr>::from_str(&param_stream_id);
                            match param_stream_id {
                            Ok(param_stream_id) => Some(param_stream_id),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter streamId - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter streamId")),
                        }
                        }
                        None => None,
                    };
                    let param_offset = query_params
                        .iter()
                        .filter(|e| e.0 == "offset")
                        .map(|e| e.1.clone())
                        .next();
                    let param_offset = match param_offset {
                        Some(param_offset) => {
                            let param_offset = <f64 as std::str::FromStr>::from_str(&param_offset);
                            match param_offset {
                            Ok(param_offset) => Some(param_offset),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter offset - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter offset")),
                        }
                        }
                        None => None,
                    };
                    let param_limit = query_params
                        .iter()
                        .filter(|e| e.0 == "limit")
                        .map(|e| e.1.clone())
                        .next();
                    let param_limit = match param_limit {
                        Some(param_limit) => {
                            let param_limit = <f64 as std::str::FromStr>::from_str(&param_limit);
                            match param_limit {
                            Ok(param_limit) => Some(param_limit),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter limit - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter limit")),
                        }
                        }
                        None => None,
                    };

                    let result = api_impl
                        .ceramic_subscribe_sort_key_sort_value_get(
                            param_sort_key,
                            param_sort_value,
                            param_controller,
                            param_stream_id,
                            param_offset,
                            param_limit,
                            &context,
                        )
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
                            CeramicSubscribeSortKeySortValueGetResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CERAMIC_SUBSCRIBE_SORT_KEY_SORT_VALUE_GET_SUCCESS"));
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

                _ if path.matched(paths::ID_CERAMIC_EVENTS) => method_not_allowed(),
                _ if path.matched(paths::ID_CERAMIC_SUBSCRIBE_SORT_KEY_SORT_VALUE) => {
                    method_not_allowed()
                }
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
            // CeramicEventsPost - POST /ceramic/events
            hyper::Method::POST if path.matched(paths::ID_CERAMIC_EVENTS) => {
                Some("CeramicEventsPost")
            }
            // CeramicSubscribeSortKeySortValueGet - GET /ceramic/subscribe/{sort-key}/{sort-value}
            hyper::Method::GET if path.matched(paths::ID_CERAMIC_SUBSCRIBE_SORT_KEY_SORT_VALUE) => {
                Some("CeramicSubscribeSortKeySortValueGet")
            }
            _ => None,
        }
    }
}
