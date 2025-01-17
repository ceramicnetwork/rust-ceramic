#![allow(clippy::blocks_in_conditions)]
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

use crate::{
    Api, ConfigNetworkGetResponse, ConfigNetworkOptionsResponse, DebugHeapGetResponse,
    DebugHeapOptionsResponse, EventsEventIdGetResponse, EventsEventIdOptionsResponse,
    EventsOptionsResponse, EventsPostResponse, ExperimentalEventsSepSepValueGetResponse,
    ExperimentalEventsSepSepValueOptionsResponse, ExperimentalInterestsGetResponse,
    ExperimentalInterestsOptionsResponse, FeedEventsGetResponse, FeedEventsOptionsResponse,
    FeedResumeTokenGetResponse, FeedResumeTokenOptionsResponse, InterestsOptionsResponse,
    InterestsPostResponse, InterestsSortKeySortValueOptionsResponse,
    InterestsSortKeySortValuePostResponse, LivenessGetResponse, LivenessOptionsResponse,
    PeersGetResponse, PeersOptionsResponse, PeersPostResponse, StreamsStreamIdGetResponse,
    StreamsStreamIdOptionsResponse, VersionGetResponse, VersionOptionsResponse,
    VersionPostResponse,
};

mod paths {
    use lazy_static::lazy_static;

    lazy_static! {
        pub static ref GLOBAL_REGEX_SET: regex::RegexSet = regex::RegexSet::new(vec![
            r"^/ceramic/config/network$",
            r"^/ceramic/debug/heap$",
            r"^/ceramic/events$",
            r"^/ceramic/events/(?P<event_id>[^/?#]*)$",
            r"^/ceramic/experimental/events/(?P<sep>[^/?#]*)/(?P<sepValue>[^/?#]*)$",
            r"^/ceramic/experimental/interests$",
            r"^/ceramic/feed/events$",
            r"^/ceramic/feed/resumeToken$",
            r"^/ceramic/interests$",
            r"^/ceramic/interests/(?P<sort_key>[^/?#]*)/(?P<sort_value>[^/?#]*)$",
            r"^/ceramic/liveness$",
            r"^/ceramic/peers$",
            r"^/ceramic/streams/(?P<stream_id>[^/?#]*)$",
            r"^/ceramic/version$"
        ])
        .expect("Unable to create global regex set");
    }
    pub(crate) static ID_CONFIG_NETWORK: usize = 0;
    pub(crate) static ID_DEBUG_HEAP: usize = 1;
    pub(crate) static ID_EVENTS: usize = 2;
    pub(crate) static ID_EVENTS_EVENT_ID: usize = 3;
    lazy_static! {
        pub static ref REGEX_EVENTS_EVENT_ID: regex::Regex =
            #[allow(clippy::invalid_regex)]
            regex::Regex::new(r"^/ceramic/events/(?P<event_id>[^/?#]*)$")
                .expect("Unable to create regex for EVENTS_EVENT_ID");
    }
    pub(crate) static ID_EXPERIMENTAL_EVENTS_SEP_SEPVALUE: usize = 4;
    lazy_static! {
        pub static ref REGEX_EXPERIMENTAL_EVENTS_SEP_SEPVALUE: regex::Regex =
            #[allow(clippy::invalid_regex)]
            regex::Regex::new(
                r"^/ceramic/experimental/events/(?P<sep>[^/?#]*)/(?P<sepValue>[^/?#]*)$"
            )
            .expect("Unable to create regex for EXPERIMENTAL_EVENTS_SEP_SEPVALUE");
    }
    pub(crate) static ID_EXPERIMENTAL_INTERESTS: usize = 5;
    pub(crate) static ID_FEED_EVENTS: usize = 6;
    pub(crate) static ID_FEED_RESUMETOKEN: usize = 7;
    pub(crate) static ID_INTERESTS: usize = 8;
    pub(crate) static ID_INTERESTS_SORT_KEY_SORT_VALUE: usize = 9;
    lazy_static! {
        pub static ref REGEX_INTERESTS_SORT_KEY_SORT_VALUE: regex::Regex =
            #[allow(clippy::invalid_regex)]
            regex::Regex::new(
                r"^/ceramic/interests/(?P<sort_key>[^/?#]*)/(?P<sort_value>[^/?#]*)$"
            )
            .expect("Unable to create regex for INTERESTS_SORT_KEY_SORT_VALUE");
    }
    pub(crate) static ID_LIVENESS: usize = 10;
    pub(crate) static ID_PEERS: usize = 11;
    pub(crate) static ID_STREAMS_STREAM_ID: usize = 12;
    lazy_static! {
        pub static ref REGEX_STREAMS_STREAM_ID: regex::Regex =
            #[allow(clippy::invalid_regex)]
            regex::Regex::new(r"^/ceramic/streams/(?P<stream_id>[^/?#]*)$")
                .expect("Unable to create regex for STREAMS_STREAM_ID");
    }
    pub(crate) static ID_VERSION: usize = 13;
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
                // ConfigNetworkGet - GET /config/network
                hyper::Method::GET if path.matched(paths::ID_CONFIG_NETWORK) => {
                    let result = api_impl.config_network_get(&context).await;
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
                            ConfigNetworkGetResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for CONFIG_NETWORK_GET_SUCCESS"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
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

                // ConfigNetworkOptions - OPTIONS /config/network
                hyper::Method::OPTIONS if path.matched(paths::ID_CONFIG_NETWORK) => {
                    let result = api_impl.config_network_options(&context).await;
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
                            ConfigNetworkOptionsResponse::Cors => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
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

                // DebugHeapGet - GET /debug/heap
                hyper::Method::GET if path.matched(paths::ID_DEBUG_HEAP) => {
                    let result = api_impl.debug_heap_get(&context).await;
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
                            DebugHeapGetResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/octet-stream")
                                                            .expect("Unable to create Content-Type header for DEBUG_HEAP_GET_SUCCESS"));
                                let body_content = body.0;
                                *response.body_mut() = Body::from(body_content);
                            }
                            DebugHeapGetResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DEBUG_HEAP_GET_BAD_REQUEST"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            DebugHeapGetResponse::InternalServerError(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 500 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for DEBUG_HEAP_GET_INTERNAL_SERVER_ERROR"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
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

                // DebugHeapOptions - OPTIONS /debug/heap
                hyper::Method::OPTIONS if path.matched(paths::ID_DEBUG_HEAP) => {
                    let result = api_impl.debug_heap_options(&context).await;
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
                            DebugHeapOptionsResponse::Cors => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
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

                // EventsEventIdGet - GET /events/{event_id}
                hyper::Method::GET if path.matched(paths::ID_EVENTS_EVENT_ID) => {
                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_EVENTS_EVENT_ID
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE EVENTS_EVENT_ID in set but failed match against \"{}\"", path, paths::REGEX_EVENTS_EVENT_ID.as_str())
                    );

                    let param_event_id = match percent_encoding::percent_decode(path_params["event_id"].as_bytes()).decode_utf8() {
                    Ok(param_event_id) => match param_event_id.parse::<String>() {
                        Ok(param_event_id) => param_event_id,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter event_id: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["event_id"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let result = api_impl.events_event_id_get(param_event_id, &context).await;
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
                            EventsEventIdGetResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for EVENTS_EVENT_ID_GET_SUCCESS"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            EventsEventIdGetResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for EVENTS_EVENT_ID_GET_BAD_REQUEST"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            EventsEventIdGetResponse::EventNotFound(body) => {
                                *response.status_mut() = StatusCode::from_u16(404)
                                    .expect("Unable to turn 404 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("text/plain")
                                                            .expect("Unable to create Content-Type header for EVENTS_EVENT_ID_GET_EVENT_NOT_FOUND"));
                                let body_content = body;
                                *response.body_mut() = Body::from(body_content);
                            }
                            EventsEventIdGetResponse::InternalServerError(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 500 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for EVENTS_EVENT_ID_GET_INTERNAL_SERVER_ERROR"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
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

                // EventsEventIdOptions - OPTIONS /events/{event_id}
                hyper::Method::OPTIONS if path.matched(paths::ID_EVENTS_EVENT_ID) => {
                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_EVENTS_EVENT_ID
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE EVENTS_EVENT_ID in set but failed match against \"{}\"", path, paths::REGEX_EVENTS_EVENT_ID.as_str())
                    );

                    let param_event_id = match percent_encoding::percent_decode(path_params["event_id"].as_bytes()).decode_utf8() {
                    Ok(param_event_id) => match param_event_id.parse::<String>() {
                        Ok(param_event_id) => param_event_id,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter event_id: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["event_id"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let result = api_impl
                        .events_event_id_options(param_event_id, &context)
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
                            EventsEventIdOptionsResponse::Cors => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
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

                // EventsOptions - OPTIONS /events
                hyper::Method::OPTIONS if path.matched(paths::ID_EVENTS) => {
                    let result = api_impl.events_options(&context).await;
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
                            EventsOptionsResponse::Cors => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
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

                // EventsPost - POST /events
                hyper::Method::POST if path.matched(paths::ID_EVENTS) => {
                    // Body parameters (note that non-required body parameters will ignore garbage
                    // values, rather than causing a 400 response). Produce warning header and logs for
                    // any unused fields.
                    let result = body.into_raw().await;
                    match result {
                            Ok(body) => {
                                let mut unused_elements = Vec::new();
                                let param_event_data: Option<models::EventData> = if !body.is_empty() {
                                    let deserializer = &mut serde_json::Deserializer::from_slice(&body);
                                    match serde_ignored::deserialize(deserializer, |path| {
                                            warn!("Ignoring unknown field in body: {}", path);
                                            unused_elements.push(path.to_string());
                                    }) {
                                        Ok(param_event_data) => param_event_data,
                                        Err(e) => return Ok(Response::builder()
                                                        .status(StatusCode::BAD_REQUEST)
                                                        .body(Body::from(format!("Couldn't parse body parameter EventData - doesn't match schema: {}", e)))
                                                        .expect("Unable to create Bad Request response for invalid body parameter EventData due to schema")),
                                    }
                                } else {
                                    None
                                };
                                let param_event_data = match param_event_data {
                                    Some(param_event_data) => param_event_data,
                                    None => return Ok(Response::builder()
                                                        .status(StatusCode::BAD_REQUEST)
                                                        .body(Body::from("Missing required body parameter EventData"))
                                                        .expect("Unable to create Bad Request response for missing body parameter EventData")),
                                };

                                let result = api_impl.events_post(
                                            param_event_data,
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
                                                EventsPostResponse::Success
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(204).expect("Unable to turn 204 into a StatusCode");
                                                },
                                                EventsPostResponse::BadRequest
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(400).expect("Unable to turn 400 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for EVENTS_POST_BAD_REQUEST"));
                                                    let body_content = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body_content);
                                                },
                                                EventsPostResponse::InternalServerError
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(500).expect("Unable to turn 500 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for EVENTS_POST_INTERNAL_SERVER_ERROR"));
                                                    let body_content = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body_content);
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
                                                .body(Body::from(format!("Couldn't read body parameter EventData: {}", e)))
                                                .expect("Unable to create Bad Request response due to unable to read body parameter EventData")),
                        }
                }

                // ExperimentalEventsSepSepValueGet - GET /experimental/events/{sep}/{sepValue}
                hyper::Method::GET if path.matched(paths::ID_EXPERIMENTAL_EVENTS_SEP_SEPVALUE) => {
                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_EXPERIMENTAL_EVENTS_SEP_SEPVALUE
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE EXPERIMENTAL_EVENTS_SEP_SEPVALUE in set but failed match against \"{}\"", path, paths::REGEX_EXPERIMENTAL_EVENTS_SEP_SEPVALUE.as_str())
                    );

                    let param_sep = match percent_encoding::percent_decode(path_params["sep"].as_bytes()).decode_utf8() {
                    Ok(param_sep) => match param_sep.parse::<String>() {
                        Ok(param_sep) => param_sep,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter sep: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["sep"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_sep_value = match percent_encoding::percent_decode(path_params["sepValue"].as_bytes()).decode_utf8() {
                    Ok(param_sep_value) => match param_sep_value.parse::<String>() {
                        Ok(param_sep_value) => param_sep_value,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter sepValue: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["sepValue"])))
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
                            let param_offset = <i32 as std::str::FromStr>::from_str(&param_offset);
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
                            let param_limit = <i32 as std::str::FromStr>::from_str(&param_limit);
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
                        .experimental_events_sep_sep_value_get(
                            param_sep,
                            param_sep_value,
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
                            ExperimentalEventsSepSepValueGetResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for EXPERIMENTAL_EVENTS_SEP_SEP_VALUE_GET_SUCCESS"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            ExperimentalEventsSepSepValueGetResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for EXPERIMENTAL_EVENTS_SEP_SEP_VALUE_GET_BAD_REQUEST"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            ExperimentalEventsSepSepValueGetResponse::InternalServerError(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 500 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for EXPERIMENTAL_EVENTS_SEP_SEP_VALUE_GET_INTERNAL_SERVER_ERROR"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
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

                // ExperimentalEventsSepSepValueOptions - OPTIONS /experimental/events/{sep}/{sepValue}
                hyper::Method::OPTIONS
                    if path.matched(paths::ID_EXPERIMENTAL_EVENTS_SEP_SEPVALUE) =>
                {
                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_EXPERIMENTAL_EVENTS_SEP_SEPVALUE
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE EXPERIMENTAL_EVENTS_SEP_SEPVALUE in set but failed match against \"{}\"", path, paths::REGEX_EXPERIMENTAL_EVENTS_SEP_SEPVALUE.as_str())
                    );

                    let param_sep = match percent_encoding::percent_decode(path_params["sep"].as_bytes()).decode_utf8() {
                    Ok(param_sep) => match param_sep.parse::<String>() {
                        Ok(param_sep) => param_sep,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter sep: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["sep"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_sep_value = match percent_encoding::percent_decode(path_params["sepValue"].as_bytes()).decode_utf8() {
                    Ok(param_sep_value) => match param_sep_value.parse::<String>() {
                        Ok(param_sep_value) => param_sep_value,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter sepValue: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["sepValue"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let result = api_impl
                        .experimental_events_sep_sep_value_options(
                            param_sep,
                            param_sep_value,
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
                            ExperimentalEventsSepSepValueOptionsResponse::Cors => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
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

                // ExperimentalInterestsGet - GET /experimental/interests
                hyper::Method::GET if path.matched(paths::ID_EXPERIMENTAL_INTERESTS) => {
                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_peer_id = query_params
                        .iter()
                        .filter(|e| e.0 == "peerId")
                        .map(|e| e.1.clone())
                        .next();
                    let param_peer_id = match param_peer_id {
                        Some(param_peer_id) => {
                            let param_peer_id =
                                <String as std::str::FromStr>::from_str(&param_peer_id);
                            match param_peer_id {
                            Ok(param_peer_id) => Some(param_peer_id),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter peerId - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter peerId")),
                        }
                        }
                        None => None,
                    };

                    let result = api_impl
                        .experimental_interests_get(param_peer_id, &context)
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
                            ExperimentalInterestsGetResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for EXPERIMENTAL_INTERESTS_GET_SUCCESS"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            ExperimentalInterestsGetResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for EXPERIMENTAL_INTERESTS_GET_BAD_REQUEST"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            ExperimentalInterestsGetResponse::InternalServerError(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 500 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for EXPERIMENTAL_INTERESTS_GET_INTERNAL_SERVER_ERROR"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
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

                // ExperimentalInterestsOptions - OPTIONS /experimental/interests
                hyper::Method::OPTIONS if path.matched(paths::ID_EXPERIMENTAL_INTERESTS) => {
                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_peer_id = query_params
                        .iter()
                        .filter(|e| e.0 == "peerId")
                        .map(|e| e.1.clone())
                        .next();
                    let param_peer_id = match param_peer_id {
                        Some(param_peer_id) => {
                            let param_peer_id =
                                <String as std::str::FromStr>::from_str(&param_peer_id);
                            match param_peer_id {
                            Ok(param_peer_id) => Some(param_peer_id),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter peerId - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter peerId")),
                        }
                        }
                        None => None,
                    };

                    let result = api_impl
                        .experimental_interests_options(param_peer_id, &context)
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
                            ExperimentalInterestsOptionsResponse::Cors => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
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

                // FeedEventsGet - GET /feed/events
                hyper::Method::GET if path.matched(paths::ID_FEED_EVENTS) => {
                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_resume_at = query_params
                        .iter()
                        .filter(|e| e.0 == "resumeAt")
                        .map(|e| e.1.clone())
                        .next();
                    let param_resume_at = match param_resume_at {
                        Some(param_resume_at) => {
                            let param_resume_at =
                                <String as std::str::FromStr>::from_str(&param_resume_at);
                            match param_resume_at {
                            Ok(param_resume_at) => Some(param_resume_at),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter resumeAt - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter resumeAt")),
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
                            let param_limit = <i32 as std::str::FromStr>::from_str(&param_limit);
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
                    let param_include_data = query_params
                        .iter()
                        .filter(|e| e.0 == "includeData")
                        .map(|e| e.1.clone())
                        .next();
                    let param_include_data = match param_include_data {
                        Some(param_include_data) => {
                            let param_include_data =
                                <String as std::str::FromStr>::from_str(&param_include_data);
                            match param_include_data {
                            Ok(param_include_data) => Some(param_include_data),
                            Err(e) => return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(format!("Couldn't parse query parameter includeData - doesn't match schema: {}", e)))
                                .expect("Unable to create Bad Request response for invalid query parameter includeData")),
                        }
                        }
                        None => None,
                    };

                    let result = api_impl
                        .feed_events_get(param_resume_at, param_limit, param_include_data, &context)
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
                            FeedEventsGetResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for FEED_EVENTS_GET_SUCCESS"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            FeedEventsGetResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for FEED_EVENTS_GET_BAD_REQUEST"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            FeedEventsGetResponse::InternalServerError(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 500 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for FEED_EVENTS_GET_INTERNAL_SERVER_ERROR"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
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

                // FeedEventsOptions - OPTIONS /feed/events
                hyper::Method::OPTIONS if path.matched(paths::ID_FEED_EVENTS) => {
                    let result = api_impl.feed_events_options(&context).await;
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
                            FeedEventsOptionsResponse::Cors => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
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

                // FeedResumeTokenGet - GET /feed/resumeToken
                hyper::Method::GET if path.matched(paths::ID_FEED_RESUMETOKEN) => {
                    let result = api_impl.feed_resume_token_get(&context).await;
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
                            FeedResumeTokenGetResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for FEED_RESUME_TOKEN_GET_SUCCESS"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            FeedResumeTokenGetResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for FEED_RESUME_TOKEN_GET_BAD_REQUEST"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            FeedResumeTokenGetResponse::InternalServerError(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 500 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for FEED_RESUME_TOKEN_GET_INTERNAL_SERVER_ERROR"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
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

                // FeedResumeTokenOptions - OPTIONS /feed/resumeToken
                hyper::Method::OPTIONS if path.matched(paths::ID_FEED_RESUMETOKEN) => {
                    let result = api_impl.feed_resume_token_options(&context).await;
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
                            FeedResumeTokenOptionsResponse::Cors => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
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

                // InterestsOptions - OPTIONS /interests
                hyper::Method::OPTIONS if path.matched(paths::ID_INTERESTS) => {
                    let result = api_impl.interests_options(&context).await;
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
                            InterestsOptionsResponse::Cors => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
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

                // InterestsPost - POST /interests
                hyper::Method::POST if path.matched(paths::ID_INTERESTS) => {
                    // Body parameters (note that non-required body parameters will ignore garbage
                    // values, rather than causing a 400 response). Produce warning header and logs for
                    // any unused fields.
                    let result = body.into_raw().await;
                    match result {
                            Ok(body) => {
                                let mut unused_elements = Vec::new();
                                let param_interest: Option<models::Interest> = if !body.is_empty() {
                                    let deserializer = &mut serde_json::Deserializer::from_slice(&body);
                                    match serde_ignored::deserialize(deserializer, |path| {
                                            warn!("Ignoring unknown field in body: {}", path);
                                            unused_elements.push(path.to_string());
                                    }) {
                                        Ok(param_interest) => param_interest,
                                        Err(e) => return Ok(Response::builder()
                                                        .status(StatusCode::BAD_REQUEST)
                                                        .body(Body::from(format!("Couldn't parse body parameter Interest - doesn't match schema: {}", e)))
                                                        .expect("Unable to create Bad Request response for invalid body parameter Interest due to schema")),
                                    }
                                } else {
                                    None
                                };
                                let param_interest = match param_interest {
                                    Some(param_interest) => param_interest,
                                    None => return Ok(Response::builder()
                                                        .status(StatusCode::BAD_REQUEST)
                                                        .body(Body::from("Missing required body parameter Interest"))
                                                        .expect("Unable to create Bad Request response for missing body parameter Interest")),
                                };

                                let result = api_impl.interests_post(
                                            param_interest,
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
                                                InterestsPostResponse::Success
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(204).expect("Unable to turn 204 into a StatusCode");
                                                },
                                                InterestsPostResponse::BadRequest
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(400).expect("Unable to turn 400 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for INTERESTS_POST_BAD_REQUEST"));
                                                    let body_content = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body_content);
                                                },
                                                InterestsPostResponse::InternalServerError
                                                    (body)
                                                => {
                                                    *response.status_mut() = StatusCode::from_u16(500).expect("Unable to turn 500 into a StatusCode");
                                                    response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for INTERESTS_POST_INTERNAL_SERVER_ERROR"));
                                                    let body_content = serde_json::to_string(&body).expect("impossible to fail to serialize");
                                                    *response.body_mut() = Body::from(body_content);
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
                                                .body(Body::from(format!("Couldn't read body parameter Interest: {}", e)))
                                                .expect("Unable to create Bad Request response due to unable to read body parameter Interest")),
                        }
                }

                // InterestsSortKeySortValueOptions - OPTIONS /interests/{sort_key}/{sort_value}
                hyper::Method::OPTIONS if path.matched(paths::ID_INTERESTS_SORT_KEY_SORT_VALUE) => {
                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_INTERESTS_SORT_KEY_SORT_VALUE
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE INTERESTS_SORT_KEY_SORT_VALUE in set but failed match against \"{}\"", path, paths::REGEX_INTERESTS_SORT_KEY_SORT_VALUE.as_str())
                    );

                    let param_sort_key = match percent_encoding::percent_decode(path_params["sort_key"].as_bytes()).decode_utf8() {
                    Ok(param_sort_key) => match param_sort_key.parse::<String>() {
                        Ok(param_sort_key) => param_sort_key,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter sort_key: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["sort_key"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_sort_value = match percent_encoding::percent_decode(path_params["sort_value"].as_bytes()).decode_utf8() {
                    Ok(param_sort_value) => match param_sort_value.parse::<String>() {
                        Ok(param_sort_value) => param_sort_value,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter sort_value: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["sort_value"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let result = api_impl
                        .interests_sort_key_sort_value_options(
                            param_sort_key,
                            param_sort_value,
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
                            InterestsSortKeySortValueOptionsResponse::Cors => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
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

                // InterestsSortKeySortValuePost - POST /interests/{sort_key}/{sort_value}
                hyper::Method::POST if path.matched(paths::ID_INTERESTS_SORT_KEY_SORT_VALUE) => {
                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_INTERESTS_SORT_KEY_SORT_VALUE
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE INTERESTS_SORT_KEY_SORT_VALUE in set but failed match against \"{}\"", path, paths::REGEX_INTERESTS_SORT_KEY_SORT_VALUE.as_str())
                    );

                    let param_sort_key = match percent_encoding::percent_decode(path_params["sort_key"].as_bytes()).decode_utf8() {
                    Ok(param_sort_key) => match param_sort_key.parse::<String>() {
                        Ok(param_sort_key) => param_sort_key,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter sort_key: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["sort_key"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let param_sort_value = match percent_encoding::percent_decode(path_params["sort_value"].as_bytes()).decode_utf8() {
                    Ok(param_sort_value) => match param_sort_value.parse::<String>() {
                        Ok(param_sort_value) => param_sort_value,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter sort_value: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["sort_value"])))
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

                    let result = api_impl
                        .interests_sort_key_sort_value_post(
                            param_sort_key,
                            param_sort_value,
                            param_controller,
                            param_stream_id,
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
                            InterestsSortKeySortValuePostResponse::Success => {
                                *response.status_mut() = StatusCode::from_u16(204)
                                    .expect("Unable to turn 204 into a StatusCode");
                            }
                            InterestsSortKeySortValuePostResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for INTERESTS_SORT_KEY_SORT_VALUE_POST_BAD_REQUEST"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            InterestsSortKeySortValuePostResponse::InternalServerError(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 500 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for INTERESTS_SORT_KEY_SORT_VALUE_POST_INTERNAL_SERVER_ERROR"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
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

                // LivenessGet - GET /liveness
                hyper::Method::GET if path.matched(paths::ID_LIVENESS) => {
                    let result = api_impl.liveness_get(&context).await;
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
                            LivenessGetResponse::Success => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                            }
                            LivenessGetResponse::InternalServerError(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 500 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for LIVENESS_GET_INTERNAL_SERVER_ERROR"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
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

                // LivenessOptions - OPTIONS /liveness
                hyper::Method::OPTIONS if path.matched(paths::ID_LIVENESS) => {
                    let result = api_impl.liveness_options(&context).await;
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
                            LivenessOptionsResponse::Cors => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
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

                // PeersGet - GET /peers
                hyper::Method::GET if path.matched(paths::ID_PEERS) => {
                    let result = api_impl.peers_get(&context).await;
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
                            PeersGetResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for PEERS_GET_SUCCESS"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            PeersGetResponse::InternalServerError(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 500 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for PEERS_GET_INTERNAL_SERVER_ERROR"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
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

                // PeersOptions - OPTIONS /peers
                hyper::Method::OPTIONS if path.matched(paths::ID_PEERS) => {
                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_addresses = query_params
                        .iter()
                        .filter(|e| e.0 == "addresses")
                        .map(|e| e.1.clone())
                        .filter_map(|param_addresses| param_addresses.parse().ok())
                        .collect::<Vec<_>>();

                    let result = api_impl
                        .peers_options(param_addresses.as_ref(), &context)
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
                            PeersOptionsResponse::Cors => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
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

                // PeersPost - POST /peers
                hyper::Method::POST if path.matched(paths::ID_PEERS) => {
                    // Query parameters (note that non-required or collection query parameters will ignore garbage values, rather than causing a 400 response)
                    let query_params =
                        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .collect::<Vec<_>>();
                    let param_addresses = query_params
                        .iter()
                        .filter(|e| e.0 == "addresses")
                        .map(|e| e.1.clone())
                        .filter_map(|param_addresses| param_addresses.parse().ok())
                        .collect::<Vec<_>>();

                    let result = api_impl
                        .peers_post(param_addresses.as_ref(), &context)
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
                            PeersPostResponse::Success => {
                                *response.status_mut() = StatusCode::from_u16(204)
                                    .expect("Unable to turn 204 into a StatusCode");
                            }
                            PeersPostResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for PEERS_POST_BAD_REQUEST"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            PeersPostResponse::InternalServerError(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 500 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for PEERS_POST_INTERNAL_SERVER_ERROR"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
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

                // StreamsStreamIdGet - GET /streams/{stream_id}
                hyper::Method::GET if path.matched(paths::ID_STREAMS_STREAM_ID) => {
                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_STREAMS_STREAM_ID
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE STREAMS_STREAM_ID in set but failed match against \"{}\"", path, paths::REGEX_STREAMS_STREAM_ID.as_str())
                    );

                    let param_stream_id = match percent_encoding::percent_decode(path_params["stream_id"].as_bytes()).decode_utf8() {
                    Ok(param_stream_id) => match param_stream_id.parse::<String>() {
                        Ok(param_stream_id) => param_stream_id,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter stream_id: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["stream_id"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let result = api_impl
                        .streams_stream_id_get(param_stream_id, &context)
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
                            StreamsStreamIdGetResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for STREAMS_STREAM_ID_GET_SUCCESS"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            StreamsStreamIdGetResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for STREAMS_STREAM_ID_GET_BAD_REQUEST"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            StreamsStreamIdGetResponse::StreamNotFound(body) => {
                                *response.status_mut() = StatusCode::from_u16(404)
                                    .expect("Unable to turn 404 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("text/plain")
                                                            .expect("Unable to create Content-Type header for STREAMS_STREAM_ID_GET_STREAM_NOT_FOUND"));
                                let body_content = body;
                                *response.body_mut() = Body::from(body_content);
                            }
                            StreamsStreamIdGetResponse::InternalServerError(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 500 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for STREAMS_STREAM_ID_GET_INTERNAL_SERVER_ERROR"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
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

                // StreamsStreamIdOptions - OPTIONS /streams/{stream_id}
                hyper::Method::OPTIONS if path.matched(paths::ID_STREAMS_STREAM_ID) => {
                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_STREAMS_STREAM_ID
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE STREAMS_STREAM_ID in set but failed match against \"{}\"", path, paths::REGEX_STREAMS_STREAM_ID.as_str())
                    );

                    let param_stream_id = match percent_encoding::percent_decode(path_params["stream_id"].as_bytes()).decode_utf8() {
                    Ok(param_stream_id) => match param_stream_id.parse::<String>() {
                        Ok(param_stream_id) => param_stream_id,
                        Err(e) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't parse path parameter stream_id: {}", e)))
                                        .expect("Unable to create Bad Request response for invalid path parameter")),
                    },
                    Err(_) => return Ok(Response::builder()
                                        .status(StatusCode::BAD_REQUEST)
                                        .body(Body::from(format!("Couldn't percent-decode path parameter as UTF-8: {}", &path_params["stream_id"])))
                                        .expect("Unable to create Bad Request response for invalid percent decode"))
                };

                    let result = api_impl
                        .streams_stream_id_options(param_stream_id, &context)
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
                            StreamsStreamIdOptionsResponse::Cors => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
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

                // VersionGet - GET /version
                hyper::Method::GET if path.matched(paths::ID_VERSION) => {
                    let result = api_impl.version_get(&context).await;
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
                            VersionGetResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for VERSION_GET_SUCCESS"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            VersionGetResponse::InternalServerError(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 500 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for VERSION_GET_INTERNAL_SERVER_ERROR"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
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

                // VersionOptions - OPTIONS /version
                hyper::Method::OPTIONS if path.matched(paths::ID_VERSION) => {
                    let result = api_impl.version_options(&context).await;
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
                            VersionOptionsResponse::Cors => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
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
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            VersionPostResponse::InternalServerError(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 500 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for VERSION_POST_INTERNAL_SERVER_ERROR"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
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

                _ if path.matched(paths::ID_CONFIG_NETWORK) => method_not_allowed(),
                _ if path.matched(paths::ID_DEBUG_HEAP) => method_not_allowed(),
                _ if path.matched(paths::ID_EVENTS) => method_not_allowed(),
                _ if path.matched(paths::ID_EVENTS_EVENT_ID) => method_not_allowed(),
                _ if path.matched(paths::ID_EXPERIMENTAL_EVENTS_SEP_SEPVALUE) => {
                    method_not_allowed()
                }
                _ if path.matched(paths::ID_EXPERIMENTAL_INTERESTS) => method_not_allowed(),
                _ if path.matched(paths::ID_FEED_EVENTS) => method_not_allowed(),
                _ if path.matched(paths::ID_FEED_RESUMETOKEN) => method_not_allowed(),
                _ if path.matched(paths::ID_INTERESTS) => method_not_allowed(),
                _ if path.matched(paths::ID_INTERESTS_SORT_KEY_SORT_VALUE) => method_not_allowed(),
                _ if path.matched(paths::ID_LIVENESS) => method_not_allowed(),
                _ if path.matched(paths::ID_PEERS) => method_not_allowed(),
                _ if path.matched(paths::ID_STREAMS_STREAM_ID) => method_not_allowed(),
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
            // ConfigNetworkGet - GET /config/network
            hyper::Method::GET if path.matched(paths::ID_CONFIG_NETWORK) => {
                Some("ConfigNetworkGet")
            }
            // ConfigNetworkOptions - OPTIONS /config/network
            hyper::Method::OPTIONS if path.matched(paths::ID_CONFIG_NETWORK) => {
                Some("ConfigNetworkOptions")
            }
            // DebugHeapGet - GET /debug/heap
            hyper::Method::GET if path.matched(paths::ID_DEBUG_HEAP) => Some("DebugHeapGet"),
            // DebugHeapOptions - OPTIONS /debug/heap
            hyper::Method::OPTIONS if path.matched(paths::ID_DEBUG_HEAP) => {
                Some("DebugHeapOptions")
            }
            // EventsEventIdGet - GET /events/{event_id}
            hyper::Method::GET if path.matched(paths::ID_EVENTS_EVENT_ID) => {
                Some("EventsEventIdGet")
            }
            // EventsEventIdOptions - OPTIONS /events/{event_id}
            hyper::Method::OPTIONS if path.matched(paths::ID_EVENTS_EVENT_ID) => {
                Some("EventsEventIdOptions")
            }
            // EventsOptions - OPTIONS /events
            hyper::Method::OPTIONS if path.matched(paths::ID_EVENTS) => Some("EventsOptions"),
            // EventsPost - POST /events
            hyper::Method::POST if path.matched(paths::ID_EVENTS) => Some("EventsPost"),
            // ExperimentalEventsSepSepValueGet - GET /experimental/events/{sep}/{sepValue}
            hyper::Method::GET if path.matched(paths::ID_EXPERIMENTAL_EVENTS_SEP_SEPVALUE) => {
                Some("ExperimentalEventsSepSepValueGet")
            }
            // ExperimentalEventsSepSepValueOptions - OPTIONS /experimental/events/{sep}/{sepValue}
            hyper::Method::OPTIONS if path.matched(paths::ID_EXPERIMENTAL_EVENTS_SEP_SEPVALUE) => {
                Some("ExperimentalEventsSepSepValueOptions")
            }
            // ExperimentalInterestsGet - GET /experimental/interests
            hyper::Method::GET if path.matched(paths::ID_EXPERIMENTAL_INTERESTS) => {
                Some("ExperimentalInterestsGet")
            }
            // ExperimentalInterestsOptions - OPTIONS /experimental/interests
            hyper::Method::OPTIONS if path.matched(paths::ID_EXPERIMENTAL_INTERESTS) => {
                Some("ExperimentalInterestsOptions")
            }
            // FeedEventsGet - GET /feed/events
            hyper::Method::GET if path.matched(paths::ID_FEED_EVENTS) => Some("FeedEventsGet"),
            // FeedEventsOptions - OPTIONS /feed/events
            hyper::Method::OPTIONS if path.matched(paths::ID_FEED_EVENTS) => {
                Some("FeedEventsOptions")
            }
            // FeedResumeTokenGet - GET /feed/resumeToken
            hyper::Method::GET if path.matched(paths::ID_FEED_RESUMETOKEN) => {
                Some("FeedResumeTokenGet")
            }
            // FeedResumeTokenOptions - OPTIONS /feed/resumeToken
            hyper::Method::OPTIONS if path.matched(paths::ID_FEED_RESUMETOKEN) => {
                Some("FeedResumeTokenOptions")
            }
            // InterestsOptions - OPTIONS /interests
            hyper::Method::OPTIONS if path.matched(paths::ID_INTERESTS) => Some("InterestsOptions"),
            // InterestsPost - POST /interests
            hyper::Method::POST if path.matched(paths::ID_INTERESTS) => Some("InterestsPost"),
            // InterestsSortKeySortValueOptions - OPTIONS /interests/{sort_key}/{sort_value}
            hyper::Method::OPTIONS if path.matched(paths::ID_INTERESTS_SORT_KEY_SORT_VALUE) => {
                Some("InterestsSortKeySortValueOptions")
            }
            // InterestsSortKeySortValuePost - POST /interests/{sort_key}/{sort_value}
            hyper::Method::POST if path.matched(paths::ID_INTERESTS_SORT_KEY_SORT_VALUE) => {
                Some("InterestsSortKeySortValuePost")
            }
            // LivenessGet - GET /liveness
            hyper::Method::GET if path.matched(paths::ID_LIVENESS) => Some("LivenessGet"),
            // LivenessOptions - OPTIONS /liveness
            hyper::Method::OPTIONS if path.matched(paths::ID_LIVENESS) => Some("LivenessOptions"),
            // PeersGet - GET /peers
            hyper::Method::GET if path.matched(paths::ID_PEERS) => Some("PeersGet"),
            // PeersOptions - OPTIONS /peers
            hyper::Method::OPTIONS if path.matched(paths::ID_PEERS) => Some("PeersOptions"),
            // PeersPost - POST /peers
            hyper::Method::POST if path.matched(paths::ID_PEERS) => Some("PeersPost"),
            // StreamsStreamIdGet - GET /streams/{stream_id}
            hyper::Method::GET if path.matched(paths::ID_STREAMS_STREAM_ID) => {
                Some("StreamsStreamIdGet")
            }
            // StreamsStreamIdOptions - OPTIONS /streams/{stream_id}
            hyper::Method::OPTIONS if path.matched(paths::ID_STREAMS_STREAM_ID) => {
                Some("StreamsStreamIdOptions")
            }
            // VersionGet - GET /version
            hyper::Method::GET if path.matched(paths::ID_VERSION) => Some("VersionGet"),
            // VersionOptions - OPTIONS /version
            hyper::Method::OPTIONS if path.matched(paths::ID_VERSION) => Some("VersionOptions"),
            // VersionPost - POST /version
            hyper::Method::POST if path.matched(paths::ID_VERSION) => Some("VersionPost"),
            _ => None,
        }
    }
}
