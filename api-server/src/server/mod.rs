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
    Api, DebugHeapGetResponse, EventsEventIdGetResponse, EventsPostResponse,
    EventsSortKeySortValueGetResponse, ExperimentalEventsSepSepValueGetResponse,
    FeedEventsGetResponse, InterestsPostResponse, InterestsSortKeySortValuePostResponse,
    LivenessGetResponse, VersionPostResponse,
};

mod paths {
    use lazy_static::lazy_static;

    lazy_static! {
        pub static ref GLOBAL_REGEX_SET: regex::RegexSet = regex::RegexSet::new(vec![
            r"^/ceramic/debug/heap$",
            r"^/ceramic/events$",
            r"^/ceramic/events/(?P<event_id>[^/?#]*)$",
            r"^/ceramic/events/(?P<sort_key>[^/?#]*)/(?P<sort_value>[^/?#]*)$",
            r"^/ceramic/experimental/events/(?P<sep>[^/?#]*)/(?P<sepValue>[^/?#]*)$",
            r"^/ceramic/feed/events$",
            r"^/ceramic/interests$",
            r"^/ceramic/interests/(?P<sort_key>[^/?#]*)/(?P<sort_value>[^/?#]*)$",
            r"^/ceramic/liveness$",
            r"^/ceramic/version$"
        ])
        .expect("Unable to create global regex set");
    }
    pub(crate) static ID_DEBUG_HEAP: usize = 0;
    pub(crate) static ID_EVENTS: usize = 1;
    pub(crate) static ID_EVENTS_EVENT_ID: usize = 2;
    lazy_static! {
        pub static ref REGEX_EVENTS_EVENT_ID: regex::Regex =
            #[allow(clippy::invalid_regex)]
            regex::Regex::new(r"^/ceramic/events/(?P<event_id>[^/?#]*)$")
                .expect("Unable to create regex for EVENTS_EVENT_ID");
    }
    pub(crate) static ID_EVENTS_SORT_KEY_SORT_VALUE: usize = 3;
    lazy_static! {
        pub static ref REGEX_EVENTS_SORT_KEY_SORT_VALUE: regex::Regex =
            #[allow(clippy::invalid_regex)]
            regex::Regex::new(r"^/ceramic/events/(?P<sort_key>[^/?#]*)/(?P<sort_value>[^/?#]*)$")
                .expect("Unable to create regex for EVENTS_SORT_KEY_SORT_VALUE");
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
    pub(crate) static ID_FEED_EVENTS: usize = 5;
    pub(crate) static ID_INTERESTS: usize = 6;
    pub(crate) static ID_INTERESTS_SORT_KEY_SORT_VALUE: usize = 7;
    lazy_static! {
        pub static ref REGEX_INTERESTS_SORT_KEY_SORT_VALUE: regex::Regex =
            #[allow(clippy::invalid_regex)]
            regex::Regex::new(
                r"^/ceramic/interests/(?P<sort_key>[^/?#]*)/(?P<sort_value>[^/?#]*)$"
            )
            .expect("Unable to create regex for INTERESTS_SORT_KEY_SORT_VALUE");
    }
    pub(crate) static ID_LIVENESS: usize = 8;
    pub(crate) static ID_VERSION: usize = 9;
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

                // EventsPost - POST /events
                hyper::Method::POST if path.matched(paths::ID_EVENTS) => {
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

                                let result = api_impl.events_post(
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
                                                .body(Body::from(format!("Couldn't read body parameter Event: {}", e)))
                                                .expect("Unable to create Bad Request response due to unable to read body parameter Event")),
                        }
                }

                // EventsSortKeySortValueGet - GET /events/{sort_key}/{sort_value}
                hyper::Method::GET if path.matched(paths::ID_EVENTS_SORT_KEY_SORT_VALUE) => {
                    // Path parameters
                    let path: &str = uri.path();
                    let path_params =
                    paths::REGEX_EVENTS_SORT_KEY_SORT_VALUE
                    .captures(path)
                    .unwrap_or_else(||
                        panic!("Path {} matched RE EVENTS_SORT_KEY_SORT_VALUE in set but failed match against \"{}\"", path, paths::REGEX_EVENTS_SORT_KEY_SORT_VALUE.as_str())
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
                        .events_sort_key_sort_value_get(
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
                            EventsSortKeySortValueGetResponse::Success(body) => {
                                *response.status_mut() = StatusCode::from_u16(200)
                                    .expect("Unable to turn 200 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for EVENTS_SORT_KEY_SORT_VALUE_GET_SUCCESS"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            EventsSortKeySortValueGetResponse::BadRequest(body) => {
                                *response.status_mut() = StatusCode::from_u16(400)
                                    .expect("Unable to turn 400 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for EVENTS_SORT_KEY_SORT_VALUE_GET_BAD_REQUEST"));
                                let body_content = serde_json::to_string(&body)
                                    .expect("impossible to fail to serialize");
                                *response.body_mut() = Body::from(body_content);
                            }
                            EventsSortKeySortValueGetResponse::InternalServerError(body) => {
                                *response.status_mut() = StatusCode::from_u16(500)
                                    .expect("Unable to turn 500 into a StatusCode");
                                response.headers_mut().insert(
                                                        CONTENT_TYPE,
                                                        HeaderValue::from_str("application/json")
                                                            .expect("Unable to create Content-Type header for EVENTS_SORT_KEY_SORT_VALUE_GET_INTERNAL_SERVER_ERROR"));
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

                    let result = api_impl
                        .feed_events_get(param_resume_at, param_limit, &context)
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

                _ if path.matched(paths::ID_DEBUG_HEAP) => method_not_allowed(),
                _ if path.matched(paths::ID_EVENTS) => method_not_allowed(),
                _ if path.matched(paths::ID_EVENTS_EVENT_ID) => method_not_allowed(),
                _ if path.matched(paths::ID_EVENTS_SORT_KEY_SORT_VALUE) => method_not_allowed(),
                _ if path.matched(paths::ID_EXPERIMENTAL_EVENTS_SEP_SEPVALUE) => {
                    method_not_allowed()
                }
                _ if path.matched(paths::ID_FEED_EVENTS) => method_not_allowed(),
                _ if path.matched(paths::ID_INTERESTS) => method_not_allowed(),
                _ if path.matched(paths::ID_INTERESTS_SORT_KEY_SORT_VALUE) => method_not_allowed(),
                _ if path.matched(paths::ID_LIVENESS) => method_not_allowed(),
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
            // DebugHeapGet - GET /debug/heap
            hyper::Method::GET if path.matched(paths::ID_DEBUG_HEAP) => Some("DebugHeapGet"),
            // EventsEventIdGet - GET /events/{event_id}
            hyper::Method::GET if path.matched(paths::ID_EVENTS_EVENT_ID) => {
                Some("EventsEventIdGet")
            }
            // EventsPost - POST /events
            hyper::Method::POST if path.matched(paths::ID_EVENTS) => Some("EventsPost"),
            // EventsSortKeySortValueGet - GET /events/{sort_key}/{sort_value}
            hyper::Method::GET if path.matched(paths::ID_EVENTS_SORT_KEY_SORT_VALUE) => {
                Some("EventsSortKeySortValueGet")
            }
            // ExperimentalEventsSepSepValueGet - GET /experimental/events/{sep}/{sepValue}
            hyper::Method::GET if path.matched(paths::ID_EXPERIMENTAL_EVENTS_SEP_SEPVALUE) => {
                Some("ExperimentalEventsSepSepValueGet")
            }
            // FeedEventsGet - GET /feed/events
            hyper::Method::GET if path.matched(paths::ID_FEED_EVENTS) => Some("FeedEventsGet"),
            // InterestsPost - POST /interests
            hyper::Method::POST if path.matched(paths::ID_INTERESTS) => Some("InterestsPost"),
            // InterestsSortKeySortValuePost - POST /interests/{sort_key}/{sort_value}
            hyper::Method::POST if path.matched(paths::ID_INTERESTS_SORT_KEY_SORT_VALUE) => {
                Some("InterestsSortKeySortValuePost")
            }
            // LivenessGet - GET /liveness
            hyper::Method::GET if path.matched(paths::ID_LIVENESS) => Some("LivenessGet"),
            // VersionPost - POST /version
            hyper::Method::POST if path.matched(paths::ID_VERSION) => Some("VersionPost"),
            _ => None,
        }
    }
}
