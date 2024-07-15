use std::{
    collections::HashSet,
    error::Error,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use ceramic_metrics::Recorder;
use futures::{future::BoxFuture, Future};
use hyper::{header::HeaderValue, service::Service, Body, Request, Response, StatusCode};

use crate::http_metrics::{Event, Metrics};

type ServiceError = Box<dyn Error + Send + Sync + 'static>;
type ServiceFuture = BoxFuture<'static, Result<Response<Body>, ServiceError>>;

/// Compose two services into a single service where each service handles requests with different path prefixes.
pub struct MakePrefixService<A, B> {
    a: (String, A),
    b: (String, B),
}

impl<A, B> MakePrefixService<A, B> {
    /// Construct a service composed of other services where each handles different path prefixes.
    pub fn new(a: (String, A), b: (String, B)) -> Self {
        MakePrefixService { a, b }
    }
}

impl<
        A,
        B,
        Target,
        AResponse,
        AError,
        AMakeError,
        AFuture,
        BResponse,
        BError,
        BMakeError,
        BFuture,
    > Service<Target> for MakePrefixService<A, B>
where
    A: for<'a> Service<&'a Target, Response = AResponse, Error = AMakeError, Future = AFuture>,
    AResponse: Service<Request<Body>, Response = Response<Body>> + Send,
    AMakeError: Into<Box<dyn std::error::Error + Send + Sync>>,
    AError: Into<Box<dyn std::error::Error + Send + Sync>>,
    AFuture: Future<Output = Result<AResponse, AError>> + Send + 'static,

    B: for<'b> Service<&'b Target, Response = BResponse, Error = BMakeError, Future = BFuture>,
    BResponse: Service<Request<Body>, Response = Response<Body>> + Send,
    BMakeError: Into<Box<dyn std::error::Error + Send + Sync>>,
    BError: Into<Box<dyn std::error::Error + Send + Sync>>,
    BFuture: Future<Output = Result<BResponse, BError>> + Send + 'static,
{
    type Response = PrefixService<AResponse, BResponse>;
    type Error = ServiceError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // only report we are ready if both services are ready
        match self.a.1.poll_ready(cx) {
            std::task::Poll::Ready(_) => self.b.1.poll_ready(cx).map_err(|err| err.into()),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn call(&mut self, target: Target) -> Self::Future {
        // Make service for each path
        let a_prefix = self.a.0.clone();
        let b_prefix = self.b.0.clone();

        let a_service = self.a.1.call(&target);
        let b_service = self.b.1.call(&target);

        Box::pin(async move {
            let a_service = a_service.await.map_err(|err| err.into())?;
            let b_service = b_service.await.map_err(|err| err.into())?;
            Ok(PrefixService::new(
                (a_prefix, a_service),
                (b_prefix, b_service),
            ))
        })
    }
}

pub struct PrefixService<A, B> {
    a: (String, A),
    b: (String, B),
}
impl<A, B> PrefixService<A, B> {
    pub fn new(a: (String, A), b: (String, B)) -> Self {
        Self { a, b }
    }
}

impl<C, K> Service<Request<Body>> for PrefixService<C, K>
where
    C: Service<Request<Body>, Error = ServiceError, Future = ServiceFuture>,
    K: Service<Request<Body>, Error = ServiceError, Future = ServiceFuture>,
{
    type Response = Response<Body>;

    type Error = ServiceError;

    type Future = ServiceFuture;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        // only report we are ready if both services are ready
        match self.a.1.poll_ready(cx) {
            std::task::Poll::Ready(_) => self.b.1.poll_ready(cx),
            p @ std::task::Poll::Pending => p,
        }
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        if req.uri().path().starts_with(&self.a.0) {
            self.a.1.call(req)
        } else if req.uri().path().starts_with(&self.b.0) {
            self.b.1.call(req)
        } else {
            Box::pin(async move {
                Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .map_err(|err| err.into())
            })
        }
    }
}

pub struct CorsService<T> {
    default_allow_origin: HeaderValue,
    allow_methods: HeaderValue,
    allow_headers: HeaderValue,
    allowed_origins: Arc<HashSet<Vec<u8>>>,
    inner: T,
}

impl<T> CorsService<T> {
    fn new(
        inner: T,
        default_allow_origin: HeaderValue,
        allow_methods: HeaderValue,
        allow_headers: HeaderValue,
        allowed_origins: Arc<HashSet<Vec<u8>>>,
    ) -> Self {
        Self {
            default_allow_origin,
            allow_methods,
            allow_headers,
            allowed_origins,
            inner,
        }
    }
}

impl<T> Service<Request<Body>> for CorsService<T>
where
    T: Service<Request<Body>, Error = ServiceError, Future = ServiceFuture> + Send + Sync + 'static,
{
    type Response = Response<Body>;
    type Error = ServiceError;
    type Future = ServiceFuture;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let allow_methods = self.allow_methods.clone();
        let allow_headers = self.allow_headers.clone();
        // you can only respond with a single origin or *, so we just echo it back if it's in our list
        let allow_origin = if let Some(requested_origin) = req.headers().get("Origin") {
            if self.allowed_origins.contains(requested_origin.as_bytes()) {
                requested_origin.to_owned()
            } else {
                // will fail cors check
                self.default_allow_origin.clone()
            }
        } else {
            self.default_allow_origin.clone()
        };
        let fut = self.inner.call(req);

        Box::pin(async move {
            match fut.await {
                Ok(mut response) => {
                    let headers = response.headers_mut();
                    headers.insert("Access-Control-Allow-Origin", allow_origin);
                    headers.insert("Access-Control-Allow-Methods", allow_methods);
                    headers.insert("Access-Control-Allow-Headers", allow_headers);
                    Ok(response)
                }
                Err(err) => Err(err),
            }
        })
    }
}

pub struct MakeCorsService<T> {
    inner: T,
    origins: Arc<HashSet<Vec<u8>>>,
    default_allow_origin: HeaderValue,
    allow_methods: HeaderValue,
    allow_headers: HeaderValue,
}

impl<T> MakeCorsService<T> {
    /// Currently only the first value is used
    pub fn new(inner: T, origins: Vec<String>) -> Self {
        let default_allow_origin = origins.first().map(|o| o.as_bytes()).unwrap_or(b"*");
        let origins = Arc::new(
            origins
                .iter()
                .map(|o| o.as_bytes().to_vec())
                .collect::<HashSet<Vec<u8>>>(),
        );
        Self {
            inner,
            origins,
            default_allow_origin: HeaderValue::from_bytes(default_allow_origin).unwrap(),
            // this should be per route, but open API doesn't support generating shared responses very well
            // so for now we just return everything and it may error if the route doesn't support it
            // we currently don't have any PUT or DELETE but including in case we add them
            allow_methods: HeaderValue::from_bytes(b"POST, GET, OPTIONS, DELETE, PUT").unwrap(),
            // need to add sensitive headers to this list
            allow_headers: HeaderValue::from_bytes(b"*").unwrap(),
        }
    }
}

impl<A, Target, AResponse, AError, AMakeError, AFuture> Service<Target> for MakeCorsService<A>
where
    A: for<'a> Service<&'a Target, Response = AResponse, Error = AMakeError, Future = AFuture>,
    AResponse: Service<Request<Body>, Response = Response<Body>> + Send,
    AMakeError: Into<Box<dyn std::error::Error + Send + Sync>>,
    AError: Into<Box<dyn std::error::Error + Send + Sync>>,
    AFuture: Future<Output = Result<AResponse, AError>> + Send + 'static,
{
    type Response = CorsService<AResponse>;
    type Error = ServiceError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|err| err.into())
    }

    fn call(&mut self, target: Target) -> Self::Future {
        let target = self.inner.call(&target);
        let allow_origin = self.default_allow_origin.clone();
        let allow_headers = self.allow_headers.clone();
        let allow_methods = self.allow_methods.clone();
        let allowed_origins = self.origins.clone();
        Box::pin(async move {
            let inner = target.await.map_err(|err| err.into())?;
            Ok(CorsService::new(
                inner,
                allow_origin,
                allow_methods,
                allow_headers,
                allowed_origins,
            ))
        })
    }
}

pub struct MetricsService<T> {
    inner: T,
    metrics: Arc<Metrics>,
}

impl<T> MetricsService<T> {
    pub fn new(inner: T, metrics: Arc<Metrics>) -> Self {
        MetricsService { inner, metrics }
    }
}

impl<T> Service<Request<Body>> for MetricsService<T>
where
    T: Service<Request<Body>, Error = ServiceError, Future = ServiceFuture> + Send + Sync + 'static,
{
    type Response = Response<Body>;
    type Error = ServiceError;
    type Future = ServiceFuture;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let start_time = Instant::now();
        let method = req.method().clone();
        let path = req.uri().path().to_string();

        let fut = self.inner.call(req);

        let metrics = self.metrics.clone();
        Box::pin(async move {
            let response = fut.await;
            let elapsed = start_time.elapsed();
            let status = response
                .as_ref()
                .map(|res| res.status())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

            metrics.record(&reduce_prom_cardinality(Event {
                method: method.to_string(),
                path,
                duration: elapsed,
                status_code: status.as_u16(),
            }));

            response
        })
    }
}

pub struct MakeMetricsService<T> {
    inner: T,
    metrics: Arc<Metrics>,
}

impl<T> MakeMetricsService<T> {
    pub fn new(inner: T, metrics: Arc<Metrics>) -> Self {
        Self { inner, metrics }
    }
}

impl<A, Target, AResponse, AError, AMakeError, AFuture> Service<Target> for MakeMetricsService<A>
where
    A: for<'a> Service<&'a Target, Response = AResponse, Error = AMakeError, Future = AFuture>,
    AResponse: Service<Request<Body>, Response = Response<Body>> + Send,
    AMakeError: Into<Box<dyn std::error::Error + Send + Sync>>,
    AError: Into<Box<dyn std::error::Error + Send + Sync>>,
    AFuture: Future<Output = Result<AResponse, AError>> + Send + 'static,
{
    type Response = MetricsService<AResponse>;
    type Error = ServiceError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|err| err.into())
    }

    fn call(&mut self, target: Target) -> Self::Future {
        let target = self.inner.call(&target);
        let metrics = self.metrics.clone();
        Box::pin(async move {
            let inner = target.await.map_err(|err| err.into())?;
            Ok(MetricsService::new(inner, metrics))
        })
    }
}

/// Replace certain variable paths with a more general path to reduce prometheus cardinality
/// Without this, we could end up with 1000s (or millions) of unique paths in prometheus
fn reduce_prom_cardinality(mut event: Event) -> Event {
    // split('/') returns the number of '/' + 1
    match event.method.as_str() {
        "GET" => {
            if event
                .path
                .strip_prefix("/ceramic/experimental/events/")
                .is_some()
                && event.path.split('/').count() == 5
            {
                event.path = "/ceramic/experimental/events/{event_id}".to_string();
            } else if event.path.strip_prefix("/ceramic/events/").is_some()
                && event.path.split('/').count() == 4
            {
                event.path = "/ceramic/events/{event_id}".to_string();
            } else if event
                .path
                .strip_prefix("/ceramic/experimental/events/")
                .is_some()
                && event.path.split('/').count() == 6
            {
                event.path = "/ceramic/experimental/events/{sep_key}/{sep_value}".to_string();
            }
        }
        "POST" => {
            // It might be worth leaving this in prom since we shouldn't have _that_ many interests
            // and it's useful for debugging, but we could just log an info message or something.
            if event.path.strip_prefix("/ceramic/interests/").is_some()
                && event.path.split('/').count() == 5
            {
                event.path = "/ceramic/interests/{sep_key}/{sep_value}".to_string();
            }
        }
        _ => {}
    }
    event
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn expected_prom_cardinality_replacement() {
        let experimental = reduce_prom_cardinality(Event {
            method: "GET".to_string(),
            path: "/ceramic/experimental/events/1234".to_string(),
            status_code: 200,
            duration: std::time::Duration::from_secs(1),
        });
        assert_eq!(
            &experimental.path,
            "/ceramic/experimental/events/{event_id}"
        );

        let get_event = reduce_prom_cardinality(Event {
            method: "GET".to_string(),
            path: "/ceramic/events/1234".to_string(),
            status_code: 200,
            duration: std::time::Duration::from_secs(1),
        });
        assert_eq!(&get_event.path, "/ceramic/events/{event_id}");

        let experimental_get = reduce_prom_cardinality(Event {
            method: "GET".to_string(),
            path: "/ceramic/experimental/events/model/k2t6wz4z9kggnsqejudguto4u2wqbepja581hy1dsfs16ltohp1ncxs8d1rbvr".to_string(),
            status_code: 200,
            duration: std::time::Duration::from_secs(1),
        });
        assert_eq!(
            &experimental_get.path,
            "/ceramic/experimental/events/{sep_key}/{sep_value}"
        );

        let interest = reduce_prom_cardinality(Event {
            method: "POST".to_string(),
            path: "/ceramic/interests/model/1234".to_string(),
            status_code: 200,
            duration: std::time::Duration::from_secs(1),
        });
        assert_eq!(&interest.path, "/ceramic/interests/{sep_key}/{sep_value}");
    }

    #[test]
    fn should_not_replace_wrong_method() {
        let experimental = reduce_prom_cardinality(Event {
            method: "POST".to_string(),
            path: "/ceramic/experimental/events/1234".to_string(),
            status_code: 200,
            duration: std::time::Duration::from_secs(1),
        });
        assert_ne!(
            &experimental.path,
            "/ceramic/experimental/events/{event_id}"
        );

        let get_event = reduce_prom_cardinality(Event {
            method: "POST".to_string(),
            path: "/ceramic/events/1234".to_string(),
            status_code: 200,
            duration: std::time::Duration::from_secs(1),
        });
        assert_ne!(&get_event.path, "/ceramic/events/{event_id}");

        let experimental_get = reduce_prom_cardinality(Event {
            method: "POST".to_string(),
            path: "/ceramic/experimental/events/model/k2t6wz4z9kggnsqejudguto4u2wqbepja581hy1dsfs16ltohp1ncxs8d1rbvr".to_string(),
            status_code: 200,
            duration: std::time::Duration::from_secs(1),
        });
        assert_ne!(
            &experimental_get.path,
            "/ceramic/experimental/events/{sep_key}/{sep_value}"
        );

        let interest = reduce_prom_cardinality(Event {
            method: "GET".to_string(),
            path: "/ceramic/interests/model/1234".to_string(),
            status_code: 200,
            duration: std::time::Duration::from_secs(1),
        });
        assert_ne!(&interest.path, "/ceramic/interests/{sep_key}/{sep_value}");
    }

    #[test]
    fn should_not_replace_extra_parts() {
        let experimental = reduce_prom_cardinality(Event {
            method: "GET".to_string(),
            path: "/ceramic/experimental/events/1234/hello".to_string(),
            status_code: 200,
            duration: std::time::Duration::from_secs(1),
        });
        assert_ne!(
            &experimental.path,
            "/ceramic/experimental/events/{event_id}"
        );

        let get_event = reduce_prom_cardinality(Event {
            method: "GET".to_string(),
            path: "/ceramic/events/1234/resources".to_string(),
            status_code: 200,
            duration: std::time::Duration::from_secs(1),
        });
        assert_ne!(&get_event.path, "/ceramic/events/{event_id}");

        let experimental_get = reduce_prom_cardinality(Event {
            method: "POST".to_string(),
            path: "/ceramic/experimental/events/model/k2t6wz4z9kggnsqejudguto4u2wqbepja581hy1dsfs16ltohp1ncxs8d1rbvr/123".to_string(),
            status_code: 200,
            duration: std::time::Duration::from_secs(1),
        });
        assert_ne!(
            &experimental_get.path,
            "/ceramic/experimental/events/{sep_key}/{sep_value}"
        );

        let interest = reduce_prom_cardinality(Event {
            method: "POST".to_string(),
            path: "/ceramic/interests/model/1234/strip".to_string(),
            status_code: 200,
            duration: std::time::Duration::from_secs(1),
        });
        assert_ne!(&interest.path, "/ceramic/interests/{sep_key}/{sep_value}");
    }
}
