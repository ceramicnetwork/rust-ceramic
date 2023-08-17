use std::{
    error::Error,
    task::{Context, Poll},
};

use futures::{future::BoxFuture, Future};
use hyper::{service::Service, Body, Request, Response, StatusCode};

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
