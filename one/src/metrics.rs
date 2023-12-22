use std::{convert::Infallible, net::SocketAddr};

use anyhow::Result;
use hyper::{
    http::HeaderValue,
    service::{make_service_fn, service_fn},
    Body, Request, Response,
};
use prometheus_client::{encoding::EncodeLabelSet, metrics::info::Info, registry::Registry};
use tokio::{sync::oneshot, task::JoinHandle};

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct InfoLabels {
    service_name: String,
    version: String,
    build: String,
    instance_id: String,
    exe_hash: String,
}

impl From<crate::Info> for InfoLabels {
    fn from(info: crate::Info) -> Self {
        Self {
            service_name: info.service_name,
            version: info.version,
            build: info.build,
            instance_id: info.instance_id,
            exe_hash: info.exe_hash,
        }
    }
}

pub struct Metrics {}

impl Metrics {
    pub fn register(info: crate::Info, registry: &mut Registry) -> Self {
        let sub_registry = registry.sub_registry_with_prefix("ceramic");

        let info: Info<InfoLabels> = Info::new(info.into());
        sub_registry.register("one", "Information about the ceramic-one process", info);

        Self {}
    }
}

async fn handle(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let data = ceramic_metrics::MetricsHandle::encode();
    let mut resp = Response::new(Body::from(data));
    resp.headers_mut().insert(
        "Content-Type",
        // Use OpenMetrics content type so prometheus knows to parse it accordingly
        HeaderValue::from_static("application/openmetrics-text"),
    );
    Ok(resp)
}

/// Start metrics server.
/// Sending on the returned channel will cause the server to shutdown gracefully.
pub fn start(addr: &SocketAddr) -> (oneshot::Sender<()>, JoinHandle<Result<(), hyper::Error>>) {
    let (tx, rx) = oneshot::channel::<()>();
    let service = make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(handle)) });

    let server = hyper::Server::bind(addr)
        .serve(service)
        .with_graceful_shutdown(async {
            rx.await.ok();
        });
    (tx, tokio::spawn(server))
}
