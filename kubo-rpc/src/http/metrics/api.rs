use async_trait::async_trait;
use ceramic_kubo_rpc_server::{
    models, Api, BlockGetPostResponse, BlockStatPostResponse, DagGetPostResponse,
    DagResolvePostResponse, IdPostResponse, PinAddPostResponse, PinRmPostResponse,
    SwarmConnectPostResponse, SwarmPeersPostResponse, VersionPostResponse,
};
use ceramic_metrics::Recorder;
use futures_util::Future;
use swagger::ApiError;
use tokio::time::Instant;

use crate::http::{metrics::Event, Metrics};

/// Implement the API and record metrics
#[derive(Clone)]
pub struct MetricsMiddleware<A: Clone> {
    api: A,
    metrics: Metrics,
}

impl<A: Clone> MetricsMiddleware<A> {
    /// Construct a new MetricsMiddleware.
    /// The metrics should have already be registered.
    pub fn new(api: A, metrics: Metrics) -> Self {
        Self { api, metrics }
    }
    // Record metrics for a given API endpoint
    async fn record<T>(&self, path: &'static str, fut: impl Future<Output = T>) -> T {
        let start = Instant::now();
        let ret = fut.await;
        let duration = start.elapsed();
        let event = Event { path, duration };
        self.metrics.record(&event);
        ret
    }
}

#[async_trait]
impl<A, C> Api<C> for MetricsMiddleware<A>
where
    A: Api<C>,
    A: Clone + Send + Sync,
    C: Send + Sync,
{
    /// Get a single IPFS block
    async fn block_get_post(
        &self,
        arg: String,
        timeout: Option<String>,
        offline: Option<bool>,
        context: &C,
    ) -> Result<BlockGetPostResponse, ApiError> {
        self.record(
            "/block/get",
            self.api.block_get_post(arg, timeout, offline, context),
        )
        .await
    }

    /// Report statistics about a block
    async fn block_stat_post(
        &self,
        arg: String,
        context: &C,
    ) -> Result<BlockStatPostResponse, ApiError> {
        self.record("/block/stat", self.api.block_stat_post(arg, context))
            .await
    }

    /// Get an IPLD node from IPFS
    async fn dag_get_post(
        &self,
        arg: String,
        output_codec: Option<models::Codecs>,
        context: &C,
    ) -> Result<DagGetPostResponse, ApiError> {
        self.record(
            "/dag/get",
            self.api.dag_get_post(arg, output_codec, context),
        )
        .await
    }

    /// Resolve an IPFS path to a DAG node
    async fn dag_resolve_post(
        &self,
        arg: String,
        context: &C,
    ) -> Result<DagResolvePostResponse, ApiError> {
        self.record("/dag/resolve", self.api.dag_resolve_post(arg, context))
            .await
    }

    /// Report identifying information about a node
    async fn id_post(&self, arg: Option<String>, context: &C) -> Result<IdPostResponse, ApiError> {
        self.record("/id", self.api.id_post(arg, context)).await
    }

    /// Add a block to the pin store
    async fn pin_add_post(
        &self,
        arg: String,
        recursive: Option<bool>,
        progress: Option<bool>,
        context: &C,
    ) -> Result<PinAddPostResponse, ApiError> {
        self.record(
            "/pin/add",
            self.api.pin_add_post(arg, recursive, progress, context),
        )
        .await
    }

    /// Remove a block from the pin store
    async fn pin_rm_post(&self, arg: String, context: &C) -> Result<PinRmPostResponse, ApiError> {
        self.record("/pin/rm", self.api.pin_rm_post(arg, context))
            .await
    }

    /// Connect to peers
    async fn swarm_connect_post(
        &self,
        arg: &Vec<String>,
        context: &C,
    ) -> Result<SwarmConnectPostResponse, ApiError> {
        self.record("/swarm/connect", self.api.swarm_connect_post(arg, context))
            .await
    }

    /// Report connected peers
    async fn swarm_peers_post(&self, context: &C) -> Result<SwarmPeersPostResponse, ApiError> {
        self.record("/swarm/peers", self.api.swarm_peers_post(context))
            .await
    }

    /// Report server version
    async fn version_post(&self, context: &C) -> Result<VersionPostResponse, ApiError> {
        self.record("/version", self.api.version_post(context))
            .await
    }
}
