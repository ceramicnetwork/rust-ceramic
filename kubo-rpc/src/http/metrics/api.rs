use async_trait::async_trait;
use ceramic_kubo_rpc_server::{
    models, Api, BlockGetPostResponse, BlockPutPostResponse, BlockStatPostResponse,
    DagGetPostResponse, DagImportPostResponse, DagPutPostResponse, DagResolvePostResponse,
    IdPostResponse, PinAddPostResponse, PinRmPostResponse, PubsubLsPostResponse,
    PubsubPubPostResponse, PubsubSubPostResponse, SwarmConnectPostResponse, SwarmPeersPostResponse,
    VersionPostResponse,
};
use ceramic_metrics::Recorder;
use futures_util::Future;
use swagger::{ApiError, ByteArray};
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

    /// Put a single IPFS block
    async fn block_put_post(
        &self,
        file: ByteArray,
        cid_codec: Option<models::Codecs>,
        mhtype: Option<models::Multihash>,
        pin: Option<bool>,
        context: &C,
    ) -> Result<BlockPutPostResponse, ApiError> {
        self.record(
            "/block/put",
            self.api
                .block_put_post(file, cid_codec, mhtype, pin, context),
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

    /// Import a CAR file of IPLD nodes into IPFS
    async fn dag_import_post(
        &self,
        file: ByteArray,
        context: &C,
    ) -> Result<DagImportPostResponse, ApiError> {
        self.record("/dag/import", self.api.dag_import_post(file, context))
            .await
    }

    /// Put an IPLD node into IPFS
    async fn dag_put_post(
        &self,
        file: ByteArray,
        store_codec: Option<models::Codecs>,
        input_codec: Option<models::Codecs>,
        context: &C,
    ) -> Result<DagPutPostResponse, ApiError> {
        self.record(
            "/dag/put",
            self.api
                .dag_put_post(file, store_codec, input_codec, context),
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

    /// List topic with active subscriptions
    async fn pubsub_ls_post(&self, context: &C) -> Result<PubsubLsPostResponse, ApiError> {
        self.record("/pubsub/ls", self.api.pubsub_ls_post(context))
            .await
    }

    /// Publish a message to a topic
    async fn pubsub_pub_post(
        &self,
        arg: String,
        file: ByteArray,
        context: &C,
    ) -> Result<PubsubPubPostResponse, ApiError> {
        self.record("/pubsub/pub", self.api.pubsub_pub_post(arg, file, context))
            .await
    }

    /// Subscribe to a topic, blocks until a message is received
    async fn pubsub_sub_post(
        &self,
        arg: String,
        context: &C,
    ) -> Result<PubsubSubPostResponse, ApiError> {
        self.record("/pubsub/sub", self.api.pubsub_sub_post(arg, context))
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
