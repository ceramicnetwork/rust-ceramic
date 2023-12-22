#![allow(
    missing_docs,
    trivial_casts,
    unused_variables,
    unused_mut,
    unused_imports,
    unused_extern_crates,
    non_camel_case_types
)]
#![allow(unused_imports, unused_attributes)]
#![allow(clippy::derive_partial_eq_without_eq, clippy::disallowed_names)]

use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::task::{Context, Poll};
use swagger::{ApiError, ContextWrapper};

type ServiceError = Box<dyn Error + Send + Sync + 'static>;

pub const BASE_PATH: &str = "/api/v0";
pub const API_VERSION: &str = "0.9.0";

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum BlockGetPostResponse {
    /// success
    Success(swagger::ByteArray),
    /// bad request
    BadRequest(models::Error),
    /// internal error
    InternalError(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum BlockPutPostResponse {
    /// success
    Success(models::BlockPutPost200Response),
    /// bad request
    BadRequest(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum BlockStatPostResponse {
    /// success
    Success(models::BlockPutPost200Response),
    /// bad request
    BadRequest(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum DagGetPostResponse {
    /// success
    Success(swagger::ByteArray),
    /// bad request
    BadRequest(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum DagImportPostResponse {
    /// success
    Success(models::DagImportPost200Response),
    /// bad request
    BadRequest(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum DagPutPostResponse {
    /// success
    Success(models::DagPutPost200Response),
    /// bad request
    BadRequest(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum DagResolvePostResponse {
    /// success
    Success(models::DagResolvePost200Response),
    /// bad request
    BadRequest(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum IdPostResponse {
    /// success
    Success(models::IdPost200Response),
    /// bad request
    BadRequest(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum PinAddPostResponse {
    /// success
    Success(models::PinAddPost200Response),
    /// bad request
    BadRequest(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum PinRmPostResponse {
    /// success
    Success(models::PinAddPost200Response),
    /// bad request
    BadRequest(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum SwarmConnectPostResponse {
    /// success
    Success(models::SwarmConnectPost200Response),
    /// bad request
    BadRequest(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum SwarmPeersPostResponse {
    /// success
    Success(models::SwarmPeersPost200Response),
    /// bad request
    BadRequest(models::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[must_use]
pub enum VersionPostResponse {
    /// success
    Success(models::VersionPost200Response),
    /// bad request
    BadRequest(models::Error),
}

/// API
#[async_trait]
#[allow(clippy::too_many_arguments, clippy::ptr_arg)]
pub trait Api<C: Send + Sync> {
    fn poll_ready(
        &self,
        _cx: &mut Context,
    ) -> Poll<Result<(), Box<dyn Error + Send + Sync + 'static>>> {
        Poll::Ready(Ok(()))
    }

    /// Get a single IPFS block
    async fn block_get_post(
        &self,
        arg: String,
        timeout: Option<String>,
        offline: Option<bool>,
        context: &C,
    ) -> Result<BlockGetPostResponse, ApiError>;

    /// Put a single IPFS block
    async fn block_put_post(
        &self,
        file: swagger::ByteArray,
        cid_codec: Option<models::Codecs>,
        mhtype: Option<models::Multihash>,
        pin: Option<bool>,
        context: &C,
    ) -> Result<BlockPutPostResponse, ApiError>;

    /// Report statistics about a block
    async fn block_stat_post(
        &self,
        arg: String,
        context: &C,
    ) -> Result<BlockStatPostResponse, ApiError>;

    /// Get an IPLD node from IPFS
    async fn dag_get_post(
        &self,
        arg: String,
        output_codec: Option<models::Codecs>,
        context: &C,
    ) -> Result<DagGetPostResponse, ApiError>;

    /// Import a CAR file of IPLD nodes into IPFS
    async fn dag_import_post(
        &self,
        file: swagger::ByteArray,
        context: &C,
    ) -> Result<DagImportPostResponse, ApiError>;

    /// Put an IPLD node into IPFS
    async fn dag_put_post(
        &self,
        file: swagger::ByteArray,
        store_codec: Option<models::Codecs>,
        input_codec: Option<models::Codecs>,
        context: &C,
    ) -> Result<DagPutPostResponse, ApiError>;

    /// Resolve an IPFS path to a DAG node
    async fn dag_resolve_post(
        &self,
        arg: String,
        context: &C,
    ) -> Result<DagResolvePostResponse, ApiError>;

    /// Report identifying information about a node
    async fn id_post(&self, arg: Option<String>, context: &C) -> Result<IdPostResponse, ApiError>;

    /// Add a block to the pin store
    async fn pin_add_post(
        &self,
        arg: String,
        recursive: Option<bool>,
        progress: Option<bool>,
        context: &C,
    ) -> Result<PinAddPostResponse, ApiError>;

    /// Remove a block from the pin store
    async fn pin_rm_post(&self, arg: String, context: &C) -> Result<PinRmPostResponse, ApiError>;

    /// Connect to peers
    async fn swarm_connect_post(
        &self,
        arg: &Vec<String>,
        context: &C,
    ) -> Result<SwarmConnectPostResponse, ApiError>;

    /// Report connected peers
    async fn swarm_peers_post(&self, context: &C) -> Result<SwarmPeersPostResponse, ApiError>;

    /// Report server version
    async fn version_post(&self, context: &C) -> Result<VersionPostResponse, ApiError>;
}

/// API where `Context` isn't passed on every API call
#[async_trait]
#[allow(clippy::too_many_arguments, clippy::ptr_arg)]
pub trait ApiNoContext<C: Send + Sync> {
    fn poll_ready(
        &self,
        _cx: &mut Context,
    ) -> Poll<Result<(), Box<dyn Error + Send + Sync + 'static>>>;

    fn context(&self) -> &C;

    /// Get a single IPFS block
    async fn block_get_post(
        &self,
        arg: String,
        timeout: Option<String>,
        offline: Option<bool>,
    ) -> Result<BlockGetPostResponse, ApiError>;

    /// Put a single IPFS block
    async fn block_put_post(
        &self,
        file: swagger::ByteArray,
        cid_codec: Option<models::Codecs>,
        mhtype: Option<models::Multihash>,
        pin: Option<bool>,
    ) -> Result<BlockPutPostResponse, ApiError>;

    /// Report statistics about a block
    async fn block_stat_post(&self, arg: String) -> Result<BlockStatPostResponse, ApiError>;

    /// Get an IPLD node from IPFS
    async fn dag_get_post(
        &self,
        arg: String,
        output_codec: Option<models::Codecs>,
    ) -> Result<DagGetPostResponse, ApiError>;

    /// Import a CAR file of IPLD nodes into IPFS
    async fn dag_import_post(
        &self,
        file: swagger::ByteArray,
    ) -> Result<DagImportPostResponse, ApiError>;

    /// Put an IPLD node into IPFS
    async fn dag_put_post(
        &self,
        file: swagger::ByteArray,
        store_codec: Option<models::Codecs>,
        input_codec: Option<models::Codecs>,
    ) -> Result<DagPutPostResponse, ApiError>;

    /// Resolve an IPFS path to a DAG node
    async fn dag_resolve_post(&self, arg: String) -> Result<DagResolvePostResponse, ApiError>;

    /// Report identifying information about a node
    async fn id_post(&self, arg: Option<String>) -> Result<IdPostResponse, ApiError>;

    /// Add a block to the pin store
    async fn pin_add_post(
        &self,
        arg: String,
        recursive: Option<bool>,
        progress: Option<bool>,
    ) -> Result<PinAddPostResponse, ApiError>;

    /// Remove a block from the pin store
    async fn pin_rm_post(&self, arg: String) -> Result<PinRmPostResponse, ApiError>;

    /// Connect to peers
    async fn swarm_connect_post(
        &self,
        arg: &Vec<String>,
    ) -> Result<SwarmConnectPostResponse, ApiError>;

    /// Report connected peers
    async fn swarm_peers_post(&self) -> Result<SwarmPeersPostResponse, ApiError>;

    /// Report server version
    async fn version_post(&self) -> Result<VersionPostResponse, ApiError>;
}

/// Trait to extend an API to make it easy to bind it to a context.
pub trait ContextWrapperExt<C: Send + Sync>
where
    Self: Sized,
{
    /// Binds this API to a context.
    fn with_context(self, context: C) -> ContextWrapper<Self, C>;
}

impl<T: Api<C> + Send + Sync, C: Clone + Send + Sync> ContextWrapperExt<C> for T {
    fn with_context(self: T, context: C) -> ContextWrapper<T, C> {
        ContextWrapper::<T, C>::new(self, context)
    }
}

#[async_trait]
impl<T: Api<C> + Send + Sync, C: Clone + Send + Sync> ApiNoContext<C> for ContextWrapper<T, C> {
    fn poll_ready(&self, cx: &mut Context) -> Poll<Result<(), ServiceError>> {
        self.api().poll_ready(cx)
    }

    fn context(&self) -> &C {
        ContextWrapper::context(self)
    }

    /// Get a single IPFS block
    async fn block_get_post(
        &self,
        arg: String,
        timeout: Option<String>,
        offline: Option<bool>,
    ) -> Result<BlockGetPostResponse, ApiError> {
        let context = self.context().clone();
        self.api()
            .block_get_post(arg, timeout, offline, &context)
            .await
    }

    /// Put a single IPFS block
    async fn block_put_post(
        &self,
        file: swagger::ByteArray,
        cid_codec: Option<models::Codecs>,
        mhtype: Option<models::Multihash>,
        pin: Option<bool>,
    ) -> Result<BlockPutPostResponse, ApiError> {
        let context = self.context().clone();
        self.api()
            .block_put_post(file, cid_codec, mhtype, pin, &context)
            .await
    }

    /// Report statistics about a block
    async fn block_stat_post(&self, arg: String) -> Result<BlockStatPostResponse, ApiError> {
        let context = self.context().clone();
        self.api().block_stat_post(arg, &context).await
    }

    /// Get an IPLD node from IPFS
    async fn dag_get_post(
        &self,
        arg: String,
        output_codec: Option<models::Codecs>,
    ) -> Result<DagGetPostResponse, ApiError> {
        let context = self.context().clone();
        self.api().dag_get_post(arg, output_codec, &context).await
    }

    /// Import a CAR file of IPLD nodes into IPFS
    async fn dag_import_post(
        &self,
        file: swagger::ByteArray,
    ) -> Result<DagImportPostResponse, ApiError> {
        let context = self.context().clone();
        self.api().dag_import_post(file, &context).await
    }

    /// Put an IPLD node into IPFS
    async fn dag_put_post(
        &self,
        file: swagger::ByteArray,
        store_codec: Option<models::Codecs>,
        input_codec: Option<models::Codecs>,
    ) -> Result<DagPutPostResponse, ApiError> {
        let context = self.context().clone();
        self.api()
            .dag_put_post(file, store_codec, input_codec, &context)
            .await
    }

    /// Resolve an IPFS path to a DAG node
    async fn dag_resolve_post(&self, arg: String) -> Result<DagResolvePostResponse, ApiError> {
        let context = self.context().clone();
        self.api().dag_resolve_post(arg, &context).await
    }

    /// Report identifying information about a node
    async fn id_post(&self, arg: Option<String>) -> Result<IdPostResponse, ApiError> {
        let context = self.context().clone();
        self.api().id_post(arg, &context).await
    }

    /// Add a block to the pin store
    async fn pin_add_post(
        &self,
        arg: String,
        recursive: Option<bool>,
        progress: Option<bool>,
    ) -> Result<PinAddPostResponse, ApiError> {
        let context = self.context().clone();
        self.api()
            .pin_add_post(arg, recursive, progress, &context)
            .await
    }

    /// Remove a block from the pin store
    async fn pin_rm_post(&self, arg: String) -> Result<PinRmPostResponse, ApiError> {
        let context = self.context().clone();
        self.api().pin_rm_post(arg, &context).await
    }

    /// Connect to peers
    async fn swarm_connect_post(
        &self,
        arg: &Vec<String>,
    ) -> Result<SwarmConnectPostResponse, ApiError> {
        let context = self.context().clone();
        self.api().swarm_connect_post(arg, &context).await
    }

    /// Report connected peers
    async fn swarm_peers_post(&self) -> Result<SwarmPeersPostResponse, ApiError> {
        let context = self.context().clone();
        self.api().swarm_peers_post(&context).await
    }

    /// Report server version
    async fn version_post(&self) -> Result<VersionPostResponse, ApiError> {
        let context = self.context().clone();
        self.api().version_post(&context).await
    }
}

#[cfg(feature = "client")]
pub mod client;

// Re-export Client as a top-level name
#[cfg(feature = "client")]
pub use client::Client;

#[cfg(feature = "server")]
pub mod server;

// Re-export router() as a top-level name
#[cfg(feature = "server")]
pub use self::server::Service;

#[cfg(feature = "server")]
pub mod context;

pub mod models;

#[cfg(any(feature = "client", feature = "server"))]
pub(crate) mod header;
