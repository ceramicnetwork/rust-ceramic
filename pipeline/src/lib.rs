//! Pipeline provides a set of tables of Ceramic events and transformations between them.
//!
//! Transformations are implemented as actors where each actor communicates with other actors via
//! message passing. Each actor owns its tables, meaning its is the only code allowed to query its
//! tables directly. However all actors share the same datafusion [`SessionContext`] so that all
//! tables can be exposed via a FlightSQL server.
#![warn(missing_docs)]

#[macro_use]
pub(crate) mod macros;

pub mod aggregator;
mod cache_table;
pub mod cid_part;
pub mod cid_string;
pub mod concluder;
mod config;
pub mod dimension_extract;
mod metrics;
pub mod schemas;
mod since;
pub mod stream_id_string;
pub mod stream_id_to_cid;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use aggregator::{Aggregator, AggregatorHandle};
use anyhow::Result;
use concluder::{Concluder, ConcluderHandle};
use datafusion::{
    catalog::MemorySchemaProvider,
    execution::{config::SessionConfig, context::SessionContext},
    functions_aggregate::first_last::LastValue,
    logical_expr::{AggregateUDF, ScalarUDF},
};
use object_store::ObjectStore;
use tokio::task::JoinHandle;
use url::Url;

use cid_string::{CidString, CidStringList};
use dimension_extract::DimensionExtract;
use stream_id_string::{StreamIdString, StreamIdStringList};

pub use concluder::{
    conclusion_events_to_record_batch, ConclusionData, ConclusionEvent, ConclusionFeed,
    ConclusionInit, ConclusionTime,
};
pub use config::{ConclusionFeedSource, Config};
pub use metrics::Metrics;

/// A reference to a shared [`SessionContext`].
///
pub type SessionContextRef = Arc<SessionContext>;

/// Shared context between all pipeline actors.
#[derive(Clone)]
pub struct PipelineContext {
    session: SessionContextRef,
    object_store_url: Url,
}

impl PipelineContext {
    /// Returns a [`SessionContext`] configured with all tables in the pipeline.
    pub fn session(&self) -> SessionContextRef {
        self.session.clone()
    }
}

/// A pipeline
pub struct Pipeline {
    handle: PipelineHandle,
    waiter: PipelineWaiter,
}
impl Pipeline {
    /// Returns the shared context for the pipeline.
    pub fn pipeline_ctx(&self) -> &PipelineContext {
        self.handle.pipeline_ctx()
    }
    /// Deconstruct the pipeline into its handle and waiter.
    pub fn into_parts(self) -> (PipelineHandle, PipelineWaiter) {
        (self.handle, self.waiter)
    }
}

/// Provides method to wait for all pipeline tasks and actors to finish.
pub struct PipelineWaiter {
    task_handles: Vec<JoinHandle<()>>,
}
impl PipelineWaiter {
    /// Wait for all pipeline actors to shutdown.
    /// Does not cause actors to stop, simply waits for them to do so.
    /// All outstanding handles must be dropped before this will complete.
    pub async fn wait(self) {
        for h in self.task_handles.into_iter() {
            let _ = h.await;
        }
    }
}

/// Handle to the pipeline.
/// Can be used to get handles to actors in order to send them messages.
/// Can be waited to ensure a gracefull shutdown of all actors.
#[derive(Clone)]
pub struct PipelineHandle {
    ctx: PipelineContext,
    concluder: Option<ConcluderHandle>,
    aggregator: Option<AggregatorHandle>,
}

impl PipelineHandle {
    /// Construct a pipline handle from its parts.
    pub fn new(
        ctx: PipelineContext,
        concluder: Option<ConcluderHandle>,
        aggregator: Option<AggregatorHandle>,
    ) -> Self {
        Self {
            ctx,
            concluder,
            aggregator,
        }
    }

    /// Returns a handle to the Concluder actor when it is enabled.
    pub fn concluder(&self) -> Option<ConcluderHandle> {
        self.concluder.clone()
    }

    /// Returns a handle to the Aggregator actor when it is enabled.
    pub fn aggregator(&self) -> Option<AggregatorHandle> {
        self.aggregator.clone()
    }

    /// Returns the shared context for the pipeline.
    pub fn pipeline_ctx(&self) -> &PipelineContext {
        &self.ctx
    }
}

/// Construct a shared pipeline context for all pipeline actors.
pub async fn pipeline_ctx(object_store: Arc<dyn ObjectStore>) -> Result<PipelineContext> {
    let session_config = SessionConfig::new()
        .with_default_catalog_and_schema("ceramic", "v0")
        .with_information_schema(true);

    let mut ctx = SessionContext::new_with_config(session_config);

    // Register the _internal schema
    ctx.catalog("ceramic")
        .expect("ceramic catalog should always exist")
        .register_schema("_internal", Arc::new(MemorySchemaProvider::default()))?;

    // Register various UDxFs
    ctx.register_udaf(AggregateUDF::new_from_impl(LastValue::default()));
    ctx.register_udf(ScalarUDF::new_from_impl(CidString::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(CidStringList::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(StreamIdString::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(StreamIdStringList::new()));
    ctx.register_udf(ScalarUDF::new_from_impl(DimensionExtract::new()));

    // Register JSON functions
    datafusion_functions_json::register_all(&mut ctx)?;

    // Register s3 object store, use hardcoded bucket name `pipeline` as the actual bucket name is
    // already known by the object store.
    let url = Url::parse("s3://pipeline")?;
    ctx.register_object_store(&url, object_store);

    Ok(PipelineContext {
        session: Arc::new(ctx),
        object_store_url: url,
    })
}

/// Starts various actors that process the pipeline.
pub async fn spawn_actors<F: ConclusionFeed + 'static>(
    config: impl Into<Config<F>>,
) -> Result<Pipeline> {
    let config: Config<F> = config.into();

    let pipeline_ctx = pipeline_ctx(config.object_store).await?;

    // Spawn the various actors
    let mut task_handles = Vec::new();

    let (concluder, mut tasks) = Concluder::spawn_new(
        100,
        &pipeline_ctx,
        config.conclusion_feed,
        config.metrics.clone(),
        config.shutdown.clone(),
    )
    .await?;
    task_handles.append(&mut tasks);

    let aggregator = if config.aggregator {
        let (aggregator, mut tasks) = Aggregator::spawn_new(
            1_000,
            &pipeline_ctx,
            None,
            concluder.clone(),
            config.metrics.clone(),
            config.shutdown.clone(),
        )
        .await?;
        task_handles.append(&mut tasks);
        Some(aggregator)
    } else {
        None
    };

    Ok(Pipeline {
        handle: PipelineHandle {
            ctx: pipeline_ctx,
            concluder: Some(concluder),
            aggregator,
        },
        waiter: PipelineWaiter { task_handles },
    })
}
