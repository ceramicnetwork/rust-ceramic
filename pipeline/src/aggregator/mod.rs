//! Aggregation functions for Ceramic Model Instance Document streams.
//!
//! Applies each new event to the previous state of the stream producing the stream state at each
//! event in the stream.
//!
#[macro_use]
pub(crate) mod result;

mod dict_or_array;
mod metrics;
#[cfg(any(test, feature = "mock"))]
pub mod mock;
mod model_instance_patch;
mod model_instance_validate;
mod model_patch;
mod model_validate;
mod validation;

use object_store::ObjectStore;
pub use validation::{ModelAccountRelationV2, ModelDefinition};

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use anyhow::Context;
use arrow::{
    array::{
        Array as _, ArrayAccessor as _, ArrayBuilder as _, BinaryArray, BinaryBuilder, RecordBatch,
        UInt32Builder,
    },
    compute::concat_batches,
    datatypes::Int32Type,
};
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use ceramic_actor::{actor_envelope, Actor, Handler, Message, Shutdown as ActorShutdown};
use ceramic_core::{StreamId, StreamIdType};
use cid::Cid;
use datafusion::{
    common::{
        cast::{
            as_binary_array, as_dictionary_array, as_map_array, as_string_array, as_uint64_array,
        },
        exec_datafusion_err, Column, JoinType,
    },
    dataframe::DataFrameWriteOptions,
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    execution::SendableRecordBatchStream,
    functions_aggregate::expr_fn::last_value,
    functions_array::extract::array_element,
    functions_window::expr_fn::row_number,
    logical_expr::{
        col, dml::InsertOp, expr::WindowFunction, lit, Expr, ExprFunctionExt as _,
        WindowFunctionDefinition, UNNAMED_TABLE,
    },
    physical_plan::stream::RecordBatchStreamAdapter,
    prelude::{array_empty, cast, when, wildcard, DataFrame},
    sql::TableReference,
};
use futures::{StreamExt as _, TryStreamExt as _};
use model_instance_patch::ModelInstancePatch;
use model_patch::ModelPatch;
use shutdown::{Shutdown, ShutdownSignal};
use tokio::{select, sync::broadcast, task::JoinHandle};
use tracing::{debug, error, info, instrument};

use crate::{
    cache_table::CacheTable,
    cid_part::cid_part,
    concluder::ConcluderHandle,
    dimension_extract::dimension_extract,
    metrics::Metrics,
    schemas,
    since::{gt_expression, rows_since, FeedTable, FeedTableSource, RowsSinceInput},
    stream_id_to_cid::stream_id_to_cid,
    PipelineContext, Result, SessionContextRef,
};
// Use the SubscribeSinceMsg so its clear its a message for this actor
pub use crate::since::SubscribeSinceMsg;

const EVENT_STATES_TABLE: &str = "ceramic.v0.event_states";
const EVENT_STATES_FEED_TABLE: &str = "ceramic.v0.event_states_feed";
const EVENT_STATES_MEM_TABLE: &str = "ceramic._internal.event_states_mem";
const EVENT_STATES_PERSISTENT_TABLE: &str = "ceramic._internal.event_states_persistent";
// Path within the object store where the event states table is stored
// This path should be updated when the underlying storage structure changes. (i.e. the parition
// columns change). The revision is a physical versioning number and not directly associated with
// the logical schema version of the table. There are many cases where the physical revision may
// need to change while the logical version remains the same.
const EVENT_STATES_TABLE_OBJECT_STORE_PATH: &str = "ceramic/rev3/event_states/";

const PENDING_EVENT_STATES_TABLE: &str = "ceramic._internal.pending_event_states";
const PENDING_EVENT_STATES_TABLE_OBJECT_STORE_PATH: &str = "ceramic/rev1/pending_event_states/";

// Maximum number of rows to cache in memory before writing to object store.
const DEFAULT_MAX_CACHED_ROWS: usize = 10_000;

/// Aggregator is responsible for computing the state of a stream for each event within the stream.
/// The aggregator only operates on model and model instance stream types.
#[derive(Actor)]
pub struct Aggregator {
    ctx: SessionContextRef,
    broadcast_tx: broadcast::Sender<RecordBatch>,
    max_cached_rows: usize,
    max_event_state_order: u64,
    object_store: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for Aggregator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Aggregator")
            .field("ctx", &"Pipeline(SessionContext)")
            .field("broadcast_tx", &self.broadcast_tx)
            .finish()
    }
}

impl Aggregator {
    /// Create a new aggregator actor and spawn its tasks.
    pub async fn spawn_new(
        size: usize,
        ctx: &PipelineContext,
        max_cached_rows: Option<usize>,
        concluder: ConcluderHandle,
        metrics: Metrics,
        shutdown: Shutdown,
    ) -> Result<(AggregatorHandle, Vec<JoinHandle<()>>)> {
        // Register event_states tables
        let file_format = ParquetFormat::default().with_enable_pruning(true);
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![("event_cid_partition".to_owned(), DataType::Int32)])
            .with_file_sort_order(vec![vec![col("event_cid").sort(true, true)]]);

        // Set the path within the bucket for the event_states table
        let mut event_states_url = ctx.object_store_url.clone();
        event_states_url.set_path(EVENT_STATES_TABLE_OBJECT_STORE_PATH);
        // Register event_states_persistent as a listing table
        ctx.session().register_table(
            EVENT_STATES_PERSISTENT_TABLE,
            Arc::new(ListingTable::try_new(
                ListingTableConfig::new(ListingTableUrl::parse(event_states_url)?)
                    .with_listing_options(listing_options)
                    // Use the non partitioned schema as the parquet files themselves do not
                    // contain the partition columns.
                    .with_schema(schemas::event_states()),
            )?),
        )?;

        ctx.session().register_table(
            EVENT_STATES_MEM_TABLE,
            Arc::new(CacheTable::try_new(
                schemas::event_states_partitioned(),
                vec![vec![RecordBatch::new_empty(
                    schemas::event_states_partitioned(),
                )]],
            )?),
        )?;

        ctx.session().register_table(
            EVENT_STATES_TABLE,
            ctx.session()
                .table(EVENT_STATES_MEM_TABLE)
                .await?
                .union(ctx.session().table(EVENT_STATES_PERSISTENT_TABLE).await?)?
                .into_view(),
        )?;

        // Register event_states tables
        let file_format = ParquetFormat::default().with_enable_pruning(true);
        let listing_options =
            ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

        // Set the path within the bucket for the event_states table
        let mut pending_event_states_url = ctx.object_store_url.clone();
        pending_event_states_url.set_path(PENDING_EVENT_STATES_TABLE_OBJECT_STORE_PATH);

        // Register pending_event_states as a listing table
        ctx.session().register_table(
            PENDING_EVENT_STATES_TABLE,
            Arc::new(ListingTable::try_new(
                ListingTableConfig::new(ListingTableUrl::parse(pending_event_states_url)?)
                    .with_listing_options(listing_options)
                    .with_schema(schemas::pending_event_states()),
            )?),
        )?;

        // Query for max event_state_order in persistent event_states, this is where we should start
        // the new order values.
        let batches = ctx
            .session()
            .table(EVENT_STATES_PERSISTENT_TABLE)
            .await?
            .aggregate(
                vec![],
                vec![
                    datafusion::functions_aggregate::min_max::max(col("conclusion_event_order"))
                        .alias("max_conclusion_event_order"),
                    datafusion::functions_aggregate::min_max::max(col("event_state_order"))
                        .alias("max_event_state_order"),
                ],
            )?
            .collect()
            .await?;
        let max_conclusion_event_order = batches.first().and_then(|batch| {
            batch
                .column_by_name("max_conclusion_event_order")
                .and_then(|col| {
                    as_uint64_array(&col)
                        .ok()
                        .and_then(|col| col.is_valid(0).then(|| col.value(0)))
                })
        });
        let max_event_state_order = batches.first().and_then(|batch| {
            batch
                .column_by_name("max_event_state_order")
                .and_then(|col| {
                    as_uint64_array(&col)
                        .ok()
                        .and_then(|col| col.is_valid(0).then(|| col.value(0)))
                })
        });

        debug!(
            ?max_conclusion_event_order,
            ?max_event_state_order,
            "aggregator highwater marks"
        );

        // Spawn actor
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(1_000);
        let aggregator = Aggregator {
            ctx: ctx.session(),
            broadcast_tx,
            max_cached_rows: max_cached_rows.unwrap_or(DEFAULT_MAX_CACHED_ROWS),
            max_event_state_order: max_event_state_order.unwrap_or_default(),
            object_store: ctx.object_store.clone(),
        };

        let (handle, task_handle) = Self::spawn(size, aggregator, metrics, shutdown.wait_fut());
        let h = handle.clone();
        let sub_handle = tokio::spawn(async move {
            if let Err(err) = concluder_subscription(
                shutdown.wait_fut(),
                concluder,
                h,
                max_conclusion_event_order,
            )
            .await
            {
                error!(%err, "aggregator actor poll_concluder task failed");
            }
        });

        ctx.session()
            .register_table(
                EVENT_STATES_FEED_TABLE,
                Arc::new(FeedTable::new(handle.clone())),
            )
            .expect("should be able to register table");

        Ok((handle, vec![task_handle, sub_handle]))
    }

    // Process a batch of conclusion events, producing a new document state for each input event.
    // The conclusion event must be ordered by "conclusion_event_order".
    // The output event states are ordered by "event_state_order".
    //
    // The process has two main phases:
    // 1. Aggregate events to produce their new states
    // 2. Validate new states.
    //
    // Computing states and validation are stream type specific operations. So these phases are
    // repeated for each stream type.
    #[instrument(skip(self, conclusion_events), err)]
    async fn process_conclusion_events_batch(
        &mut self,
        conclusion_events: RecordBatch,
    ) -> Result<RecordBatch> {
        let events_with_previous = self
            .join_event_states(self.ctx.read_batch(conclusion_events)?)
            .await
            .context("joining events with previous states")?
            .cache()
            .await
            .context("caching events joined with previous states")?;
        // NOTE: Cloning a cached data frame is cheap as the data itself is not cloned.
        let models = events_with_previous
            .clone()
            .filter(col("stream_type").eq(lit(StreamIdType::Model as u8)))?;
        // Completely process models first for the case when mids may reference a newly
        // created/updated model.
        let models = self.patch_models(models).await.context("patching models")?;
        let models = self.validate_models(models).context("validating models")?;
        let mut models = self
            .store_event_states(models, false)
            .await
            .context("storing models")?;

        let pending = self.fetch_pending(models.clone()).await?;
        // Find any pending mids that can now be processed
        let ready_mids = pending
            .clone()
            .filter(col("model_definition").is_not_null())?;
        let pending_mids = pending
            .filter(col("model_definition").is_null())?
            .drop_columns(&["model_definition"])?;

        let mids = events_with_previous
            .filter(col("stream_type").eq(lit(StreamIdType::ModelInstanceDocument as u8)))?;
        let mids = self
            .patch_model_instances(mids)
            .await
            .context("patching mids")?;
        let mids = self
            .join_with_models(mids)
            .await
            .context("join mids with models")?
            .cache()
            .await
            .context("caching mids with their models")?;

        let pending_mids = mids
            .clone()
            .filter(col("model_definition").is_null())?
            .drop_columns(&["event_cid_partition", "model_definition"])?
            .union(pending_mids)?;

        let ready_mids = mids
            .clone()
            .filter(col("model_definition").is_not_null())?
            .union(
                ready_mids
                    .select(vec![
                        col("conclusion_event_order"),
                        col("stream_cid"),
                        col("stream_type"),
                        col("controller"),
                        col("dimensions"),
                        col("event_cid"),
                        cid_part(col("event_cid")).alias("event_cid_partition"),
                        col("event_type"),
                        col("event_height"),
                        col("data"),
                        col("patch"),
                        col("model_version"),
                        col("model_definition"),
                        col("before"),
                        col("chain_id"),
                    ])
                    .context("select ready mids")?,
            )
            .context("union ready mids")?;

        // HACK: Explicitly delete old files in the pending_event_states table.
        //
        // We can do this because we know that this task is the only reader and writer to the
        // table.
        //
        // This hack is being used because alternatives all hit dead ends:
        //  1. https://github.com/delta-io/delta-rs uses global state to register object_store instances and so tests were
        //     difficult to isolate
        //  2. https://github.com/apache/iceberg-rust/tree/main does not yet support deletes
        //     although it is in-progress https://github.com/apache/iceberg-rust/issues/630
        //  3. https://github.com/JanKaul/iceberg-rust/tree/main supports deletes but does not
        //     support Map or Dictionary types, we could upstream a change there but it seems like
        //     long term will be to use option 2 once deletes are supported.
        //
        // Updating the pending_event_states table is a three step process:
        //  1. List all existing files
        //  2. Write new files
        //  3. Delete previously existing files.
        //
        //  If we fail in between any of these steps we will end up with duplicate rows, but not
        //  missing rows. Additionally a successful pass after a failure will remove all duplicates.

        let existing_files = self
            .object_store
            .list(Some(&PENDING_EVENT_STATES_TABLE_OBJECT_STORE_PATH.into()))
            .map_ok(|f| f.location)
            .try_collect::<Vec<_>>()
            .await?;

        let stats = pending_mids
            .write_table(PENDING_EVENT_STATES_TABLE, DataFrameWriteOptions::new())
            .await
            .context("writing pending events")?;

        let mut retries = 3;
        loop {
            let result = self
                .object_store
                .delete_stream(tokio_stream::iter(existing_files.clone()).map(Ok).boxed())
                .try_collect::<Vec<_>>()
                .await;
            if let Err(err) = result {
                if retries == 0 {
                    error!(
                        %err,
                        "failed to delete pending event files, this will result in duplicate pending events which may remain in pending state forever. To remove them an operator can delete all event_states and pending_event_states files from object store, on restart the process will rebuild all event_state data."
                    );
                    return Err(err.into());
                }
                retries -= 1;
            } else {
                break;
            }
        }

        let total_pending_events = stats
            .first()
            .and_then(|batch| {
                batch.column_by_name("count").and_then(|col| {
                    as_uint64_array(&col)
                        .ok()
                        .and_then(|col| col.is_valid(0).then(|| col.value(0)))
                })
            })
            .unwrap_or_default();
        tracing::debug!(total_pending_events, "pending stats");

        let mids = self
            .validate_model_instances(ready_mids)
            .await
            .context("validating mids")?;
        let mut mids = self
            .store_event_states(mids, true)
            .await
            .context("storing mids")?;
        let mut ordered_events = Vec::with_capacity(models.len() + mids.len());
        ordered_events.append(&mut models);
        ordered_events.append(&mut mids);
        // TODO: return a vec<record batch> so we do not have to reallocate them
        Ok(concat_batches(&schemas::event_states(), &ordered_events)?)
    }

    // Joins events with their previous state if any
    #[instrument(skip_all)]
    async fn join_event_states(&self, conclusion_events: DataFrame) -> Result<DataFrame> {
        let conclusion_events = conclusion_events
            // MID only ever use the first previous, so we can optimize the join by selecting the
            // first element of the previous array.
            .select(vec![
                col("conclusion_event_order"),
                col("stream_cid"),
                col("stream_type"),
                col("controller"),
                col("dimensions"),
                col("event_cid"),
                col("event_type"),
                col("data"),
                array_element(col("previous"), lit(1)).alias("previous"),
                cid_part(array_element(col("previous"), lit(1)))
                    .alias("previous_event_cid_partition"),
                col("before"),
                col("chain_id"),
                cid_part(col("event_cid")).alias("event_cid_partition"),
            ])
            .context("selecting conclusion events")?
            .alias("conclusion_events")?;

        // remove events we've already seen
        let known_event_states = self
            .ctx
            .table(EVENT_STATES_TABLE)
            .await?
            .select_columns(&["event_cid"])?
            .with_column_renamed("event_cid", "seen_event_cid")?;

        let conclusion_events = conclusion_events
            .join_on(
                known_event_states,
                JoinType::LeftAnti,
                vec![col("conclusion_events.event_cid").eq(col("seen_event_cid"))],
            )
            .context("anti join")?;

        // join all new conclusion_events with known history to apply patches
        // Include both EVENT_STATES_TABLE and PENDING_EVENT_STATES_TABLE
        // because events may reference pending events that haven't been fully processed yet
        // but will be 'unlocked' in our batch if their model being processed as well
        let event_states_for_previous = self
            .ctx
            .table(EVENT_STATES_TABLE)
            .await?
            .select_columns(&["event_cid_partition", "event_cid", "event_height", "data"])?
            .union(
                self.ctx
                    .table(PENDING_EVENT_STATES_TABLE)
                    .await?
                    .select(vec![
                        cid_part(col("event_cid")).alias("event_cid_partition"),
                        col("event_cid"),
                        col("event_height"),
                        col("data"),
                    ])?,
            )?
            // Alias column so it does not conflict with the column from conclusion_events
            // in the join.
            .with_column_renamed("event_cid_partition", "ecp")?
            .with_column_renamed("event_cid", "previous_event_cid")?
            .with_column_renamed("data", "previous_data")?
            .with_column_renamed("event_height", "previous_height")?;

        let conclusion_events = conclusion_events
            .join_on(
                event_states_for_previous,
                JoinType::Left,
                [
                    col("previous_event_cid_partition").eq(col("ecp")),
                    col("previous").eq(col("previous_event_cid")),
                ],
            )
            .context("setup join")?
            .cache()
            .await
            .context("caching joined events")?
            .select(vec![
                col("conclusion_event_order"),
                col("stream_cid"),
                col("stream_type"),
                col("controller"),
                col("dimensions"),
                col("event_cid"),
                col("event_type"),
                col("data"),
                col("previous"),
                col("previous_data"),
                col("previous_height"),
                col("before"),
                col("chain_id"),
                col("event_cid_partition"),
            ])
            .context("select joined conclusion events")?;

        Ok(conclusion_events)
    }

    // Retrieves all pending events and joins them with the models if they are now available.
    #[instrument(skip_all)]
    async fn fetch_pending(&self, models: Vec<RecordBatch>) -> Result<DataFrame> {
        self.ctx
            .table(PENDING_EVENT_STATES_TABLE)
            .await
            .context("read pending events")?
            .join_on(
                self.ctx
                    .read_batches(models)?
                    .select_columns(&["event_cid", "data"])?
                    .with_column_renamed("event_cid", "model_cid")?
                    .with_column_renamed("data", "model_definition")?,
                JoinType::Left,
                [col("model_version").eq(col("model_cid"))],
            )?
            .select_columns(&[
                "conclusion_event_order",
                "stream_cid",
                "stream_type",
                "controller",
                "dimensions",
                "event_cid",
                "event_type",
                "event_height",
                "data",
                "patch",
                "model_version",
                "model_definition",
                "before",
                "chain_id",
            ])
            .context("select pending events")?
            .cache()
            .await
            .context("read pending events")
    }

    // Applies patches to models
    #[instrument(skip_all)]
    async fn patch_models(&self, events_with_previous: DataFrame) -> Result<DataFrame> {
        Ok(events_with_previous
            .window(vec![Expr::WindowFunction(WindowFunction::new(
                WindowFunctionDefinition::WindowUDF(Arc::new(ModelPatch::new_udwf())),
                vec![
                    col("event_cid"),
                    col("previous"),
                    col("previous_data"),
                    col("previous_height"),
                    col("data"),
                ],
            ))
            .partition_by(vec![col("stream_cid")])
            .order_by(vec![col("conclusion_event_order").sort(true, true)])
            .build()?
            .alias("patched")])?
            .unnest_columns(&["patched"])?
            // Rename columns to match event_states table schema
            .select(vec![
                col("conclusion_event_order"),
                col("stream_cid"),
                col("stream_type"),
                col("controller"),
                col("dimensions"),
                col("event_cid"),
                col("event_type"),
                Expr::Column(Column {
                    relation: None,
                    name: "patched.event_height".to_owned(),
                })
                .alias("event_height"),
                Expr::Column(Column {
                    relation: None,
                    name: "patched.previous_data".to_owned(),
                })
                .alias("previous_data"),
                Expr::Column(Column {
                    relation: None,
                    name: "patched.data".to_owned(),
                })
                .alias("data"),
                col("before"),
                col("chain_id"),
                col("event_cid_partition"),
            ])?)
    }
    #[instrument(skip_all)]
    fn validate_models(&self, models: DataFrame) -> Result<DataFrame> {
        Ok(models.select(vec![
            col("conclusion_event_order"),
            col("stream_cid"),
            col("stream_type"),
            col("controller"),
            col("dimensions"),
            col("event_cid"),
            col("event_type"),
            col("event_height"),
            col("data"),
            model_validate::model_validate(
                col("data"),
                col("previous_data"),
                dimension_extract(col("dimensions"), lit("model")),
            )
            .alias("validation_errors"),
            col("before"),
            col("chain_id"),
            col("event_cid_partition"),
        ])?)
    }
    // Applies patches to model instances
    #[instrument(skip_all)]
    async fn patch_model_instances(&self, events_with_previous: DataFrame) -> Result<DataFrame> {
        events_with_previous
            .window(vec![Expr::WindowFunction(WindowFunction::new(
                WindowFunctionDefinition::WindowUDF(Arc::new(ModelInstancePatch::new_udwf())),
                vec![
                    col("event_cid"),
                    col("previous"),
                    col("previous_data"),
                    col("previous_height"),
                    col("data"),
                ],
            ))
            .partition_by(vec![col("stream_cid")])
            .order_by(vec![col("conclusion_event_order").sort(true, true)])
            .build()?
            .alias("patched")])?
            // Flatten struct from model_instance_patch
            .unnest_columns(&["patched"])
            .context("unnest_columns")?
            .select(vec![
                col("conclusion_event_order"),
                col("stream_cid"),
                col("stream_type"),
                col("controller"),
                col("dimensions"),
                col("event_cid"),
                col("event_type"),
                Expr::Column(Column {
                    relation: None,
                    name: "patched.event_height".to_owned(),
                })
                .alias("event_height"),
                // Determine model version from either the new metadata or from the dimensions.
                when(
                    Expr::Column(Column {
                        relation: None,
                        name: "patched.model_version".to_owned(),
                    })
                    .is_not_null(),
                    Expr::Column(Column {
                        relation: None,
                        name: "patched.model_version".to_owned(),
                    }),
                )
                .otherwise(stream_id_to_cid(cast(
                    dimension_extract(col("dimensions"), lit("model")),
                    DataType::Binary,
                )))?
                .alias("model_version"),
                Expr::Column(Column {
                    relation: None,
                    name: "patched.data".to_owned(),
                })
                .alias("data"),
                Expr::Column(Column {
                    relation: None,
                    name: "patched.patch".to_owned(),
                })
                .alias("patch"),
                col("before"),
                col("chain_id"),
                col("event_cid_partition"),
            ])
            .context("select")
    }
    #[instrument(skip_all)]
    async fn join_with_models(&self, model_instances: DataFrame) -> Result<DataFrame> {
        let states = self
            .ctx
            .table(EVENT_STATES_TABLE)
            .await?
            .select_columns(&["event_cid_partition", "event_cid", "data"])?
            .with_column_renamed("data", "model_definition")?;
        model_instances
            .join_on(
                states,
                JoinType::Left,
                [
                    cid_part(col("model_version"))
                        .eq(table_col(EVENT_STATES_TABLE, "event_cid_partition")),
                    col("model_version").eq(table_col(EVENT_STATES_TABLE, "event_cid")),
                ],
            )
            .context("join")?
            .select(vec![
                anon_col("conclusion_event_order"),
                anon_col("stream_cid"),
                anon_col("stream_type"),
                anon_col("controller"),
                anon_col("dimensions"),
                anon_col("event_cid"),
                anon_col("event_cid_partition"),
                anon_col("event_type"),
                col("event_height"),
                col("data"),
                col("patch"),
                col("model_version"),
                col("model_definition"),
                col("before"),
                col("chain_id"),
            ])
            .context("select")
    }
    #[instrument(skip_all)]
    async fn validate_model_instances(&self, model_instances: DataFrame) -> Result<DataFrame> {
        model_instances
            .select(vec![
                col("conclusion_event_order"),
                col("stream_cid"),
                col("stream_type"),
                col("controller"),
                col("dimensions"),
                col("event_cid"),
                col("event_type"),
                col("event_height"),
                col("data"),
                model_instance_validate::model_instance_validate(
                    col("data"),
                    col("patch"),
                    col("model_version"),
                    col("model_definition"),
                    col("event_height"),
                    dimension_extract(col("dimensions"), lit("unique")),
                )
                .alias("validation_errors"),
                col("before"),
                col("chain_id"),
                col("event_cid_partition"),
            ])
            .context("select")
    }

    #[instrument(skip_all)]
    async fn store_event_states(
        &mut self,
        event_states: DataFrame,
        allow_cache_flush: bool,
    ) -> Result<Vec<RecordBatch>> {
        let row_count = event_states.clone().count().await?;
        if row_count == 0 {
            return Ok(vec![RecordBatch::new_empty(
                schemas::event_states_partitioned(),
            )]);
        }

        let start_event_state = self.max_event_state_order;
        self.max_event_state_order += row_count as u64;
        // Assign an insertion order value to the new event state rows
        let ordered = event_states
            .window(vec![row_number()
                .order_by(vec![
                    // Then ensure that the order preserves stream order and is deterministic.
                    // Otherwise applications would see stream updates before seeing their previous
                    // state.
                    // External to this function we ensure that a model comes befores its
                    // instances.
                    col("stream_cid").sort(true, true),
                    col("event_height").sort(true, true),
                    col("event_cid").sort(true, true),
                ])
                .build()
                .context("row number")?
                .alias("row_num")])
            .context("window")?
            .select(vec![
                col("conclusion_event_order"),
                // Compute new order value for the event_states table
                (col("row_num") + lit(start_event_state)).alias("event_state_order"),
                col("stream_cid"),
                col("stream_type"),
                col("controller"),
                col("dimensions"),
                col("event_cid"),
                col("event_type"),
                col("event_height"),
                col("data"),
                col("validation_errors"),
                col("before"),
                col("chain_id"),
                col("event_cid_partition"),
            ])
            .context("select")?
            .cache()
            .await
            .context("adding event_state_order")?;
        // Write states to the in memory event_states table
        ordered
            .clone()
            .write_table(
                EVENT_STATES_MEM_TABLE,
                DataFrameWriteOptions::new()
                    .with_partition_by(vec!["event_cid_partition".to_owned()]),
            )
            .await
            .context("writing to mem table data")?;

        if allow_cache_flush {
            self.flush_cache().await?;
            let count = self.count_cache().await?;
            tracing::debug!(%count, will_flush = %count >= self.max_cached_rows, "counts for mem table");
            // If we have enough data cached in memory write it out to persistent store
            if count >= self.max_cached_rows {
                self.flush_cache().await?;
            }
        }
        ordered.collect().await.context("collecting ordered events")
    }

    async fn count_cache(&self) -> Result<usize> {
        self.ctx
            .table(EVENT_STATES_MEM_TABLE)
            .await?
            .count()
            .await
            .context("count mem table")
    }

    async fn flush_cache(&self) -> Result<usize> {
        let cnt = self.count_cache().await?;
        self.ctx
            .table(EVENT_STATES_MEM_TABLE)
            .await?
            .write_table(
                EVENT_STATES_PERSISTENT_TABLE,
                DataFrameWriteOptions::new()
                    .with_partition_by(vec!["event_cid_partition".to_owned()]),
            )
            .await
            .context("writing to persistent table")?;
        // Clear all data in the memory batch, by writing an empty batch
        self.ctx
            .read_batch(RecordBatch::new_empty(schemas::event_states_partitioned()))
            .context("reading empty batch")?
            .write_table(
                EVENT_STATES_MEM_TABLE,
                DataFrameWriteOptions::new().with_insert_operation(InsertOp::Overwrite),
            )
            .await
            .context("clearing mem table")?;
        Ok(cnt)
    }
}

// Construct a column for the field in the UNNAMED_TABLE.
fn anon_col(field: &str) -> Expr {
    table_col(UNNAMED_TABLE, field)
}
// Construct a column for the field in the table
fn table_col(table: impl Into<TableReference>, field: &str) -> Expr {
    Expr::Column(Column {
        relation: Some(table.into()),
        name: field.into(),
    })
}

async fn concluder_subscription(
    mut shutdown: ShutdownSignal,
    concluder: ConcluderHandle,
    aggregator: AggregatorHandle,
    offset: Option<u64>,
) -> Result<()> {
    debug!(?offset, "starting concluder subscription");
    let mut rx = concluder
        .send(SubscribeSinceMsg {
            projection: None,
            filters: offset.map(|o| vec![gt_expression("conclusion_event_order", o)]),
            limit: None,
        })
        .await??;
    loop {
        select! {
            _ = &mut shutdown => { break }
            r = rx.try_next() => {
                match r {
                    Ok(Some(batch)) => {
                        if let Err(err) = aggregator.send(NewConclusionEventsMsg { events: batch }).await? {
                            error!(?err, "concluder subscription loop failed to process new conclusion events");
                        }
                    }
                    // Subscription has finished, this means we are shutting down.
                    Ok(None) => { break },
                    Err(err) => {
                        error!(%err, "concluder subscription loop failed to retrieve new conclusion events.");
                    }
                }
            }
        }
    }
    Ok(())
}

actor_envelope! {
    AggregatorEnvelope,
    AggregatorActor,
    AggregatorRecorder,
    with_shutdown,
    SubscribeSince => SubscribeSinceMsg,
    NewConclusionEvents => NewConclusionEventsMsg,
    // TODO: Remove this message and use the analogous message on the Resolver.
    // This way the canonical stream state is provided via the API
    StreamState => StreamStateMsg,
}

#[async_trait]
impl ActorShutdown for Aggregator {
    async fn shutdown(&mut self) {
        info!("Aggregator shutdown: flushing cached data to persistent storage...");

        match self.flush_cache().await {
            Ok(rows) => {
                debug!(
                    "Aggregator shutdown: successfully flushed {} rows to persistent storage",
                    rows
                );
            }
            Err(e) => {
                error!("Aggregator shutdown: failed to flush cached data: {}", e);
            }
        }
    }
}

#[async_trait]
impl Handler<SubscribeSinceMsg> for Aggregator {
    async fn handle(
        &mut self,
        message: SubscribeSinceMsg,
    ) -> <SubscribeSinceMsg as Message>::Result {
        let subscription = self.broadcast_tx.subscribe();
        let ctx = self.ctx.clone();

        // Create base query
        let mut df = ctx
            .table(EVENT_STATES_TABLE)
            .await?
            .drop_columns(&["event_cid_partition"])? // Do not return the partition columns
            .select(vec![wildcard()])?
            .sort(vec![col("event_state_order").sort(true, true)])?;

        if let Some(filters) = &message.filters {
            for filter in filters.clone() {
                df = df.filter(filter)?;
            }
        }

        if let Some(limit) = message.limit {
            df = df.limit(0, Some(limit))?;
        }

        // Execute query to get initial (historical) results
        let query_stream = df.execute_stream().await?;

        // Merge query results with subscription updates
        rows_since(RowsSinceInput {
            session_context: &ctx,
            schema: schemas::event_states(),
            order_col: "event_state_order",
            projection: message.projection,
            filters: message.filters,
            limit: message.limit,
            subscription: Box::pin(RecordBatchStreamAdapter::new(
                schemas::event_states(),
                tokio_stream::wrappers::BroadcastStream::new(subscription)
                    .map_err(|err| exec_datafusion_err!("{err}")),
            )),
            since: query_stream,
        })
    }
}

/// Inform the aggregator about new conclusion events.
#[derive(Debug)]
pub struct NewConclusionEventsMsg {
    events: RecordBatch,
}
impl Message for NewConclusionEventsMsg {
    type Result = anyhow::Result<()>;
}

#[async_trait]
impl Handler<NewConclusionEventsMsg> for Aggregator {
    async fn handle(
        &mut self,
        message: NewConclusionEventsMsg,
    ) -> <NewConclusionEventsMsg as Message>::Result {
        debug!(
            event_count = message.events.num_rows(),
            "new conclusion events"
        );
        let batch = self.process_conclusion_events_batch(message.events).await?;
        if batch.num_rows() > 0 {
            tracing::trace!(event_count = %batch.num_rows(), "sending events to subscribers");
            let _ = self.broadcast_tx.send(batch);
        }
        Ok(())
    }
}

/// Request the state of a stream
#[derive(Debug)]
pub struct StreamStateMsg {
    /// Id of the stream
    pub id: StreamId,
}
impl Message for StreamStateMsg {
    type Result = anyhow::Result<Option<StreamState>>;
}

/// State of a single stream
pub struct StreamState {
    /// Multibase encoding of the stream id
    pub id: StreamId,

    /// CID of the event that produced this state
    pub event_cid: Cid,

    /// Controller of the stream
    pub controller: String,

    /// Dimensions of the stream, each value is multibase encoded.
    pub dimensions: HashMap<String, Vec<u8>>,

    /// The data of the stream. Content is stream type specific.
    pub data: Vec<u8>,
}

impl std::fmt::Debug for StreamState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !f.alternate() {
            f.debug_struct("StreamState")
                .field("id", &self.id)
                .field("event_cid", &self.event_cid)
                .field("controller", &self.controller)
                .field("dimensions", &self.dimensions)
                .field("data", &self.data)
                .finish()
        } else {
            f.debug_struct("StreamState")
                .field("id", &self.id.to_string())
                .field("event_cid", &self.event_cid.to_string())
                .field("controller", &self.controller)
                .field(
                    "dimensions",
                    &self
                        .dimensions
                        .iter()
                        .map(|(k, v)| (k, multibase::encode(multibase::Base::Base64Url, v)))
                        .collect::<std::collections::BTreeMap<_, _>>(),
                )
                .field(
                    "data",
                    &multibase::encode(multibase::Base::Base64Url, &self.data),
                )
                .finish()
        }
    }
}

#[async_trait]
impl Handler<StreamStateMsg> for Aggregator {
    #[instrument(skip(self), ret, err)]
    async fn handle(&mut self, message: StreamStateMsg) -> <StreamStateMsg as Message>::Result {
        let id = message.id;
        let state_batch = self
            .ctx
            .table(EVENT_STATES_TABLE)
            .await
            .context("table not found {EVENT_STATES_TABLE}")?
            .select(vec![
                col("event_state_order"),
                col("stream_cid"),
                col("event_cid"),
                col("dimensions"),
                col("controller"),
                col("data"),
                col("validation_errors"),
            ])
            .context("invalid select")?
            .filter(
                col("stream_cid")
                    .eq(lit(id.cid.to_bytes()))
                    .and(array_empty(col("validation_errors"))),
            )
            .context("invalid filter")?
            .aggregate(
                vec![col("stream_cid"), col("controller")],
                vec![
                    last_value(vec![col("data")])
                        .order_by(vec![col("event_state_order").sort(true, true)])
                        .build()
                        .context("invalid last_value data query")?
                        .alias("data"),
                    last_value(vec![col("event_cid")])
                        .order_by(vec![col("event_state_order").sort(true, true)])
                        .build()
                        .context("invalid last_value event_cid query")?
                        .alias("event_cid"),
                    last_value(vec![col("dimensions")])
                        .order_by(vec![col("event_state_order").sort(true, true)])
                        .build()
                        .context("invalid last_value dimensions query")?
                        .alias("dimensions"),
                ],
            )
            .context("invalid window")?
            .collect()
            .await
            .context("invalid query")?;

        let num_rows: usize = state_batch.iter().map(|b| b.num_rows()).sum();
        if num_rows == 0 {
            // No state for the stream id found
            return Ok(None);
        }
        let batch = concat_batches(&state_batch[0].schema(), state_batch.iter())
            .context("concat batches")?;

        let data = as_binary_array(
            batch
                .column_by_name("data")
                .ok_or_else(|| anyhow::anyhow!("state column should exist"))?,
        )
        .context("data as a binary column")?
        .value(0);
        let event_cid = as_binary_array(
            batch
                .column_by_name("event_cid")
                .ok_or_else(|| anyhow::anyhow!("event_cid column should exist"))?,
        )
        .context("event_cid as a binary column")?;
        let controller = as_string_array(
            batch
                .column_by_name("controller")
                .ok_or_else(|| anyhow::anyhow!("controller column should exist"))?,
        )
        .context("controller as a string column")?;
        let dimensions = as_map_array(
            batch
                .column_by_name("dimensions")
                .ok_or_else(|| anyhow::anyhow!("dimensions column should exist"))?,
        )?;
        let keys =
            as_string_array(dimensions.keys()).context("dimesions_keys as a string column")?;
        let values = as_dictionary_array::<Int32Type>(dimensions.values())?
            .downcast_dict::<BinaryArray>()
            .ok_or_else(|| anyhow::anyhow!("dimensions values should be binary"))?;
        let mut dimensions = HashMap::with_capacity(keys.len());
        for i in 0..keys.len() {
            let key = keys.value(i);
            let value = values.value(i);
            dimensions.insert(key.to_string(), value.to_vec());
        }
        Ok(Some(StreamState {
            id,
            event_cid: Cid::read_bytes(event_cid.value(0)).context("event_cid as a CID")?,
            controller: controller.value(0).to_string(),
            dimensions,
            data: data.to_vec(),
        }))
    }
}

#[async_trait]
impl FeedTableSource for AggregatorHandle {
    fn schema(&self) -> SchemaRef {
        schemas::event_states()
    }
    async fn subscribe_since(
        &self,
        projection: Option<Vec<usize>>,
        filters: Option<Vec<Expr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        Ok(self
            .send(SubscribeSinceMsg {
                projection,
                filters,
                limit,
            })
            .await??)
    }
}

// Small wrapper container around the data/state fields to hold
// other mutable metadata for the event.
// This is specific to Models and Model Instance Documents.
// Metadata is considered to be mutable from event to event and an overwriting merge is performed
// with the previous metadata to the current metadata.
// This means if a metadata key is missing it is propogated forward until a new data event changes
// its value.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventDataContainer<D> {
    metadata: BTreeMap<String, serde_json::Value>,
    content: D,
}

fn bytes_value_at(builder: &BinaryBuilder, idx: usize) -> &[u8] {
    let start = builder.offsets_slice()[idx] as usize;
    let stop = if idx < builder.len() {
        builder.offsets_slice()[idx + 1] as usize
    } else {
        builder.values_slice().len()
    };
    &builder.values_slice()[start..stop]
}
fn u32_value_at(builder: &UInt32Builder, idx: usize) -> u32 {
    builder.values_slice()[idx]
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use super::*;

    use ::object_store::ObjectStore;
    use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
    use arrow_schema::DataType;
    use ceramic_core::{StreamIdType, METAMODEL_STREAM_ID};
    use cid::Cid;
    use datafusion::{logical_expr::cast, prelude::SessionContext};
    use expect_test::expect;
    use futures::stream;
    use int_enum::IntEnum as _;
    use mockall::predicate;
    use object_store::memory::InMemory;
    use prometheus_client::registry::Registry;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use test_log::test;
    use validation::model::{ModelAccountRelationV2, ModelDefinition};

    use crate::{
        cid_string::cid_string,
        concluder::{mock::MockConcluder, TimeProof},
        conclusion_events_to_record_batch, pipeline_ctx,
        tests::TestContext,
        ConclusionData, ConclusionEvent, ConclusionInit, ConclusionTime,
    };

    async fn init_with_cache(
        max_cached_rows: Option<usize>,
    ) -> anyhow::Result<TestContext<AggregatorHandle>> {
        let mut mock_concluder = MockConcluder::new();
        mock_concluder
            .expect_handle_subscribe_since()
            .once()
            .return_once(|_msg| {
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    schemas::conclusion_events(),
                    stream::empty(),
                )))
            });
        init_with_concluder(mock_concluder, max_cached_rows).await
    }

    async fn init() -> anyhow::Result<TestContext<AggregatorHandle>> {
        let mut mock_concluder = MockConcluder::new();
        mock_concluder
            .expect_handle_subscribe_since()
            .once()
            .return_once(|_msg| {
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    schemas::conclusion_events(),
                    stream::empty(),
                )))
            });
        init_with_concluder(mock_concluder, None).await
    }

    async fn init_with_concluder(
        mock_concluder: MockConcluder,
        max_cached_rows: Option<usize>,
    ) -> anyhow::Result<TestContext<AggregatorHandle>> {
        init_with_object_store(mock_concluder, Arc::new(InMemory::new()), max_cached_rows).await
    }
    async fn init_with_object_store(
        mock_concluder: MockConcluder,
        object_store: Arc<dyn ObjectStore>,
        max_cached_rows: Option<usize>,
    ) -> anyhow::Result<TestContext<AggregatorHandle>> {
        let metrics = Metrics::register(&mut Registry::default());
        let shutdown = Shutdown::new();
        let pipeline_ctx = pipeline_ctx(object_store).await?;
        let (aggregator, handles) = Aggregator::spawn_new(
            1_000,
            &pipeline_ctx,
            max_cached_rows,
            MockConcluder::spawn(mock_concluder),
            metrics,
            shutdown.clone(),
        )
        .await?;
        Ok(TestContext::new(shutdown, handles, aggregator))
    }

    async fn do_test(conclusion_events: RecordBatch) -> anyhow::Result<impl std::fmt::Display> {
        let ctx = init().await?;
        let r = do_pass(ctx.actor_handle.clone(), None, Some(conclusion_events)).await;
        ctx.shutdown().await?;
        r
    }
    async fn do_test_since(
        conclusion_events: RecordBatch,
        offset: Option<u64>,
    ) -> anyhow::Result<impl std::fmt::Display> {
        let ctx = init().await?;
        let r = do_pass(ctx.actor_handle.clone(), offset, Some(conclusion_events)).await;
        ctx.shutdown().await?;
        r
    }

    async fn do_pass(
        aggregator: AggregatorHandle,
        offset: Option<u64>,
        conclusion_events: Option<RecordBatch>,
    ) -> anyhow::Result<impl std::fmt::Display> {
        let mut subscription = aggregator
            .send(SubscribeSinceMsg {
                projection: None,
                filters: offset.map(|o| vec![gt_expression("event_state_order", o)]),
                limit: None,
            })
            .await??;
        if let Some(conclusion_events) = conclusion_events {
            aggregator
                .send(NewConclusionEventsMsg {
                    events: conclusion_events,
                })
                .await??;
        }
        let batch = subscription
            .try_next()
            .await?
            .ok_or_else(|| anyhow::anyhow!("no events found"))?;

        pretty_event_states(vec![batch]).await
    }

    async fn pretty_event_states(
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> anyhow::Result<impl std::fmt::Display> {
        let event_states = SessionContext::new()
            .read_batches(batches)
            .context("read")?
            .select(vec![
                col("event_state_order"),
                cid_string(col("stream_cid")).alias("stream_cid"),
                col("stream_type"),
                col("controller"),
                col("dimensions"),
                cid_string(col("event_cid")).alias("event_cid"),
                col("event_type"),
                col("event_height"),
                cast(col("data"), DataType::Utf8).alias("data"),
                col("validation_errors"),
                col("before"),
                col("chain_id"),
            ])
            .context("select")?
            .collect()
            .await
            .context("collect")?;
        pretty_format_batches(&event_states).context("format")
    }
    #[derive(Deserialize, Serialize, JsonSchema)]
    #[schemars(rename_all = "camelCase", deny_unknown_fields)]
    pub struct SmallModel {
        creator: String,
        red: i32,
        green: i32,
        blue: i32,
    }
    fn model_and_mids_events() -> Vec<ConclusionEvent> {
        let model_def = ModelDefinition::new_v2::<SmallModel>(
            "TestSmallModel".to_owned(),
            None,
            false,
            None,
            None,
            ModelAccountRelationV2::List,
        )
        .unwrap();

        // NOTE: These CIDs and StreamIDs are fake and do not represent the actual hash of the data.
        // This makes testing easier as changing the contents does not mean you need to update all of
        // the cids.
        let model_stream_id =
            StreamId::from_str("k2t6wz4yhfp1pc9l42mm6vh20xmhm9ac7cznnpu4xcxe4jds13l9sjknm1accd")
                .unwrap();
        let instance_stream_id =
            StreamId::from_str("k2t6wzhjp5kk11gmt3clbfsplu6dnug4gxix1mj86w9p0ddwv3hdehc9mjawx7")
                .unwrap();
        let stream_init = ConclusionInit {
            stream_cid: instance_stream_id.cid,
            stream_type: StreamIdType::ModelInstanceDocument as u8,
            controller: "did:key:alice".to_owned(),
            dimensions: vec![
                ("controller".to_owned(), b"did:key:alice".to_vec()),
                ("model".to_owned(), model_stream_id.to_vec()),
                ("unique".to_owned(), b"wgk53".to_vec()),
            ],
        };
        vec![
            ConclusionEvent::Data(ConclusionData {
                order: 0,
                event_cid: model_stream_id.cid,
                init: ConclusionInit {
                    stream_cid: model_stream_id.cid,
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:bob".to_owned(),
                    dimensions: vec![
                        ("controller".to_owned(), b"did:key:bob".to_vec()),
                        ("model".to_owned(), METAMODEL_STREAM_ID.to_vec()),
                    ],
                },
                previous: vec![],
                data: serde_json::to_string(&json!({
                    "metadata":{
                        "foo":1,
                        "shouldIndex":true
                    },
                    "content": model_def,
                }))
                .unwrap()
                .into(),
            }),
            ConclusionEvent::Data(ConclusionData {
                order: 1,
                event_cid: Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )
                .unwrap(),
                init: stream_init.clone(),
                previous: vec![],
                data: serde_json::to_string(&json!({
                "metadata":{
                    "foo":1,
                    "shouldIndex":true
                },
                "content":{
                    "creator":"alice",
                    "red":255,
                    "green":255,
                    "blue":255
                }}))
                .unwrap()
                .into(),
            }),
            ConclusionEvent::Time(ConclusionTime {
                order: 2,
                event_cid: Cid::from_str(
                    "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                )
                .unwrap(),
                init: stream_init.clone(),
                previous: vec![Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )
                .unwrap()],
                time_proof: TimeProof {
                    before: 1744383131980,
                    chain_id: "test:chain".to_owned(),
                },
            }),
            ConclusionEvent::Data(ConclusionData {
                order: 3,
                event_cid: Cid::from_str(
                    "baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em",
                )
                .unwrap(),
                init: stream_init.clone(),
                previous: vec![Cid::from_str(
                    "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                )
                .unwrap()],
                data: serde_json::to_string(&json!({
                "metadata":{"foo":2},
                "content":[{
                    "op":"replace",
                    "path": "/red",
                    "value":0
                }]}))
                .unwrap()
                .into(),
            }),
        ]
    }
    // Append a data event to the list of events with the give data that chains off the event at
    // the specified index.
    // NOTE: This always adds the data event at the end of the list.
    fn chain_data_event(
        events: &[ConclusionEvent],
        event: &ConclusionEvent,
        cid: Cid,
        data: &str,
    ) -> ConclusionEvent {
        ConclusionEvent::Data(ConclusionData {
            order: events.iter().map(|e| e.order()).max().unwrap_or(0) + 1,
            event_cid: cid,
            init: event.init().clone(),
            previous: vec![event.event_cid()],
            data: data.into(),
        })
    }
    // Append a data event to the list of events with the give data that chains off the last event.
    fn append_data(events: &mut Vec<ConclusionEvent>, cid: Cid, data: &str) {
        let event = chain_data_event(events, &events[events.len() - 1], cid, data);
        events.push(event);
    }
    // Append a data event that is an init event for the stream
    fn append_init(events: &mut Vec<ConclusionEvent>, cid: Cid, init: ConclusionInit, data: &str) {
        events.push(ConclusionEvent::Data(ConclusionData {
            order: events.iter().map(|e| e.order()).max().unwrap_or(0) + 1,
            event_cid: cid,
            init,
            previous: vec![],
            data: data.into(),
        }));
    }
    /// Creates a set of events that defines a model and an update.
    /// Returns the model stream id, model update cid, and the events.
    fn model_with_update() -> (StreamId, Cid, Vec<ConclusionEvent>) {
        let mut events = model_and_mids_events()[0..1].to_vec();
        let model_update =
            Cid::from_str("baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm").unwrap();
        append_data(
            &mut events,
            model_update,
            // Adds alpha property to model
            &serde_json::to_string(&json!({
            "metadata":{"foo":2},
            "content":[{
                "op":"add",
                "path": "/schema/properties/alpha",
                "value":{"type":"number"}
            }]}))
            .unwrap(),
        );
        (
            StreamId {
                r#type: StreamIdType::from_int(events[0].init().stream_type as u64).unwrap(),
                cid: events[0].init().stream_cid,
            },
            model_update,
            events,
        )
    }
    /// Creates a set of events that defines a model and an update.
    /// Returns the model stream id, model update cid, and the events.
    fn model_with_update_and_mid() -> (StreamId, Cid, Vec<ConclusionEvent>) {
        let mut events = model_and_mids_events()[0..2].to_vec();
        let model_update =
            Cid::from_str("baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm").unwrap();
        events.push(chain_data_event(
            &events,
            &events[0],
            model_update,
            // Adds alpha property to model
            &serde_json::to_string(&json!({
            "metadata":{"foo":2},
            "content":[{
                "op":"add",
                "path": "/schema/properties/alpha",
                "value":{"type":"number"}
            }]}))
            .unwrap(),
        ));
        (
            StreamId {
                r#type: StreamIdType::from_int(events[0].init().stream_type as u64).unwrap(),
                cid: events[0].init().stream_cid,
            },
            model_update,
            events,
        )
    }

    //---------------------------------------------------------------
    // Tests about updating mids with a list account relation
    //---------------------------------------------------------------

    #[test(tokio::test)]
    async fn single_init_event_simple() {
        let event_states = do_test(
            conclusion_events_to_record_batch(model_and_mids_events()[0..1].iter()).unwrap(),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller  | dimensions                                                                        | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631} | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn single_init_event_projection() {
        let ctx = init().await.unwrap();
        let conclusion_events =
            conclusion_events_to_record_batch(&[ConclusionEvent::Data(ConclusionData {
                order: 0,
                event_cid: Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )
                .unwrap(),
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )
                    .unwrap(),
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:bob".to_owned(),
                    dimensions: vec![
                        ("controller".to_owned(), b"did:key:bob".to_vec()),
                        ("model".to_owned(), METAMODEL_STREAM_ID.to_vec()),
                    ],
                },
                previous: vec![],
                data: serde_json::to_string(&json!({
                    "metadata":{
                        "foo":1,"shouldIndex":true
                    },
                    "content":{"a":0}
                }))
                .unwrap()
                .into(),
            })])
            .unwrap();

        let mut subscription = ctx
            .actor_handle
            .send(SubscribeSinceMsg {
                projection: Some(vec![1, 3, 4]),
                filters: None,
                limit: None,
            })
            .await
            .unwrap()
            .unwrap();
        ctx.actor_handle
            .send(NewConclusionEventsMsg {
                events: conclusion_events,
            })
            .await
            .unwrap()
            .unwrap();
        let batch = subscription
            .try_next()
            .await
            .unwrap()
            .ok_or_else(|| anyhow::anyhow!("no events found"))
            .unwrap();
        let event_states = pretty_format_batches(&[batch]).unwrap();
        expect![[r#"
            +-------------------+-------------+-------------+
            | event_state_order | stream_type | controller  |
            +-------------------+-------------+-------------+
            | 1                 | 2           | did:key:bob |
            +-------------------+-------------+-------------+"#]]
        .assert_eq(&event_states.to_string());
        ctx.shutdown().await.unwrap();
    }
    #[test(tokio::test)]
    async fn multiple_data_and_time_events() {
        let event_states =
            do_test(conclusion_events_to_record_batch(&model_and_mids_events()).unwrap())
                .await
                .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before        | chain_id   |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |               |            |
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |               |            |
            | 3                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | 1            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                | 1744383131980 | test:chain |
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 2            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    | []                |               |            |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn multiple_single_event_passes() {
        let events = model_and_mids_events();
        // Test multiple passes where a single event for the stream is present in the conclusion
        // events for each pass.
        let ctx = init().await.unwrap();
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            None,
            Some(conclusion_events_to_record_batch(&events[0..1]).unwrap()),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller  | dimensions                                                                        | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631} | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            Some(1),
            Some(conclusion_events_to_record_batch(&events[1..2]).unwrap()),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+----------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                     | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+----------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}} | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+----------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            Some(2),
            Some(conclusion_events_to_record_batch(&events[2..3]).unwrap()),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+----------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                     | validation_errors | before        | chain_id   |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+----------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | 3                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | 1            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}} | []                | 1744383131980 | test:chain |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+----------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+"#]].assert_eq(&event_states.to_string());
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            Some(3),
            Some(conclusion_events_to_record_batch(&events[3..]).unwrap()),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+--------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                   | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+--------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 2            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}} | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+--------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
        ctx.shutdown().await.unwrap();
    }
    #[test(tokio::test)]
    async fn multiple_passes() {
        let events = model_and_mids_events();
        let ctx = init().await.unwrap();
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            None,
            Some(conclusion_events_to_record_batch(&events[0..1]).unwrap()),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller  | dimensions                                                                        | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631} | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            Some(1),
            Some(conclusion_events_to_record_batch(&events[1..]).unwrap()),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+----------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                     | validation_errors | before        | chain_id   |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+----------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}} | []                |               |            |
            | 3                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | 1            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}} | []                | 1744383131980 | test:chain |
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 2            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}   | []                |               |            |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+----------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+"#]].assert_eq(&event_states.to_string());
        ctx.shutdown().await.unwrap();
    }
    #[test(tokio::test)]
    async fn invalid_instance_data() {
        let mut events = model_and_mids_events();
        append_data(
            &mut events,
            Cid::from_str("baeabeicklrck72l22zzc324b7ordbhq2amnhhivabosngfkbhk2nmuyqom").unwrap(),
            &serde_json::to_string(&json!({
            "metadata":{"foo":2},
            "content":[{
                "op":"add",
                "path": "/color",
                "value":"00FFFF"
            }]}))
            .unwrap(),
        );
        let event_states = do_test(conclusion_events_to_record_batch(&events).unwrap())
            .await
            .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------+---------------+------------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors                                                | before        | chain_id   |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------+---------------+------------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                                                               |               |            |
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                                                               |               |            |
            | 3                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | 1            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                                                               | 1744383131980 | test:chain |
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 2            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    | []                                                               |               |            |
            | 5                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeicklrck72l22zzc324b7ordbhq2amnhhivabosngfkbhk2nmuyqom | 0          | 3            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"color":"00FFFF","creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                   | [Additional properties are not allowed ('color' was unexpected)] |               |            |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------+---------------+------------+"#]].assert_eq(&event_states.to_string());
    }

    #[test(tokio::test)]
    async fn event_height_branches() {
        //Test that event height is computed correctly with branches
        let mut events = model_and_mids_events();
        events.push(chain_data_event(
            &events,
            &events[1],
            Cid::from_str("baeabeifiqq7nc3ok6wvvvoruf6ednyhwc5jjmbn7ks5kglc6dtchqai6vu").unwrap(),
            &serde_json::to_string(&json!( {
                "metadata": { },
                "content": [{
                    "op":"replace",
                    "path":"/blue",
                    "value":127,
                }]
            }))
            .unwrap(),
        ));
        events.push(chain_data_event(
            &events,
            &events[3],
            Cid::from_str("baeabeic5wirrpbvrghm4icaoezllghar5v4z6usf6zqepfwj7hxznvwh3e").unwrap(),
            &serde_json::to_string(&json!( {
                "metadata": { },
                "content": [{
                    "op":"replace",
                    "path":"/blue",
                    "value":0,
                }]
            }))
            .unwrap(),
        ));
        let event_states = do_test(conclusion_events_to_record_batch(&events).unwrap())
            .await
            .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before        | chain_id   |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |               |            |
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |               |            |
            | 3                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeifiqq7nc3ok6wvvvoruf6ednyhwc5jjmbn7ks5kglc6dtchqai6vu | 0          | 1            | {"metadata":{"foo":1,"shouldIndex":true},"content":{"blue":127,"creator":"alice","green":255,"red":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |               |            |
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | 1            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                | 1744383131980 | test:chain |
            | 5                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 2            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    | []                |               |            |
            | 6                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeic5wirrpbvrghm4icaoezllghar5v4z6usf6zqepfwj7hxznvwh3e | 0          | 3            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":0,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                      | []                |               |            |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+"#]].assert_eq(&event_states.to_string());
    }

    #[test(tokio::test)]
    async fn instance_before_model_single_pass() {
        // Test that an instance can occur before its model and be correctly reordered when they are
        // in the same batch.
        let mut events = model_and_mids_events();
        // Move model event after instance events
        events.swap(0, 2);

        let event_states = do_test(conclusion_events_to_record_batch(&events).unwrap())
            .await
            .unwrap();
        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before        | chain_id   |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |               |            |
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |               |            |
            | 3                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | 1            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                | 1744383131980 | test:chain |
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 2            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    | []                |               |            |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn instance_before_model_multiple_passes() {
        // Test that an instance can occur before its model and be correctly reordered when they are
        // in separate batches.
        let events = model_and_mids_events();
        let ctx = init().await.unwrap();

        // Send events, do no expect to get any new events back as nothing can be processes yet.
        ctx.actor_handle
            .send(NewConclusionEventsMsg {
                events: conclusion_events_to_record_batch(&events[1..2]).unwrap(),
            })
            .await
            .unwrap()
            .unwrap();
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            None,
            Some(conclusion_events_to_record_batch(&events[0..1]).unwrap()),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |        |          |
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            Some(2),
            Some(conclusion_events_to_record_batch(&events[2..]).unwrap()),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+----------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                     | validation_errors | before        | chain_id   |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+----------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | 3                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | 1            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}} | []                | 1744383131980 | test:chain |
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 2            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}   | []                |               |            |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+----------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+"#]].assert_eq(&event_states.to_string());
        ctx.shutdown().await.unwrap();
    }

    //---------------------------------------------------------------
    // Tests about the mechanisms of the aggregator that keep data flowing
    //---------------------------------------------------------------

    #[test(tokio::test)]
    async fn sub_concluder() {
        // This test ensures that the logic that polls the concluder actor to check for new events
        // finds them and passes them into the aggregator actor.
        let conclusion_events = conclusion_events_to_record_batch(&model_and_mids_events());

        // Setup mock that returns the conclusion_events
        let mut mock_concluder = MockConcluder::new();
        mock_concluder
            .expect_handle_subscribe_since()
            .once()
            .return_once(|_msg| {
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    schemas::conclusion_events(),
                    stream::iter(conclusion_events.into_iter().map(Ok)),
                )))
            });

        let ctx = init_with_concluder(mock_concluder, None).await.unwrap();
        let event_states = do_pass(ctx.actor_handle.clone(), None, None).await.unwrap();
        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before        | chain_id   |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |               |            |
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |               |            |
            | 3                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | 1            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                | 1744383131980 | test:chain |
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 2            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    | []                |               |            |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+"#]].assert_eq(&event_states.to_string());
        ctx.shutdown().await.unwrap();
    }

    #[test(tokio::test)]
    async fn sub_concluder_preexisting_from_beginning() {
        // wrap it so it times out and we see logs if expectations are wrong
        async fn run_test() {
            // This test ensures that the logic that polls the concluder actor starts at the correct
            // offset when there is preexisting data.

            let events = model_and_mids_events();
            // Use the same object store in both instances of the actor as that is where data is
            // physically stored.
            let object_store = Arc::new(InMemory::new());
            {
                let first_events = conclusion_events_to_record_batch(&events[0..2]);

                // Setup mock that returns the conclusion_events
                let mut mock_concluder = MockConcluder::new();
                mock_concluder
                    .expect_handle_subscribe_since()
                    .once()
                    .with(predicate::eq(SubscribeSinceMsg {
                        projection: None,
                        filters: None,
                        limit: None,
                    }))
                    .return_once(|_msg| {
                        Ok(Box::pin(RecordBatchStreamAdapter::new(
                            schemas::conclusion_events(),
                            stream::iter(first_events.into_iter().map(Ok)),
                        )))
                    });

                let ctx = init_with_object_store(
                    mock_concluder,
                    object_store.clone(),
                    // Set the max_cached_rows to zero so that all rows are written to the object store
                    Some(0),
                )
                .await
                .unwrap();
                let event_states = do_pass(ctx.actor_handle.clone(), None, None).await.unwrap();
                expect![[r#"
                +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
                | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before | chain_id |
                +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
                | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |        |          |
                | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |        |          |
                +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
                ctx.shutdown().await.unwrap();
            }

            // Now we have existing data, start a new aggregator actor and ensure that it starts
            // where it left off.
            // Setup mock that returns a second batch of conclusion_events
            let second_events = conclusion_events_to_record_batch(&events[2..]);
            let mut mock_concluder = MockConcluder::new();
            mock_concluder
                .expect_handle_subscribe_since()
                .once()
                .with(predicate::eq(SubscribeSinceMsg {
                    projection: None,
                    filters: Some(vec![gt_expression("conclusion_event_order", 1)]),
                    limit: None,
                }))
                .return_once(|_msg| {
                    Ok(Box::pin(RecordBatchStreamAdapter::new(
                        schemas::conclusion_events(),
                        stream::iter(second_events.into_iter().map(Ok)),
                    )))
                });

            let ctx = init_with_object_store(mock_concluder, object_store, None)
                .await
                .unwrap();

            let mut subscription = ctx
                .actor_handle
                .send(SubscribeSinceMsg {
                    projection: None,
                    filters: None,
                    limit: Some(4),
                })
                .await
                .unwrap()
                .unwrap();
            let mut batches = Vec::new();
            while let Some(batch) = subscription.try_next().await.unwrap() {
                batches.push(batch)
            }
            let event_states = pretty_event_states(batches).await.unwrap();
            expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before        | chain_id   |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |               |            |
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |               |            |
            | 3                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | 1            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                | 1744383131980 | test:chain |
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 2            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    | []                |               |            |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+"#]].assert_eq(&event_states.to_string());
            ctx.shutdown().await.unwrap();
        }

        tokio::time::timeout(std::time::Duration::from_secs(3), run_test())
            .await
            .unwrap();
    }

    #[test(tokio::test)]
    async fn subscribe_aggregator_limit() {
        let ctx = init().await.unwrap();

        let conclusion_events =
            conclusion_events_to_record_batch(&model_and_mids_events()).unwrap();

        let mut subscription = ctx
            .actor_handle
            .send(SubscribeSinceMsg {
                projection: None,
                filters: None,
                limit: Some(2),
            })
            .await
            .unwrap()
            .unwrap();
        ctx.actor_handle
            .send(NewConclusionEventsMsg {
                events: conclusion_events,
            })
            .await
            .unwrap()
            .unwrap();
        let batch = subscription.try_next().await.unwrap().unwrap();
        let event_states = pretty_event_states(vec![batch]).await.unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |        |          |
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
        ctx.shutdown().await.unwrap();
    }

    //---------------------------------------------------------------
    // Tests about the StreamState message for the aggregator
    //---------------------------------------------------------------

    #[test(tokio::test)]
    async fn stream_state() {
        let ctx = init().await.unwrap();

        let events = model_and_mids_events();
        ctx.actor_handle
            .send(NewConclusionEventsMsg {
                events: conclusion_events_to_record_batch(&events).unwrap(),
            })
            .await
            .unwrap()
            .unwrap();

        let state = ctx
            .actor_handle
            .send(StreamStateMsg {
                id: StreamId::from_str(
                    "k2t6wzhjp5kk11gmt3clbfsplu6dnug4gxix1mj86w9p0ddwv3hdehc9mjawx7",
                )
                .unwrap(),
            })
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        expect![[r#"
            StreamState {
                id: "k2t6wzhjp5kk11gmt3clbfsplu6dnug4gxix1mj86w9p0ddwv3hdehc9mjawx7",
                event_cid: "baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em",
                controller: "did:key:alice",
                dimensions: {
                    "controller": "uZGlkOmtleTphbGljZQ",
                    "model": "uzgECAQASIICcVHDjY15JX1qYQ33mFrZhLaizdT_C7jSoMkq2hYX9",
                    "unique": "ud2drNTM",
                },
                data: "ueyJtZXRhZGF0YSI6eyJmb28iOjIsInNob3VsZEluZGV4Ijp0cnVlfSwiY29udGVudCI6eyJibHVlIjoyNTUsImNyZWF0b3IiOiJhbGljZSIsImdyZWVuIjoyNTUsInJlZCI6MH19",
            }
        "#]].assert_debug_eq(&state);
        expect![[r#"{"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}"#]]
            .assert_eq(std::str::from_utf8(&state.data).unwrap());

        ctx.shutdown().await.unwrap();
    }

    #[test(tokio::test)]
    async fn stream_state_invalid() {
        // Test that only valid stream states are returned.

        let ctx = init().await.unwrap();

        let mut events = model_and_mids_events();
        append_data(
            &mut events,
            Cid::from_str("baeabeicklrck72l22zzc324b7ordbhq2amnhhivabosngfkbhk2nmuyqom").unwrap(),
            &serde_json::to_string(&json!({
            "metadata":{},
            "content":[{
                "op":"add",
                "path": "/color",
                "value":"00FFFF"
            }]}))
            .unwrap(),
        );
        ctx.actor_handle
            .send(NewConclusionEventsMsg {
                events: conclusion_events_to_record_batch(&events).unwrap(),
            })
            .await
            .unwrap()
            .unwrap();

        let state = ctx
            .actor_handle
            .send(StreamStateMsg {
                id: StreamId::from_str(
                    "k2t6wzhjp5kk11gmt3clbfsplu6dnug4gxix1mj86w9p0ddwv3hdehc9mjawx7",
                )
                .unwrap(),
            })
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        expect![[r#"
            StreamState {
                id: "k2t6wzhjp5kk11gmt3clbfsplu6dnug4gxix1mj86w9p0ddwv3hdehc9mjawx7",
                event_cid: "baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em",
                controller: "did:key:alice",
                dimensions: {
                    "controller": "uZGlkOmtleTphbGljZQ",
                    "model": "uzgECAQASIICcVHDjY15JX1qYQ33mFrZhLaizdT_C7jSoMkq2hYX9",
                    "unique": "ud2drNTM",
                },
                data: "ueyJtZXRhZGF0YSI6eyJmb28iOjIsInNob3VsZEluZGV4Ijp0cnVlfSwiY29udGVudCI6eyJibHVlIjoyNTUsImNyZWF0b3IiOiJhbGljZSIsImdyZWVuIjoyNTUsInJlZCI6MH19",
            }
        "#]].assert_debug_eq(&state);
        expect![[r#"{"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}"#]]
            .assert_eq(std::str::from_utf8(&state.data).unwrap());

        ctx.shutdown().await.unwrap();
    }

    #[test(tokio::test)]
    async fn stream_state_not_found() {
        let ctx = init().await.unwrap();

        let state = ctx
            .actor_handle
            .send(StreamStateMsg {
                id: StreamId {
                    r#type: StreamIdType::Model,
                    cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )
                    .unwrap(),
                },
            })
            .await
            .unwrap()
            .unwrap();
        assert!(state.is_none());
        ctx.shutdown().await.unwrap();
    }

    //---------------------------------------------------------------
    // Tests about updating model schemas
    //---------------------------------------------------------------

    #[test(tokio::test)]
    async fn update_model() {
        let mut events = model_and_mids_events()[0..1].to_vec();
        append_data(
            &mut events,
            Cid::from_str("baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm").unwrap(),
            // Adds alpha property to model
            &serde_json::to_string(&json!({
            "metadata":{"foo":2},
            "content":[{
                "op":"add",
                "path": "/schema/properties/alpha",
                "value":{"type":"integer","format":"int32"}
            }]}))
            .unwrap(),
        );
        let event_states = do_test(conclusion_events_to_record_batch(&events).unwrap())
            .await
            .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller  | dimensions                                                                        | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631} | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}}                                             | []                |        |          |
            | 2                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631} | baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm | 0          | 1            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"alpha":{"format":"int32","type":"integer"},"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}}} | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn invalid_update_model_new_required() {
        let mut events = model_and_mids_events()[0..1].to_vec();
        append_data(
            &mut events,
            Cid::from_str("baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm").unwrap(),
            // Adds alpha property to model
            &serde_json::to_string(&json!({
                "metadata":{ "foo":2 },
                "content":[
                    {
                        "op":"add",
                        "path": "/schema/properties/alpha",
                        "value":{"type":"integer","format":"int32"}
                    },
                    {
                        "op":"replace",
                        "path":"/schema/required",
                        "value":["creator","red","green","blue","alpha"]
                    }
                ]
            }))
            .unwrap(),
        );
        let event_states = do_test(conclusion_events_to_record_batch(&events).unwrap())
            .await
            .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller  | dimensions                                                                        | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | validation_errors                          | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------+--------+----------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631} | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}}                                                     | []                                         |        |          |
            | 2                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631} | baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm | 0          | 1            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"alpha":{"format":"int32","type":"integer"},"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue","alpha"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}}} | [Cannot add a new required property alpha] |        |          |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn update_model_new_instance_new_schema_new_model() {
        // Add a new mid stream using the new schema referencing the new model version.
        let (model_stream_id, model_update, mut events) = model_with_update();
        append_init(
            &mut events,
            Cid::from_str("baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe").unwrap(),
            ConclusionInit {
                stream_cid: model_stream_id.cid,
                stream_type: StreamIdType::ModelInstanceDocument as u8,
                controller: "did:key:jim".to_owned(),
                dimensions: vec![
                    ("controller".to_owned(), b"did:key:jim".to_vec()),
                    ("model".to_owned(), model_stream_id.to_vec()),
                    ("unique".to_owned(), b"thgfq".to_vec()),
                ],
            },
            &serde_json::to_string(&json!( {
                "metadata": { "modelVersion": model_update.to_string(), },
                "content": {
                    "creator":"jim",
                    "red":0,
                    "green":0,
                    "blue":255,
                    "alpha":0.75,
                }
            }))
            .unwrap(),
        );
        let event_states =
            do_test_since(conclusion_events_to_record_batch(&events).unwrap(), Some(2))
                .await
                .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller  | dimensions                                                                                                                                      | event_cid                                                   | event_type | event_height | data                                                                                                                                                              | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 3                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 3           | did:key:jim | {controller: 6469643a6b65793a6a696d, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 7468676671} | baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe | 0          | 0            | {"content":{"alpha":0.75,"blue":255,"creator":"jim","green":0,"red":0},"metadata":{"modelVersion":"baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm"}} | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn update_model_new_instance_old_schema_new_model() {
        // Add a new mid stream using the old schema referencing the new model version.
        let (model_stream_id, model_update, mut events) = model_with_update();
        append_init(
            &mut events,
            Cid::from_str("baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe").unwrap(),
            ConclusionInit {
                stream_cid: model_stream_id.cid,
                stream_type: StreamIdType::ModelInstanceDocument as u8,
                controller: "did:key:jim".to_owned(),
                dimensions: vec![
                    ("controller".to_owned(), b"did:key:jim".to_vec()),
                    ("model".to_owned(), model_stream_id.to_vec()),
                    ("unique".to_owned(), b"e7t2f".to_vec()),
                ],
            },
            &serde_json::to_string(&json!( {
                "metadata": { "modelVersion": model_update.to_string(), },
                "content": {
                    "creator":"jim",
                    "red":0,
                    "green":0,
                    "blue":255,
                }
            }))
            .unwrap(),
        );
        let event_states =
            do_test_since(conclusion_events_to_record_batch(&events).unwrap(), Some(2))
                .await
                .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller  | dimensions                                                                                                                                      | event_cid                                                   | event_type | event_height | data                                                                                                                                                 | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 3                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 3           | did:key:jim | {controller: 6469643a6b65793a6a696d, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 6537743266} | baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe | 0          | 0            | {"content":{"blue":255,"creator":"jim","green":0,"red":0},"metadata":{"modelVersion":"baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm"}} | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn update_model_new_instance_new_schema_old_model() {
        // Add a new mid stream using the new schema referencing old model version.
        let (model_stream_id, _model_update, mut events) = model_with_update();
        append_init(
            &mut events,
            Cid::from_str("baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe").unwrap(),
            ConclusionInit {
                stream_cid: model_stream_id.cid,
                stream_type: StreamIdType::ModelInstanceDocument as u8,
                controller: "did:key:jim".to_owned(),
                dimensions: vec![
                    ("controller".to_owned(), b"did:key:jim".to_vec()),
                    ("model".to_owned(), model_stream_id.to_vec()),
                    ("unique".to_owned(), b"j4q6r".to_vec()),
                ],
            },
            &serde_json::to_string(&json!( {
                "metadata": { },
                "content": {
                    "creator":"jim",
                    "red":0,
                    "green":0,
                    "blue":255,
                    "alpha":0.75
                }
            }))
            .unwrap(),
        );
        let event_states =
            do_test_since(conclusion_events_to_record_batch(&events).unwrap(), Some(2))
                .await
                .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+---------------------------------------------------------------------------------------+------------------------------------------------------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller  | dimensions                                                                                                                                      | event_cid                                                   | event_type | event_height | data                                                                                  | validation_errors                                                | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+---------------------------------------------------------------------------------------+------------------------------------------------------------------+--------+----------+
            | 3                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 3           | did:key:jim | {controller: 6469643a6b65793a6a696d, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 6a34713672} | baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe | 0          | 0            | {"content":{"alpha":0.75,"blue":255,"creator":"jim","green":0,"red":0},"metadata":{}} | [Additional properties are not allowed ('alpha' was unexpected)] |        |          |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+---------------------------------------------------------------------------------------+------------------------------------------------------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn update_model_new_instance_old_schema_old_model() {
        // Add a new mid stream using the old schema referencing the old model version.
        let (model_stream_id, _model_update, mut events) = model_with_update();
        append_init(
            &mut events,
            Cid::from_str("baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe").unwrap(),
            ConclusionInit {
                stream_cid: model_stream_id.cid,
                stream_type: StreamIdType::ModelInstanceDocument as u8,
                controller: "did:key:jim".to_owned(),
                dimensions: vec![
                    ("controller".to_owned(), b"did:key:jim".to_vec()),
                    ("model".to_owned(), model_stream_id.to_vec()),
                    ("unique".to_owned(), b"thgfq".to_vec()),
                ],
            },
            &serde_json::to_string(&json!( {
                "metadata": { },
                "content": {
                    "creator":"jim",
                    "red":0,
                    "green":0,
                    "blue":255,
                }
            }))
            .unwrap(),
        );
        let event_states =
            do_test_since(conclusion_events_to_record_batch(&events).unwrap(), Some(2))
                .await
                .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+--------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller  | dimensions                                                                                                                                      | event_cid                                                   | event_type | event_height | data                                                                     | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+--------------------------------------------------------------------------+-------------------+--------+----------+
            | 3                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 3           | did:key:jim | {controller: 6469643a6b65793a6a696d, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 7468676671} | baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe | 0          | 0            | {"content":{"blue":255,"creator":"jim","green":0,"red":0},"metadata":{}} | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+--------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn update_model_update_instance_new_schema_new_model() {
        // Update a mid stream using the new schema referencing the new model version.
        let (_model_stream_id, model_update, mut events) = model_with_update_and_mid();
        events.push(chain_data_event(
            &events,
            &events[1],
            Cid::from_str("baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe").unwrap(),
            &serde_json::to_string(&json!( {
                "metadata": { "modelVersion": model_update.to_string(), },
                "content": [{
                    "op":"add",
                    "path":"/alpha",
                    "value":0.75,
                }]
            }))
            .unwrap(),
        ));
        let event_states =
            do_test_since(conclusion_events_to_record_batch(&events).unwrap(), Some(3))
                .await
                .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                               | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe | 0          | 1            | {"metadata":{"foo":1,"modelVersion":"baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm","shouldIndex":true},"content":{"alpha":0.75,"blue":255,"creator":"alice","green":255,"red":255}} | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn update_model_update_instance_old_schema_new_model() {
        // Update a mid stream using the old schema referencing the new model version.
        let (_model_stream_id, model_update, mut events) = model_with_update_and_mid();
        events.push(chain_data_event(
            &events,
            &events[1],
            Cid::from_str("baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe").unwrap(),
            &serde_json::to_string(&json!( {
                "metadata": { "modelVersion": model_update.to_string(), },
                "content": [{
                    "op":"add",
                    "path":"/green",
                    "value":0,
                }]
            }))
            .unwrap(),
        ));
        let event_states =
            do_test_since(conclusion_events_to_record_batch(&events).unwrap(), Some(3))
                .await
                .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe | 0          | 1            | {"metadata":{"foo":1,"modelVersion":"baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm","shouldIndex":true},"content":{"blue":255,"creator":"alice","green":0,"red":255}} | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn update_model_update_instance_new_schema_old_model() {
        // Update a mid stream using the new schema referencing the old model version.
        let (_model_stream_id, _model_update, mut events) = model_with_update_and_mid();
        events.push(chain_data_event(
            &events,
            &events[1],
            Cid::from_str("baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe").unwrap(),
            &serde_json::to_string(&json!( {
                "metadata": { },
                "content": [{
                    "op":"add",
                    "path":"/alpha",
                    "value":0.75,
                }]
            }))
            .unwrap(),
        ));
        let event_states =
            do_test_since(conclusion_events_to_record_batch(&events).unwrap(), Some(3))
                .await
                .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                  | validation_errors                                                | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------+--------+----------+
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe | 0          | 1            | {"metadata":{"foo":1,"shouldIndex":true},"content":{"alpha":0.75,"blue":255,"creator":"alice","green":255,"red":255}} | [Additional properties are not allowed ('alpha' was unexpected)] |        |          |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn update_model_update_instance_old_schema_old_model() {
        // Update a mid stream using the old schema referencing the old model version.
        let (_model_stream_id, _model_update, mut events) = model_with_update_and_mid();
        events.push(chain_data_event(
            &events,
            &events[1],
            Cid::from_str("baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe").unwrap(),
            &serde_json::to_string(&json!( {
                "metadata": { },
                "content": [{
                    "op":"add",
                    "path":"/green",
                    "value":0,
                }]
            }))
            .unwrap(),
        ));
        let event_states =
            do_test_since(conclusion_events_to_record_batch(&events).unwrap(), Some(3))
                .await
                .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+--------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                   | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+--------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe | 0          | 1            | {"metadata":{"foo":1,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":0,"red":255}} | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+--------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }

    //---------------------------------------------------------------
    // Tests about updating mids with different account relations
    //---------------------------------------------------------------

    #[test(tokio::test)]
    async fn update_single_relation_init_without_content() {
        let model_def = ModelDefinition::new_v2::<SmallModel>(
            "TestSmallModel".to_owned(),
            None,
            false,
            None,
            None,
            ModelAccountRelationV2::Single,
        )
        .unwrap();

        // NOTE: These CIDs and StreamIDs are fake and do not represent the actual hash of the data.
        // This makes testing easier as changing the contents does not mean you need to update all of
        // the cids.
        let model_stream_id =
            StreamId::from_str("k2t6wz4yhfp1ndie7o9o4en4qc6jic9d4p9vggb0bpjt41sxl905ak2ff4h6pk")
                .unwrap();
        let instance_stream_id =
            StreamId::from_str("k2t6wz4yhfp1s5jhjhtelcqvwaomcgretwvdx7gz3cp66i9z8fsm8m743k1vue")
                .unwrap();
        let stream_init = ConclusionInit {
            stream_cid: instance_stream_id.cid,
            stream_type: StreamIdType::ModelInstanceDocument as u8,
            controller: "did:key:alice".to_owned(),
            dimensions: vec![
                ("controller".to_owned(), b"did:key:alice".to_vec()),
                ("model".to_owned(), model_stream_id.to_vec()),
            ],
        };
        let event_states = do_test(
            conclusion_events_to_record_batch(&vec![
                ConclusionEvent::Data(ConclusionData {
                    order: 0,
                    event_cid: model_stream_id.cid,
                    init: ConclusionInit {
                        stream_cid: model_stream_id.cid,
                        stream_type: StreamIdType::Model as u8,
                        controller: "did:key:bob".to_owned(),
                        dimensions: vec![
                            ("controller".to_owned(), b"did:key:bob".to_vec()),
                            ("model".to_owned(), METAMODEL_STREAM_ID.to_vec()),
                        ],
                    },
                    previous: vec![],
                    data: serde_json::to_string(&json!({
                        "metadata":{ },
                        "content": model_def,
                    }))
                    .unwrap()
                    .into(),
                }),
                ConclusionEvent::Data(ConclusionData {
                    order: 1,
                    event_cid: Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )
                    .unwrap(),
                    init: stream_init.clone(),
                    previous: vec![],
                    data: serde_json::to_string(&json!({
                    "metadata":{ },
                    "content": serde_json::Value::Null,
                    }))
                    .unwrap()
                    .into(),
                }),
                ConclusionEvent::Data(ConclusionData {
                    order: 2,
                    event_cid: Cid::from_str(
                        "baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em",
                    )
                    .unwrap(),
                    init: stream_init.clone(),
                    previous: vec![Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )
                    .unwrap()],
                    data: serde_json::to_string(&json!({
                    "metadata":{},
                    "content":[
                        {
                            "op":"add",
                            "path": "/creator",
                            "value":"sally"
                        },
                        {
                            "op":"add",
                            "path": "/red",
                            "value": 255,
                        },
                        {
                            "op":"add",
                            "path": "/green",
                            "value": 255,
                        },
                        {
                            "op":"add",
                            "path": "/blue",
                            "value": 255,
                        },
                    ]}))
                    .unwrap()
                    .into(),
                }),
            ])
            .unwrap(),
        )
        .await
        .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+---------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                      | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+---------------+---------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 1                 | baeabeibrydyefwuzizf6s32lbuu27rpjibrbtsziqukscbnuo3y52pgmha | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                               | baeabeibrydyefwuzizf6s32lbuu27rpjibrbtsziqukscbnuo3y52pgmha | 0          | 0            | {"content":{"accountRelation":{"type":"single"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{}} | []                |        |          |
            | 2                 | baeabeihrpnqmppjqdohl2stvfevu7jn57zjqzqb4dflbl4ettogmt56xoy | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce01020100122031c0f042da99464be96f4b0d29afc5e9406219cb2885152105b476f1dd3ccc38} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":null,"metadata":{}}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | []                |        |          |
            | 3                 | baeabeihrpnqmppjqdohl2stvfevu7jn57zjqzqb4dflbl4ettogmt56xoy | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce01020100122031c0f042da99464be96f4b0d29afc5e9406219cb2885152105b476f1dd3ccc38} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 1            | {"metadata":{},"content":{"blue":255,"creator":"sally","green":255,"red":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+---------------+---------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn update_single_relation_init_with_content() {
        let model_def = ModelDefinition::new_v2::<SmallModel>(
            "TestSmallModel".to_owned(),
            None,
            false,
            None,
            None,
            ModelAccountRelationV2::Single,
        )
        .unwrap();

        // NOTE: These CIDs and StreamIDs are fake and do not represent the actual hash of the data.
        // This makes testing easier as changing the contents does not mean you need to update all of
        // the cids.
        let model_stream_id =
            StreamId::from_str("k2t6wz4yhfp1ndie7o9o4en4qc6jic9d4p9vggb0bpjt41sxl905ak2ff4h6pk")
                .unwrap();
        let instance_stream_id =
            StreamId::from_str("k2t6wz4yhfp1s5jhjhtelcqvwaomcgretwvdx7gz3cp66i9z8fsm8m743k1vue")
                .unwrap();
        let stream_init = ConclusionInit {
            stream_cid: instance_stream_id.cid,
            stream_type: StreamIdType::ModelInstanceDocument as u8,
            controller: "did:key:alice".to_owned(),
            dimensions: vec![
                ("controller".to_owned(), b"did:key:alice".to_vec()),
                ("model".to_owned(), model_stream_id.to_vec()),
            ],
        };
        let event_states = do_test(
            conclusion_events_to_record_batch(&vec![
                ConclusionEvent::Data(ConclusionData {
                    order: 0,
                    event_cid: model_stream_id.cid,
                    init: ConclusionInit {
                        stream_cid: model_stream_id.cid,
                        stream_type: StreamIdType::Model as u8,
                        controller: "did:key:bob".to_owned(),
                        dimensions: vec![
                            ("controller".to_owned(), b"did:key:bob".to_vec()),
                            ("model".to_owned(), METAMODEL_STREAM_ID.to_vec()),
                        ],
                    },
                    previous: vec![],
                    data: serde_json::to_string(&json!({
                        "metadata":{ },
                        "content": model_def,
                    }))
                    .unwrap()
                    .into(),
                }),
                ConclusionEvent::Data(ConclusionData {
                    order: 1,
                    event_cid: Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )
                    .unwrap(),
                    init: stream_init.clone(),
                    previous: vec![],
                    data: serde_json::to_string(&json!({
                    "metadata":{ },
                    "content":{
                        "creator":"alice",
                        "red":255,
                        "green":255,
                        "blue":255
                    }}))
                    .unwrap()
                    .into(),
                }),
                ConclusionEvent::Data(ConclusionData {
                    order: 2,
                    event_cid: Cid::from_str(
                        "baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em",
                    )
                    .unwrap(),
                    init: stream_init.clone(),
                    previous: vec![Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )
                    .unwrap()],
                    data: serde_json::to_string(&json!({
                    "metadata":{},
                    "content":[{
                        "op":"replace",
                        "path": "/creator",
                        "value":"sally"
                    }]}))
                    .unwrap()
                    .into(),
                }),
            ])
            .unwrap(),
        )
        .await
        .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+---------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                      | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | validation_errors                                                                      | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+---------------+---------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------+--------+----------+
            | 1                 | baeabeibrydyefwuzizf6s32lbuu27rpjibrbtsziqukscbnuo3y52pgmha | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                               | baeabeibrydyefwuzizf6s32lbuu27rpjibrbtsziqukscbnuo3y52pgmha | 0          | 0            | {"content":{"accountRelation":{"type":"single"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{}} | []                                                                                     |        |          |
            | 2                 | baeabeihrpnqmppjqdohl2stvfevu7jn57zjqzqb4dflbl4ettogmt56xoy | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce01020100122031c0f042da99464be96f4b0d29afc5e9406219cb2885152105b476f1dd3ccc38} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    | [ModelInstanceDocuments with an account relation Single must not have initial content] |        |          |
            | 3                 | baeabeihrpnqmppjqdohl2stvfevu7jn57zjqzqb4dflbl4ettogmt56xoy | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce01020100122031c0f042da99464be96f4b0d29afc5e9406219cb2885152105b476f1dd3ccc38} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 1            | {"metadata":{},"content":{"blue":255,"creator":"sally","green":255,"red":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    | []                                                                                     |        |          |
            +-------------------+-------------------------------------------------------------+-------------+---------------+---------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn update_list_relation_locked_fields() {
        let model_def = ModelDefinition::new_v2::<SmallModel>(
            "TestSmallModel".to_owned(),
            None,
            false,
            None,
            Some(vec!["creator".to_owned()]),
            ModelAccountRelationV2::List,
        )
        .unwrap();

        // NOTE: These CIDs and StreamIDs are fake and do not represent the actual hash of the data.
        // This makes testing easier as changing the contents does not mean you need to update all of
        // the cids.
        let model_stream_id =
            StreamId::from_str("k2t6wz4yhfp1ndie7o9o4en4qc6jic9d4p9vggb0bpjt41sxl905ak2ff4h6pk")
                .unwrap();
        let instance_stream_id =
            StreamId::from_str("k2t6wz4yhfp1s5jhjhtelcqvwaomcgretwvdx7gz3cp66i9z8fsm8m743k1vue")
                .unwrap();
        let stream_init = ConclusionInit {
            stream_cid: instance_stream_id.cid,
            stream_type: StreamIdType::ModelInstanceDocument as u8,
            controller: "did:key:alice".to_owned(),
            dimensions: vec![
                ("controller".to_owned(), b"did:key:alice".to_vec()),
                ("model".to_owned(), model_stream_id.to_vec()),
                ("unique".to_owned(), b"wwoyb".to_vec()),
            ],
        };
        let event_states = do_test(
            conclusion_events_to_record_batch(&vec![
                ConclusionEvent::Data(ConclusionData {
                    order: 0,
                    event_cid: model_stream_id.cid,
                    init: ConclusionInit {
                        stream_cid: model_stream_id.cid,
                        stream_type: StreamIdType::Model as u8,
                        controller: "did:key:bob".to_owned(),
                        dimensions: vec![
                            ("controller".to_owned(), b"did:key:bob".to_vec()),
                            ("model".to_owned(), METAMODEL_STREAM_ID.to_vec()),
                        ],
                    },
                    previous: vec![],
                    data: serde_json::to_string(&json!({
                        "metadata":{ },
                        "content": model_def,
                    }))
                    .unwrap()
                    .into(),
                }),
                ConclusionEvent::Data(ConclusionData {
                    order: 1,
                    event_cid: Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )
                    .unwrap(),
                    init: stream_init.clone(),
                    previous: vec![],
                    data: serde_json::to_string(&json!({
                    "metadata":{ },
                    "content":{
                        "creator":"alice",
                        "red":255,
                        "green":255,
                        "blue":255
                    }}))
                    .unwrap()
                    .into(),
                }),
                ConclusionEvent::Data(ConclusionData {
                    order: 2,
                    event_cid: Cid::from_str(
                        "baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em",
                    )
                    .unwrap(),
                    init: stream_init.clone(),
                    previous: vec![Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )
                    .unwrap()],
                    data: serde_json::to_string(&json!({
                    "metadata":{},
                    "content":[{
                        "op":"replace",
                        "path": "/creator",
                        "value":"sally"
                    }]}))
                    .unwrap()
                    .into(),
                }),
            ])
            .unwrap(),
        )
        .await
        .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | validation_errors                              | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------+--------+----------+
            | 1                 | baeabeibrydyefwuzizf6s32lbuu27rpjibrbtsziqukscbnuo3y52pgmha | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeibrydyefwuzizf6s32lbuu27rpjibrbtsziqukscbnuo3y52pgmha | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"immutableFields":["creator"],"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{}} | []                                             |        |          |
            | 2                 | baeabeihrpnqmppjqdohl2stvfevu7jn57zjqzqb4dflbl4ettogmt56xoy | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce01020100122031c0f042da99464be96f4b0d29afc5e9406219cb2885152105b476f1dd3ccc38, unique: 77776f7962} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{}}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | []                                             |        |          |
            | 3                 | baeabeihrpnqmppjqdohl2stvfevu7jn57zjqzqb4dflbl4ettogmt56xoy | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce01020100122031c0f042da99464be96f4b0d29afc5e9406219cb2885152105b476f1dd3ccc38, unique: 77776f7962} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 1            | {"metadata":{},"content":{"blue":255,"creator":"sally","green":255,"red":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | [Immutable field 'creator' cannot be modified] |        |          |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn update_single_relation_locked_fields() {
        let model_def = ModelDefinition::new_v2::<SmallModel>(
            "TestSmallModel".to_owned(),
            None,
            false,
            None,
            Some(vec!["creator".to_owned()]),
            ModelAccountRelationV2::Single,
        )
        .unwrap();

        // NOTE: These CIDs and StreamIDs are fake and do not represent the actual hash of the data.
        // This makes testing easier as changing the contents does not mean you need to update all of
        // the cids.
        let model_stream_id =
            StreamId::from_str("k2t6wz4yhfp1ndie7o9o4en4qc6jic9d4p9vggb0bpjt41sxl905ak2ff4h6pk")
                .unwrap();
        let instance_stream_id =
            StreamId::from_str("k2t6wz4yhfp1s5jhjhtelcqvwaomcgretwvdx7gz3cp66i9z8fsm8m743k1vue")
                .unwrap();
        let stream_init = ConclusionInit {
            stream_cid: instance_stream_id.cid,
            stream_type: StreamIdType::ModelInstanceDocument as u8,
            controller: "did:key:alice".to_owned(),
            dimensions: vec![
                ("controller".to_owned(), b"did:key:alice".to_vec()),
                ("model".to_owned(), model_stream_id.to_vec()),
            ],
        };
        let event_states = do_test(
            conclusion_events_to_record_batch(&vec![
                ConclusionEvent::Data(ConclusionData {
                    order: 0,
                    event_cid: model_stream_id.cid,
                    init: ConclusionInit {
                        stream_cid: model_stream_id.cid,
                        stream_type: StreamIdType::Model as u8,
                        controller: "did:key:bob".to_owned(),
                        dimensions: vec![
                            ("controller".to_owned(), b"did:key:bob".to_vec()),
                            ("model".to_owned(), METAMODEL_STREAM_ID.to_vec()),
                        ],
                    },
                    previous: vec![],
                    data: serde_json::to_string(&json!({
                        "metadata":{ },
                        "content": model_def,
                    }))
                    .unwrap()
                    .into(),
                }),
                ConclusionEvent::Data(ConclusionData {
                    order: 1,
                    event_cid: Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )
                    .unwrap(),
                    init: stream_init.clone(),
                    previous: vec![],
                    data: serde_json::to_string(&json!({
                        "metadata":{ },
                        "content": serde_json::Value::Null,
                    }))
                    .unwrap()
                    .into(),
                }),
                ConclusionEvent::Data(ConclusionData {
                    order: 2,
                    event_cid: Cid::from_str(
                        "baeabeigbuxpmhj2fkwe2oeqflwz6xair5i3sxnoyqzeh5rly25l3g5rlzy",
                    )
                    .unwrap(),
                    init: stream_init.clone(),
                    previous: vec![Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )
                    .unwrap()],
                    data: serde_json::to_string(&json!({
                    "metadata": {},
                    "content":[
                        {
                            "op":"add",
                            "path": "/creator",
                            "value":"sally"
                        },
                        {
                            "op":"add",
                            "path": "/red",
                            "value": 255,
                        },
                        {
                            "op":"add",
                            "path": "/green",
                            "value": 255,
                        },
                        {
                            "op":"add",
                            "path": "/blue",
                            "value": 255,
                        },
                    ]}))
                    .unwrap()
                    .into(),
                }),
                ConclusionEvent::Data(ConclusionData {
                    order: 3,
                    event_cid: Cid::from_str(
                        "baeabeigyxnmgyi6mekyqbx3rpp5fi7chvl5tp6vjxaumsqgaa5ggqbq4lm",
                    )
                    .unwrap(),
                    init: stream_init.clone(),
                    previous: vec![Cid::from_str(
                        "baeabeigbuxpmhj2fkwe2oeqflwz6xair5i3sxnoyqzeh5rly25l3g5rlzy",
                    )
                    .unwrap()],
                    data: serde_json::to_string(&json!({
                    "metadata":{"foo":2},
                    "content":[{
                        "op":"replace",
                        "path": "/creator",
                        "value":"sally"
                    }]}))
                    .unwrap()
                    .into(),
                }),
            ])
            .unwrap(),
        )
        .await
        .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+---------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                      | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | validation_errors                              | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+---------------+---------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------+--------+----------+
            | 1                 | baeabeibrydyefwuzizf6s32lbuu27rpjibrbtsziqukscbnuo3y52pgmha | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                               | baeabeibrydyefwuzizf6s32lbuu27rpjibrbtsziqukscbnuo3y52pgmha | 0          | 0            | {"content":{"accountRelation":{"type":"single"},"immutableFields":["creator"],"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{}} | []                                             |        |          |
            | 2                 | baeabeihrpnqmppjqdohl2stvfevu7jn57zjqzqb4dflbl4ettogmt56xoy | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce01020100122031c0f042da99464be96f4b0d29afc5e9406219cb2885152105b476f1dd3ccc38} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":null,"metadata":{}}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                                             |        |          |
            | 3                 | baeabeihrpnqmppjqdohl2stvfevu7jn57zjqzqb4dflbl4ettogmt56xoy | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce01020100122031c0f042da99464be96f4b0d29afc5e9406219cb2885152105b476f1dd3ccc38} | baeabeigbuxpmhj2fkwe2oeqflwz6xair5i3sxnoyqzeh5rly25l3g5rlzy | 0          | 1            | {"metadata":{},"content":{"blue":255,"creator":"sally","green":255,"red":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                                             |        |          |
            | 4                 | baeabeihrpnqmppjqdohl2stvfevu7jn57zjqzqb4dflbl4ettogmt56xoy | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce01020100122031c0f042da99464be96f4b0d29afc5e9406219cb2885152105b476f1dd3ccc38} | baeabeigyxnmgyi6mekyqbx3rpp5fi7chvl5tp6vjxaumsqgaa5ggqbq4lm | 0          | 2            | {"metadata":{"foo":2},"content":{"blue":255,"creator":"sally","green":255,"red":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | [Immutable field 'creator' cannot be modified] |        |          |
            +-------------------+-------------------------------------------------------------+-------------+---------------+---------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }

    #[test(tokio::test)]
    async fn update_set_relation_fields() {
        let model_def = ModelDefinition::new_v2::<SmallModel>(
            "TestSmallModel".to_owned(),
            None,
            false,
            None,
            None,
            ModelAccountRelationV2::Set {
                fields: vec!["creator".to_owned()],
            },
        )
        .unwrap();

        // NOTE: These CIDs and StreamIDs are fake and do not represent the actual hash of the data.
        // This makes testing easier as changing the contents does not mean you need to update all of
        // the cids.
        let model_stream_id =
            StreamId::from_str("k2t6wz4yhfp1ndie7o9o4en4qc6jic9d4p9vggb0bpjt41sxl905ak2ff4h6pk")
                .unwrap();
        let instance_stream_id =
            StreamId::from_str("k2t6wz4yhfp1s5jhjhtelcqvwaomcgretwvdx7gz3cp66i9z8fsm8m743k1vue")
                .unwrap();
        let stream_init = ConclusionInit {
            stream_cid: instance_stream_id.cid,
            stream_type: StreamIdType::ModelInstanceDocument as u8,
            controller: "did:key:alice".to_owned(),
            dimensions: vec![
                ("controller".to_owned(), b"did:key:alice".to_vec()),
                ("model".to_owned(), model_stream_id.to_vec()),
                ("unique".to_owned(), b"alice".to_vec()),
            ],
        };
        let event_states = do_test(
            conclusion_events_to_record_batch(&vec![
                ConclusionEvent::Data(ConclusionData {
                    order: 0,
                    event_cid: model_stream_id.cid,
                    init: ConclusionInit {
                        stream_cid: model_stream_id.cid,
                        stream_type: StreamIdType::Model as u8,
                        controller: "did:key:bob".to_owned(),
                        dimensions: vec![
                            ("controller".to_owned(), b"did:key:bob".to_vec()),
                            ("model".to_owned(), METAMODEL_STREAM_ID.to_vec()),
                        ],
                    },
                    previous: vec![],
                    data: serde_json::to_string(&json!({
                        "metadata":{ },
                        "content": model_def,
                    }))
                    .unwrap()
                    .into(),
                }),
                ConclusionEvent::Data(ConclusionData {
                    order: 1,
                    event_cid: Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )
                    .unwrap(),
                    init: stream_init.clone(),
                    previous: vec![],
                    data: serde_json::to_string(&json!({
                        "metadata":{ },
                        "content": serde_json::Value::Null,
                    }))
                    .unwrap()
                    .into(),
                }),
                ConclusionEvent::Data(ConclusionData {
                    order: 2,
                    event_cid: Cid::from_str(
                        "baeabeid7bnwog7iafyk7w543ufid7rjmvs72zvczbzpkltyy3bwaomxh7q",
                    )
                    .unwrap(),
                    init: stream_init.clone(),
                    previous: vec![Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )
                    .unwrap()],
                    data: serde_json::to_string(&json!({
                        "metadata":{ },
                        "content": serde_json::Value::Null,
                    }))
                    .unwrap()
                    .into(),
                }),
                ConclusionEvent::Data(ConclusionData {
                    order: 3,
                    event_cid: Cid::from_str(
                        "baeabeigbuxpmhj2fkwe2oeqflwz6xair5i3sxnoyqzeh5rly25l3g5rlzy",
                    )
                    .unwrap(),
                    init: stream_init.clone(),
                    previous: vec![Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )
                    .unwrap()],
                    data: serde_json::to_string(&json!({
                    "metadata": {},
                    "content":[
                        {
                            "op":"add",
                            "path": "/creator",
                            "value":"alice-not-unique-header"
                        },
                        {
                            "op":"add",
                            "path": "/red",
                            "value": 255,
                        },
                        {
                            "op":"add",
                            "path": "/green",
                            "value": 255,
                        },
                        {
                            "op":"add",
                            "path": "/blue",
                            "value": 255,
                        },
                    ]}))
                    .unwrap()
                    .into(),
                }),
                ConclusionEvent::Data(ConclusionData {
                    order: 4,
                    event_cid: Cid::from_str(
                        "baeabeibhai2zanz5v4sobyzddgvrddweqardx47frsqexv3u45wxpr22ni",
                    )
                    .unwrap(),
                    init: stream_init.clone(),
                    previous: vec![Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )
                    .unwrap()],
                    data: serde_json::to_string(&json!({
                    "metadata": {},
                    "content":[
                        {
                            "op":"add",
                            "path": "/creator",
                            "value":"alice"
                        },
                        {
                            "op":"add",
                            "path": "/red",
                            "value": 255,
                        },
                        {
                            "op":"add",
                            "path": "/green",
                            "value": 255,
                        },
                        {
                            "op":"add",
                            "path": "/blue",
                            "value": 255,
                        },
                    ]}))
                    .unwrap()
                    .into(),
                }),
                ConclusionEvent::Data(ConclusionData {
                    order: 5,
                    event_cid: Cid::from_str(
                        "baeabeigyxnmgyi6mekyqbx3rpp5fi7chvl5tp6vjxaumsqgaa5ggqbq4lm",
                    )
                    .unwrap(),
                    init: stream_init.clone(),
                    previous: vec![Cid::from_str(
                        "baeabeibhai2zanz5v4sobyzddgvrddweqardx47frsqexv3u45wxpr22ni",
                    )
                    .unwrap()],
                    data: serde_json::to_string(&json!({
                    "metadata":{},
                    "content":[{
                        "op":"replace",
                        "path": "/creator",
                        "value":"sally"
                    }]}))
                    .unwrap()
                    .into(),
                }),
            ])
            .unwrap(),
        )
        .await
        .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | validation_errors                                                                                           | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------+--------+----------+
            | 1                 | baeabeibrydyefwuzizf6s32lbuu27rpjibrbtsziqukscbnuo3y52pgmha | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeibrydyefwuzizf6s32lbuu27rpjibrbtsziqukscbnuo3y52pgmha | 0          | 0            | {"content":{"accountRelation":{"fields":["creator"],"type":"set"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{}} | []                                                                                                          |        |          |
            | 2                 | baeabeihrpnqmppjqdohl2stvfevu7jn57zjqzqb4dflbl4ettogmt56xoy | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce01020100122031c0f042da99464be96f4b0d29afc5e9406219cb2885152105b476f1dd3ccc38, unique: 616c696365} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":null,"metadata":{}}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | []                                                                                                          |        |          |
            | 3                 | baeabeihrpnqmppjqdohl2stvfevu7jn57zjqzqb4dflbl4ettogmt56xoy | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce01020100122031c0f042da99464be96f4b0d29afc5e9406219cb2885152105b476f1dd3ccc38, unique: 616c696365} | baeabeibhai2zanz5v4sobyzddgvrddweqardx47frsqexv3u45wxpr22ni | 0          | 1            | {"metadata":{},"content":{"blue":255,"creator":"alice","green":255,"red":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | []                                                                                                          |        |          |
            | 4                 | baeabeihrpnqmppjqdohl2stvfevu7jn57zjqzqb4dflbl4ettogmt56xoy | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce01020100122031c0f042da99464be96f4b0d29afc5e9406219cb2885152105b476f1dd3ccc38, unique: 616c696365} | baeabeid7bnwog7iafyk7w543ufid7rjmvs72zvczbzpkltyy3bwaomxh7q | 0          | 1            |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | [cannot validate null instance document]                                                                    |        |          |
            | 5                 | baeabeihrpnqmppjqdohl2stvfevu7jn57zjqzqb4dflbl4ettogmt56xoy | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce01020100122031c0f042da99464be96f4b0d29afc5e9406219cb2885152105b476f1dd3ccc38, unique: 616c696365} | baeabeigbuxpmhj2fkwe2oeqflwz6xair5i3sxnoyqzeh5rly25l3g5rlzy | 0          | 1            | {"metadata":{},"content":{"blue":255,"creator":"alice-not-unique-header","green":255,"red":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    | [Unique content fields value does not match metadata. Expected 'alice' but found 'alice-not-unique-header'] |        |          |
            | 6                 | baeabeihrpnqmppjqdohl2stvfevu7jn57zjqzqb4dflbl4ettogmt56xoy | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce01020100122031c0f042da99464be96f4b0d29afc5e9406219cb2885152105b476f1dd3ccc38, unique: 616c696365} | baeabeigyxnmgyi6mekyqbx3rpp5fi7chvl5tp6vjxaumsqgaa5ggqbq4lm | 0          | 2            | {"metadata":{},"content":{"blue":255,"creator":"sally","green":255,"red":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | [Set account relation field 'creator' cannot be modified]                                                   |        |          |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn model_definition_set_relation_no_fields() {
        let model_def = ModelDefinition::new_v2::<SmallModel>(
            "TestSmallModel".to_owned(),
            None,
            false,
            None,
            None,
            ModelAccountRelationV2::Set { fields: vec![] },
        )
        .unwrap();

        // NOTE: These CIDs and StreamIDs are fake and do not represent the actual hash of the data.
        // This makes testing easier as changing the contents does not mean you need to update all of
        // the cids.
        let model_stream_id =
            StreamId::from_str("k2t6wz4yhfp1ndie7o9o4en4qc6jic9d4p9vggb0bpjt41sxl905ak2ff4h6pk")
                .unwrap();
        let event_states = do_test(
            conclusion_events_to_record_batch(&vec![ConclusionEvent::Data(ConclusionData {
                order: 0,
                event_cid: model_stream_id.cid,
                init: ConclusionInit {
                    stream_cid: model_stream_id.cid,
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:bob".to_owned(),
                    dimensions: vec![
                        ("controller".to_owned(), b"did:key:bob".to_vec()),
                        ("model".to_owned(), METAMODEL_STREAM_ID.to_vec()),
                    ],
                },
                previous: vec![],
                data: serde_json::to_string(&json!({
                    "metadata":{ },
                    "content": model_def,
                }))
                .unwrap()
                .into(),
            })])
            .unwrap(),
        )
        .await
        .unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller  | dimensions                                                                        | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | validation_errors                                              | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+--------+----------+
            | 1                 | baeabeibrydyefwuzizf6s32lbuu27rpjibrbtsziqukscbnuo3y52pgmha | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631} | baeabeibrydyefwuzizf6s32lbuu27rpjibrbtsziqukscbnuo3y52pgmha | 0          | 0            | {"content":{"accountRelation":{"fields":[],"type":"set"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{}} | [Account relation of type Set must include at least one field] |        |          |
            +-------------------+-------------------------------------------------------------+-------------+-------------+-----------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------+--------+----------+"#]].assert_eq(&event_states.to_string());
    }

    #[tokio::test]
    async fn cache_flush_on_shutdown() {
        use object_store::memory::InMemory;
        let object_store = Arc::new(InMemory::new());

        // Create test events (just 2 events - will stay in cache with threshold of 5)
        let events = &model_and_mids_events()[0..2];
        let conclusion_events = conclusion_events_to_record_batch(events).unwrap();

        let phase1_result = {
            let mut mock_concluder = MockConcluder::new();
            mock_concluder
                .expect_handle_subscribe_since()
                .times(1)
                .return_once(|_msg| {
                    Ok(Box::pin(RecordBatchStreamAdapter::new(
                        schemas::conclusion_events(),
                        stream::empty(),
                    )))
                });

            let ctx = init_with_object_store(
                mock_concluder,
                object_store.clone(),
                Some(5), // Cache threshold of 5 - our 2 events will stay in memory
            )
            .await
            .unwrap();

            let result = do_pass(
                ctx.actor_handle.clone(),
                None,
                Some(conclusion_events.clone()),
            )
            .await
            .unwrap();

            ctx.shutdown().await.unwrap();
            result
        };

        // Phase 2: Restart aggregator and query for existing data
        let phase2_result = {
            let mut mock_concluder = MockConcluder::new();
            mock_concluder
                .expect_handle_subscribe_since()
                .times(1)
                .return_once(|_msg| {
                    Ok(Box::pin(RecordBatchStreamAdapter::new(
                        schemas::conclusion_events(),
                        stream::empty(),
                    )))
                });

            let ctx = init_with_object_store(mock_concluder, object_store.clone(), Some(5))
                .await
                .unwrap();

            // Query for existing data using direct subscription - finds nothing due to cache loss bug
            let mut subscription = ctx
                .actor_handle
                .send(SubscribeSinceMsg {
                    projection: None,
                    filters: None,
                    limit: None,
                })
                .await
                .unwrap()
                .unwrap();

            // DO NOT send new events - we want to see what persisted from Phase 1
            // Wait briefly for any persisted data, then timeout
            let result = tokio::time::timeout(
                std::time::Duration::from_millis(100),
                subscription.try_next(),
            )
            .await;

            let event_states = match result {
                Ok(Ok(Some(batch))) => pretty_event_states(vec![batch]).await.unwrap().to_string(),
                _ => "No persisted data found".to_string(),
            };

            ctx.shutdown().await.unwrap();
            event_states
        };

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |        |          |
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#
        ]].assert_eq(&phase1_result.to_string());

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before | chain_id |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |        |          |
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |        |          |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+--------+----------+"#
        ]].assert_eq(&phase2_result.to_string());
    }

    #[tokio::test]
    async fn cache_flush_on_threshold() {
        use object_store::memory::InMemory;
        let object_store = Arc::new(InMemory::new());

        // Create test events - we'll use 4 events with threshold of 3
        let events = &model_and_mids_events()[0..4]; // Get first 4 events
        let conclusion_events = conclusion_events_to_record_batch(events).unwrap();

        // Phase 1: Process events that will exceed cache threshold
        let phase1_result = {
            let mut mock_concluder = MockConcluder::new();
            mock_concluder
                .expect_handle_subscribe_since()
                .times(1)
                .return_once(|_msg| {
                    Ok(Box::pin(RecordBatchStreamAdapter::new(
                        schemas::conclusion_events(),
                        stream::empty(),
                    )))
                });

            let ctx = init_with_object_store(
                mock_concluder,
                object_store.clone(),
                Some(3), // Cache threshold of 3 - our 4 events should trigger flush
            )
            .await
            .unwrap();

            let result = do_pass(
                ctx.actor_handle.clone(),
                None,
                Some(conclusion_events.clone()),
            )
            .await
            .unwrap();

            ctx.shutdown().await.unwrap();
            result
        };

        // Phase 2: Restart aggregator and query for persisted data
        let phase2_result = {
            let mut mock_concluder = MockConcluder::new();
            mock_concluder
                .expect_handle_subscribe_since()
                .times(1)
                .return_once(|_msg| {
                    Ok(Box::pin(RecordBatchStreamAdapter::new(
                        schemas::conclusion_events(),
                        stream::empty(),
                    )))
                });

            let ctx = init_with_object_store(mock_concluder, object_store.clone(), Some(3))
                .await
                .unwrap();

            // Query for existing data using direct subscription - should find persisted data
            let mut subscription = ctx
                .actor_handle
                .send(SubscribeSinceMsg {
                    projection: None,
                    filters: None,
                    limit: None,
                })
                .await
                .unwrap()
                .unwrap();

            // DO NOT send new events - we want to see what persisted from Phase 1
            // The data should be available immediately since it was flushed to persistent storage
            let batch = subscription.try_next().await.unwrap().unwrap();
            let event_states = pretty_event_states(vec![batch]).await.unwrap();

            ctx.shutdown().await.unwrap();
            event_states
        };

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before        | chain_id   |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |               |            |
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |               |            |
            | 3                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | 1            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                | 1744383131980 | test:chain |
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 2            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    | []                |               |            |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+"#
        ]].assert_eq(&phase1_result.to_string());

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before        | chain_id   |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |               |            |
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |               |            |
            | 3                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | 1            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                | 1744383131980 | test:chain |
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 2            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    | []                |               |            |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+"#
        ]].assert_eq(&phase2_result.to_string());
    }

    // Tests for idempotent patch application
    #[tokio::test]
    async fn patch_idempotency() {
        // Test idempotency by processing same events twice - should never have duplicate events in one pass
        let events = &[&model_and_mids_events()[0..4]].concat();
        let conclusion_events = conclusion_events_to_record_batch(events).unwrap();

        let ctx = init().await.unwrap();

        let result = do_pass(
            ctx.actor_handle.clone(),
            None,
            Some(conclusion_events.clone()),
        )
        .await
        .unwrap();

        ctx.shutdown().await.unwrap();

        let ctx = init().await.unwrap();

        let result2 = do_pass(
            ctx.actor_handle.clone(),
            None,
            Some(conclusion_events.clone()),
        )
        .await
        .unwrap();

        ctx.shutdown().await.unwrap();

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before        | chain_id   |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |               |            |
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |               |            |
            | 3                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | 1            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                | 1744383131980 | test:chain |
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 2            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    | []                |               |            |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+"#
        ]].assert_eq(&result.to_string());

        expect![[r#"
        +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
        | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before        | chain_id   |
        +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
        | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |               |            |
        | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |               |            |
        | 3                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | 1            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                | 1744383131980 | test:chain |
        | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 2            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    | []                |               |            |
        +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+"#
    ]].assert_eq(&result2.to_string());
    }

    #[tokio::test]
    async fn patch_idempotency_at_batch_boundaries() {
        let events = &model_and_mids_events()[0..4];
        let ctx = init().await.unwrap();

        // First processing: process events normally
        let first_result = {
            let result = do_pass(
                ctx.actor_handle.clone(),
                None,
                Some(conclusion_events_to_record_batch(events).unwrap()),
            )
            .await
            .unwrap();
            // ctx.shutdown().await.unwrap();
            result
        };

        // Second processing: process same events again in a new batch
        let second_result = {
            // let ctx = init().await.unwrap();
            let result = do_pass(
                ctx.actor_handle.clone(),
                None,
                Some(conclusion_events_to_record_batch(events).unwrap()),
            )
            .await
            .unwrap();
            ctx.shutdown().await.unwrap();
            result
        };

        println!("first_result: {}", first_result);
        println!("second_result: {}", second_result);

        expect![[r#"
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | event_state_order | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                          | event_cid                                                   | event_type | event_height | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors | before        | chain_id   |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+
            | 1                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62, model: ce01040171710b0009686d6f64656c2d7631}                                                                   | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | 0            | {"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}},"metadata":{"foo":1,"shouldIndex":true}} | []                |               |            |
            | 2                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | 0            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                |               |            |
            | 3                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | 1            | {"content":{"blue":255,"creator":"alice","green":255,"red":255},"metadata":{"foo":1,"shouldIndex":true}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  | []                | 1744383131980 | test:chain |
            | 4                 | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, unique: 77676b3533} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | 2            | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    | []                |               |            |
            +-------------------+-------------------------------------------------------------+-------------+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+---------------+------------+"#
        ]].assert_eq(&first_result.to_string());

        // Results should be identical - this is the core idempotency test
        assert_eq!(second_result.to_string(), first_result.to_string());
    }

    fn random_cid() -> Cid {
        use multihash_codetable::{Code, MultihashDigest};
        use rand::{thread_rng, Rng};

        let mut data = [0u8; 8];
        thread_rng().fill(&mut data);
        let hash = MultihashDigest::digest(&Code::Sha2_256, &data);
        Cid::new_v1(0x00, hash)
    }

    fn test_model() -> (StreamId, ConclusionEvent) {
        let model_def = ModelDefinition::new_v2::<SmallModel>(
            "TestSmallModel".to_owned(),
            None,
            false,
            None,
            None,
            ModelAccountRelationV2::List,
        )
        .unwrap();

        let model_stream_id =
            StreamId::from_str("k2t6wz4yhfp1pc9l42mm6vh20xmhm9ac7cznnpu4xcxe4jds13l9sjknm1accd")
                .unwrap();
        (
            model_stream_id.clone(),
            ConclusionEvent::Data(ConclusionData {
                order: 0,
                event_cid: model_stream_id.cid,
                init: ConclusionInit {
                    stream_cid: model_stream_id.cid,
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:bob".to_owned(),
                    dimensions: vec![
                        ("controller".to_owned(), b"did:key:bob".to_vec()),
                        ("model".to_owned(), METAMODEL_STREAM_ID.to_vec()),
                    ],
                },
                previous: vec![],
                data: serde_json::to_string(&json!({
                    "metadata":{
                        "foo":1,
                        "shouldIndex":true
                    },
                    "content": model_def,
                }))
                .unwrap()
                .into(),
            }),
        )
    }

    /// WARNING:  
    ///     - the order used here MUST BE GLOBAL for your events, so if you call this multiple times
    /// you must correct the order manually.
    ///     - the event CID and unique values are random, so they are not stable across test runs
    fn n_mid_events(to_add: u64, model_stream_id: &StreamId) -> Vec<ConclusionEvent> {
        let unique = random_cid().to_bytes();
        let instance_stream_id = StreamId::document(random_cid());
        let stream_init = ConclusionInit {
            stream_cid: instance_stream_id.cid,
            stream_type: StreamIdType::ModelInstanceDocument as u8,
            controller: "did:key:alice".to_owned(),
            dimensions: vec![
                ("controller".to_owned(), b"did:key:alice".to_vec()),
                ("model".to_owned(), model_stream_id.to_vec()),
                ("unique".to_owned(), unique),
            ],
        };

        let mut cids = Vec::with_capacity(to_add as usize);
        let mut events = Vec::with_capacity(to_add as usize);
        for i in 0..to_add {
            let event_cid = random_cid();
            cids.push(event_cid);
            let event = match i {
                0 => ConclusionEvent::Data(ConclusionData {
                    order: 1,
                    event_cid,
                    init: stream_init.clone(),
                    previous: vec![],
                    data: serde_json::to_string(&json!({
                    "metadata":{
                        "foo":1,
                        "shouldIndex":true
                    },
                    "content":{
                        "creator":"alice",
                        "red":255,
                        "green":255,
                        "blue":255
                    }}))
                    .unwrap()
                    .into(),
                }),
                _ => ConclusionEvent::Data(ConclusionData {
                    order: i + 1,
                    event_cid,
                    init: stream_init.clone(),
                    previous: vec![cids[i as usize - 1]],
                    data: serde_json::to_string(&json!({
                    "metadata":{"foo":2},
                    "content":[{
                        "op":"replace",
                        "path": "/red",
                        "value":0
                    }]}))
                    .unwrap()
                    .into(),
                }),
            };
            events.push(event);
        }
        events
    }

    #[test_log::test(tokio::test)]
    async fn interspersed_events() {
        let (model_stream_id, model) = test_model();
        let mid1_events = n_mid_events(10, &model_stream_id);
        let mid2_events = n_mid_events(10, &model_stream_id);
        let mid3_events = n_mid_events(10, &model_stream_id);
        let mut events = mid1_events[0..5]
            .iter()
            .cloned()
            .chain(mid2_events[0..5].iter().cloned())
            .chain(mid3_events[0..5].iter().cloned())
            .chain(mid2_events[5..].iter().cloned())
            .chain(mid1_events[5..].iter().cloned())
            .chain([model])
            .chain(mid3_events[5..].iter().cloned())
            .collect::<Vec<_>>();
        // events must come after their previous and the order number has to incrememt globally.
        // we ensured condition 1, now we rewrite the order
        events
            .iter_mut()
            .enumerate()
            .for_each(|(i, event)| match event {
                ConclusionEvent::Data(data) => data.order = i as u64,
                ConclusionEvent::Time(time) => time.order = i as u64,
            });

        let events = &events;

        // First processing: process events normally
        let first_result = {
            let ctx = init_with_cache(Some(7)).await.unwrap();
            let result = do_pass(
                ctx.actor_handle.clone(),
                None,
                Some(conclusion_events_to_record_batch(events).unwrap()),
            )
            .await
            .unwrap();
            ctx.shutdown().await.unwrap();
            result
        };
        let partial_result = {
            let ctx = init_with_cache(Some(7)).await.unwrap();
            let result = do_pass(
                ctx.actor_handle.clone(),
                Some(20),
                Some(conclusion_events_to_record_batch(events).unwrap()),
            )
            .await
            .unwrap();
            ctx.shutdown().await.unwrap();
            result
        };
        let third_result = {
            let ctx = init_with_cache(Some(7)).await.unwrap();
            let result = do_pass(
                ctx.actor_handle.clone(),
                None,
                Some(conclusion_events_to_record_batch(events).unwrap()),
            )
            .await
            .unwrap();
            ctx.shutdown().await.unwrap();
            result
        };
        let res = first_result.to_string();
        let res2 = partial_result.to_string();
        let res3 = third_result.to_string();
        // Results should be identical - this is the core idempotency test
        tracing::info!("first_result: {}", res);
        tracing::info!("partial_result: {}", res2);
        tracing::info!("third_result: {}", res3);
        assert_eq!(res, res3);
        assert!(!res.contains("cannot validate"));
        assert!(!res2.contains("cannot validate"));
    }

    async fn batched_test(
        batch1: &[ConclusionEvent],
        batch2: &[ConclusionEvent],
    ) -> Vec<RecordBatch> {
        let ctx = init_with_cache(Some(7)).await.unwrap();

        let mut subscription = ctx
            .actor_handle
            .send(SubscribeSinceMsg {
                projection: None,
                filters: None,
                limit: None,
            })
            .await
            .unwrap()
            .unwrap();

        ctx.actor_handle
            .send(NewConclusionEventsMsg {
                events: conclusion_events_to_record_batch(batch1).unwrap(),
            })
            .await
            .unwrap()
            .unwrap();

        ctx.actor_handle
            .send(NewConclusionEventsMsg {
                events: conclusion_events_to_record_batch(batch2).unwrap(),
            })
            .await
            .unwrap()
            .unwrap();

        let results = subscription.try_next().await.unwrap().unwrap();

        ctx.shutdown().await.unwrap();
        vec![results]
    }

    #[test_log::test(tokio::test)]
    async fn pending_model_batching() {
        let (model_stream_id, model) = test_model();
        let mid1_events = n_mid_events(10, &model_stream_id);
        let mid2_events = n_mid_events(10, &model_stream_id);
        let mut events = mid1_events[0..1]
            .iter()
            .cloned()
            .chain(mid2_events.iter().cloned())
            .chain([model])
            .chain(mid1_events[1..].iter().cloned())
            .collect::<Vec<_>>();
        // events must come after their previous and the order number has to incrememt globally.
        // we ensured condition 1, now we rewrite the order
        events
            .iter_mut()
            .enumerate()
            .for_each(|(i, event)| match event {
                ConclusionEvent::Data(data) => data.order = i as u64,
                ConclusionEvent::Time(time) => time.order = i as u64,
            });

        let batch1 = &events[0..10].to_vec();
        let batch2 = &events[10..].to_vec();

        // First processing: process events normally
        let results = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            batched_test(batch1, batch2),
        )
        .await
        .unwrap();
        let res = pretty_event_states(results).await.unwrap();

        assert!(!res.to_string().contains("null instance"), "{res}");
    }
}
