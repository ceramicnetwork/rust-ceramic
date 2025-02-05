//! Aggregation functions for Ceramic Model Instance Document streams.
//!
//! Applies each new event to the previous state of the stream producing the stream state at each
//! event in the stream.
mod ceramic_patch;
mod metrics;
#[cfg(any(test, feature = "mock"))]
pub mod mock;
mod model_instance_validate;
mod model_validate;

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use anyhow::Context;
use arrow::{
    array::{Array as _, ArrayAccessor as _, BinaryArray, RecordBatch},
    compute::concat_batches,
    datatypes::Int32Type,
    util::pretty::pretty_format_batches,
};
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use ceramic_actor::{actor_envelope, Actor, Handler, Message};
use ceramic_core::{StreamId, StreamIdType};
use ceramic_patch::CeramicPatch;
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
    logical_expr::{
        col, dml::InsertOp, expr::WindowFunction, lit, Expr, ExprFunctionExt as _, ScalarUDF,
        WindowFunctionDefinition, UNNAMED_TABLE,
    },
    physical_plan::stream::RecordBatchStreamAdapter,
    prelude::{cast, DataFrame, SessionContext},
    sql::TableReference,
};
use futures::TryStreamExt as _;
use shutdown::{Shutdown, ShutdownSignal};
use tokio::{select, sync::broadcast, task::JoinHandle};
use tracing::{debug, error, instrument};

use crate::{
    cache_table::CacheTable,
    cid_part::cid_part,
    concluder::ConcluderHandle,
    dimension_extract::dimension_extract,
    metrics::Metrics,
    schemas,
    since::{rows_since, FeedTable, FeedTableSource},
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
const EVENT_STATES_TABLE_OBJECT_STORE_PATH: &str = "ceramic/rev1/event_states/";

// Maximum number of rows to cache in memory before writing to object store.
const DEFAULT_MAX_CACHED_ROWS: usize = 10_000;

/// Aggregator is responsible for computing the state of a stream for each event within the stream.
/// The aggregator only operates on model and model instance stream types.
#[derive(Actor)]
pub struct Aggregator {
    ctx: SessionContextRef,
    broadcast_tx: broadcast::Sender<RecordBatch>,
    max_cached_rows: usize,
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
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(1_000);
        let aggregator = Aggregator {
            ctx: ctx.session(),
            broadcast_tx,
            max_cached_rows: max_cached_rows.unwrap_or(DEFAULT_MAX_CACHED_ROWS),
        };
        Self::spawn_with(size, ctx, aggregator, concluder, metrics, shutdown).await
    }
    /// Spawn the actor given an implementation of the aggregator.
    pub async fn spawn_with(
        size: usize,
        ctx: &PipelineContext,
        aggregator: impl AggregatorActor,
        concluder: ConcluderHandle,
        metrics: Metrics,
        shutdown: Shutdown,
    ) -> Result<(AggregatorHandle, Vec<JoinHandle<()>>)> {
        let (handle, task_handle) = Self::spawn(size, aggregator, metrics, shutdown.wait_fut());

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

        ctx.session()
            .register_table(
                EVENT_STATES_FEED_TABLE,
                Arc::new(FeedTable::new(handle.clone())),
            )
            .expect("should be able to register table");

        let h = handle.clone();
        let c = ctx.session();
        let sub_handle = tokio::spawn(async move {
            if let Err(err) = concluder_subscription(c, shutdown.wait_fut(), concluder, h).await {
                error!(%err, "aggregator actor poll_concluder task failed");
            }
        });

        Ok((handle, vec![task_handle, sub_handle]))
    }

    // Process a batch of conclusion events, producing a new document state for each input event.
    // The conclusion event must be ordered by "index".
    // The output event states are ordered by "index".
    //
    // The process has two main phases:
    // 1. Aggregate events to produce their new states
    // 2. Validate new states.
    //
    // Computing states and validation are stream type specific operations. So these phases are
    // repeated for each stream type.
    #[instrument(skip(self, conclusion_events))]
    async fn process_conclusion_events_batch(
        &self,
        conclusion_events: RecordBatch,
    ) -> Result<RecordBatch> {
        // Aggregate all events into their new states
        let events = self
            .aggregate_event_states(self.ctx.read_batch(conclusion_events)?)
            .await
            .context("aggregating events")?;
        tracing::debug!(
            events = pretty_format_batches(&events)?.to_string(),
            "all aggregated events"
        );
        let models = self
            .ctx
            // NOTE: Cloning a record batch is cheap as it is immutable
            .read_batches(events.clone())?
            .filter(col("stream_type").eq(lit(StreamIdType::Model as u8)))?;
        // Completely process models first for the case when mids may reference a newly
        // created/updated model.
        let models = self
            .validate_models(models)
            .context("validating models")?
            .collect()
            .await?;
        self.store_event_states(models.clone())
            .await
            .context("storing models")?;

        let mids = self
            .ctx
            .read_batches(events)?
            .filter(col("stream_type").eq(lit(StreamIdType::ModelInstanceDocument as u8)))?;
        let mids = self
            .validate_model_instances(mids)
            .await
            .context("validating mids")?
            .collect()
            .await?;
        self.store_event_states(mids.clone())
            .await
            .context("storing mids")?;
        // TODO: Can we make this do less copying of the data?
        let num_models = batches_num_rows(&models);
        let num_mids = batches_num_rows(&mids);
        if num_models > 0 && num_mids > 0 {
            let ordered_events = self
                .ctx
                .read_batches(models)?
                .union(self.ctx.read_batches(mids)?)?
                .sort(vec![col("index").sort(true, true)])?
                .collect()
                .await?;
            Ok(concat_batches(&schemas::event_states(), &ordered_events)?)
        } else if num_models > 0 {
            let ordered_events = self
                .ctx
                .read_batches(models)?
                .sort(vec![col("index").sort(true, true)])?
                .collect()
                .await?;
            Ok(concat_batches(&schemas::event_states(), &ordered_events)?)
        } else {
            let ordered_events = self
                .ctx
                .read_batches(mids)?
                .sort(vec![col("index").sort(true, true)])?
                .collect()
                .await?;
            Ok(concat_batches(&schemas::event_states(), &ordered_events)?)
        }
    }

    #[instrument(skip_all)]
    async fn aggregate_event_states(
        &self,
        conclusion_events: DataFrame,
    ) -> Result<Vec<RecordBatch>> {
        let event_states = self
            .ctx
            .table(EVENT_STATES_TABLE)
            .await?
            .select(vec![
                // Alias column so it does not conflict with the column from conclusion_events
                // in the join.
                col("event_cid_partition").alias("ecp"),
                col("stream_cid"),
                col("event_cid"),
                col("data"),
            ])
            .context("reading event_states")?;

        let new_event_states = conclusion_events
            // MID only ever use the first previous, so we can optimize the join by selecting the
            // first element of the previous array.
            .select(vec![
                col("index"),
                col("event_type"),
                col("stream_cid"),
                col("stream_type"),
                col("controller"),
                col("dimensions"),
                col("event_cid"),
                col("data"),
                array_element(col("previous"), lit(1)).alias("previous"),
                cid_part(array_element(col("previous"), lit(1)))
                    .alias("previous_event_cid_partition"),
                cid_part(col("event_cid")).alias("event_cid_partition"),
            ])?
            .join_on(
                event_states,
                JoinType::Left,
                [
                    col("previous_event_cid_partition").eq(col("ecp")),
                    col("previous").eq(table_col(EVENT_STATES_TABLE, "event_cid")),
                ],
            )
            .context("setup join")?
            .select(vec![
                anon_col("index").alias("index"),
                anon_col("event_type").alias("event_type"),
                anon_col("stream_cid").alias("stream_cid"),
                anon_col("stream_type").alias("stream_type"),
                anon_col("controller").alias("controller"),
                anon_col("dimensions").alias("dimensions"),
                anon_col("event_cid").alias("event_cid"),
                col("previous"),
                table_col(EVENT_STATES_TABLE, "data").alias("previous_data"),
                anon_col("data").alias("data"),
                col("event_cid_partition"),
            ])?
            .window(vec![Expr::WindowFunction(WindowFunction::new(
                WindowFunctionDefinition::WindowUDF(Arc::new(CeramicPatch::new_udwf())),
                vec![
                    col("event_cid"),
                    col("previous"),
                    col("previous_data"),
                    col("data"),
                ],
            ))
            .partition_by(vec![col("stream_cid")])
            .order_by(vec![col("index").sort(true, true)])
            .build()?
            .alias("new_data")])?
            // Rename columns to match event_states table schema
            .select_columns(&[
                "index",
                "stream_cid",
                "stream_type",
                "controller",
                "dimensions",
                "event_cid",
                "event_type",
                "previous_data",
                "new_data",
                "event_cid_partition",
            ])?
            .with_column_renamed("new_data", "data")?;
        Ok(new_event_states.collect().await?)
    }
    #[instrument(skip_all)]
    fn validate_models(&self, models: DataFrame) -> Result<DataFrame> {
        Ok(models.select(vec![
            col("index"),
            col("stream_cid"),
            col("stream_type"),
            col("controller"),
            col("dimensions"),
            col("event_cid"),
            col("event_type"),
            col("data"),
            model_validate::model_validate(col("data"), col("previous_data"))
                .alias("validation_errors"),
            col("event_cid_partition"),
        ])?)
    }
    #[instrument(skip_all)]
    async fn validate_model_instances(&self, model_instances: DataFrame) -> Result<DataFrame> {
        let states = self.ctx.table(EVENT_STATES_TABLE).await?.select_columns(&[
            "event_cid_partition", //alias to avoid join name collision
            "event_cid",
            "data",
        ])?;
        Ok(model_instances
            .select(vec![
                col("index"),
                col("stream_cid"),
                col("stream_type"),
                col("controller"),
                col("dimensions"),
                col("event_cid"),
                col("event_type"),
                col("previous_data"),
                col("data"),
                dimension_extract(col("dimensions"), lit("model_version")).alias("model_version"),
                col("event_cid_partition"),
            ])?
            .join_on(
                states,
                JoinType::Left,
                [
                    cid_part(col("model_version"))
                        .eq(table_col(EVENT_STATES_TABLE, "event_cid_partition")),
                    col("model_version").eq(table_col(EVENT_STATES_TABLE, "event_cid")),
                ],
            )?
            .select(vec![
                anon_col("index"),
                anon_col("stream_cid"),
                anon_col("stream_type"),
                anon_col("controller"),
                anon_col("dimensions"),
                anon_col("event_cid"),
                anon_col("event_type"),
                anon_col("data"),
                model_instance_validate::model_instance_validate(
                    anon_col("data"),
                    anon_col("previous_data"),
                    col("model_version"),
                    table_col(EVENT_STATES_TABLE, "data"),
                )
                .alias("validation_errors"),
                anon_col("event_cid_partition"),
            ])?)
    }
    #[instrument(skip_all)]
    async fn store_event_states(&self, event_states: Vec<RecordBatch>) -> Result<()> {
        if event_states
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>()
            == 0
        {
            return Ok(());
        }
        // Write states to the in memory event_states table
        self.ctx
            .read_batches(event_states)?
            .write_table(
                EVENT_STATES_MEM_TABLE,
                DataFrameWriteOptions::new()
                    .with_partition_by(vec!["event_cid_partition".to_owned()]),
            )
            .await
            .context("writing to mem table data")?;

        let count = self
            .ctx
            .table(EVENT_STATES_MEM_TABLE)
            .await?
            .count()
            .await?;
        // If we have enough data cached in memory write it out to persistent store
        if count >= self.max_cached_rows {
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
                .read_batch(RecordBatch::new_empty(schemas::event_states_partitioned()))?
                .write_table(
                    EVENT_STATES_MEM_TABLE,
                    DataFrameWriteOptions::new().with_insert_operation(InsertOp::Overwrite),
                )
                .await
                .context("clearing mem table")
                .unwrap();
        }
        Ok(())
    }
}

fn batches_num_rows<'a>(batches: impl IntoIterator<Item = &'a RecordBatch>) -> usize {
    batches.into_iter().map(|batch| batch.num_rows()).sum()
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
    ctx: SessionContextRef,
    mut shutdown: ShutdownSignal,
    concluder: ConcluderHandle,
    aggregator: AggregatorHandle,
) -> Result<()> {
    // Query for max index in persistent event_states, this is where we should start
    // the subscription for new conlcuder events.
    let batches = ctx
        .table(EVENT_STATES_PERSISTENT_TABLE)
        .await?
        .aggregate(
            vec![],
            vec![datafusion::functions_aggregate::min_max::max(col("index")).alias("max_index")],
        )?
        .collect()
        .await?;
    let offset = batches.first().and_then(|batch| {
        batch.column_by_name("max_index").and_then(|index_col| {
            as_uint64_array(&index_col)
                .ok()
                .and_then(|index_col| index_col.is_valid(0).then(|| index_col.value(0)))
        })
    });

    debug!(?offset, "starting concluder subscription");
    let mut rx = concluder
        .send(SubscribeSinceMsg {
            projection: None,
            offset,
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
    SubscribeSince => SubscribeSinceMsg,
    NewConclusionEvents => NewConclusionEventsMsg,
    StreamState => StreamStateMsg,
}

#[async_trait]
impl Handler<SubscribeSinceMsg> for Aggregator {
    async fn handle(
        &mut self,
        message: SubscribeSinceMsg,
    ) -> <SubscribeSinceMsg as Message>::Result {
        let subscription = self.broadcast_tx.subscribe();
        let ctx = self.ctx.clone();
        rows_since(
            schemas::conclusion_events(),
            message.projection,
            message.offset,
            message.limit,
            Box::pin(RecordBatchStreamAdapter::new(
                schemas::conclusion_events(),
                tokio_stream::wrappers::BroadcastStream::new(subscription)
                    .map_err(|err| exec_datafusion_err!("{err}")),
            )),
            // Future Optimization can be to send the projection and limit into the events_since call.
            events_since(&ctx, message.offset).await?,
        )
    }
}

async fn events_since(
    ctx: &SessionContext,
    offset: Option<u64>,
) -> Result<SendableRecordBatchStream> {
    let mut event_states = ctx.table(EVENT_STATES_TABLE).await?.select(vec![
        col("index"),
        col("stream_cid"),
        col("stream_type"),
        col("controller"),
        col("dimensions"),
        col("event_cid"),
        col("event_type"),
        col("data"),
        col("validation_errors"),
    ])?;
    if let Some(offset) = offset {
        event_states = event_states.filter(col("index").gt(lit(offset)))?;
    }
    Ok(event_states
        .sort(vec![col("index").sort(true, true)])?
        .execute_stream()
        .await?)
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
                col("stream_cid"),
                col("event_cid"),
                col("dimensions"),
                col("controller"),
                col("data"),
                col("index"),
            ])
            .context("invalid select")?
            .filter(col("stream_cid").eq(lit(id.cid.to_bytes())))
            .context("invalid filter")?
            .aggregate(
                vec![col("stream_cid"), col("controller")],
                vec![
                    last_value(vec![col("data")])
                        .order_by(vec![col("index").sort(true, true)])
                        .build()
                        .context("invalid last_value data query")?
                        .alias("data"),
                    last_value(vec![col("event_cid")])
                        .order_by(vec![col("index").sort(true, true)])
                        .build()
                        .context("invalid last_value event_cid query")?
                        .alias("event_cid"),
                    last_value(vec![col("dimensions")])
                        .order_by(vec![col("index").sort(true, true)])
                        .build()
                        .context("invalid last_value dimensions query")?
                        .alias("dimensions"),
                ],
            )
            .context("invalid window")?
            .collect()
            .await
            .context("invalid query")?;

        let batch = concat_batches(&state_batch[0].schema(), state_batch.iter())
            .context("concat batches")?;

        if batch.num_rows() == 0 {
            // No state for the stream id found
            return Ok(None);
        }

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
        offset: Option<u64>,
        limit: Option<usize>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        Ok(self
            .send(SubscribeSinceMsg {
                projection,
                offset,
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

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use super::*;

    use ::object_store::ObjectStore;
    use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
    use arrow_schema::DataType;
    use ceramic_core::StreamIdType;
    use cid::Cid;
    use datafusion::{
        logical_expr::{cast, expr::ScalarFunction, ScalarUDF},
        prelude::SessionContext,
    };
    use expect_test::expect;
    use futures::stream;
    use mockall::predicate;
    use object_store::memory::InMemory;
    use prometheus_client::registry::Registry;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use test_log::test;

    use crate::{
        ceramic_stream::model::{ModelAccountRelationV2, ModelDefinition},
        cid_string::{cid_string, CidString},
        concluder::mock::MockConcluder,
        conclusion_events_to_record_batch, pipeline_ctx,
        tests::TestContext,
        ConclusionData, ConclusionEvent, ConclusionInit, ConclusionTime,
    };

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
        init_with_concluder(mock_concluder).await
    }

    async fn init_with_concluder(
        mock_concluder: MockConcluder,
    ) -> anyhow::Result<TestContext<AggregatorHandle>> {
        let object_store = Arc::new(InMemory::new());
        init_with_object_store(mock_concluder, object_store, None).await
    }
    async fn init_with_object_store(
        mock_concluder: MockConcluder,
        object_store: Arc<dyn ObjectStore>,
        max_cached_rows: Option<usize>,
    ) -> anyhow::Result<TestContext<AggregatorHandle>> {
        let metrics = Metrics::register(&mut Registry::default());
        let shutdown = Shutdown::new();
        let pipeline_ctx = pipeline_ctx(object_store.clone()).await?;
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

    async fn do_pass(
        aggregator: AggregatorHandle,
        offset: Option<u64>,
        conclusion_events: Option<RecordBatch>,
    ) -> anyhow::Result<impl std::fmt::Display> {
        let mut subscription = aggregator
            .send(SubscribeSinceMsg {
                projection: None,
                offset,
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
                col("index"),
                cid_string(col("stream_cid")).alias("stream_cid"),
                col("stream_type"),
                col("controller"),
                col("dimensions"),
                cid_string(col("event_cid")).alias("event_cid"),
                col("event_type"),
                cast(col("data"), DataType::Utf8).alias("data"),
                col("validation_errors"),
            ])
            .context("select")?
            .collect()
            .await
            .context("collect")?;
        Ok(pretty_format_batches(&event_states).context("format")?)
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
        let model_def_json = serde_json::to_string(&model_def).unwrap();

        // NOTE: These CIDs and StreamIDs are fake and do not represent the actual hash of the data.
        // This makes testing easier as changing the contents does not mean you need to update all of
        // the cids.
        let model_stream_id =
            StreamId::from_str("k2t6wz4yhfp1pc9l42mm6vh20xmhm9ac7cznnpu4xcxe4jds13l9sjknm1accd")
                .unwrap();
        let instance_stream_id =
            StreamId::from_str("k2t6wzhjp5kk11gmt3clbfsplu6dnug4gxix1mj86w9p0ddwv3hdehc9mjawx7")
                .unwrap();
        vec![
            ConclusionEvent::Data(ConclusionData {
                index: 0,
                event_cid: model_stream_id.cid,
                init: ConclusionInit {
                    stream_cid: model_stream_id.cid,
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![("controller".to_string(), b"did:key:bob".to_vec())],
                },
                previous: vec![],
                data: format!(
                    r#"{{"metadata":{{"foo":1,"shouldIndex":true}},"content":{model_def_json}}}"#
                )
                .into(),
            }),
            ConclusionEvent::Data(ConclusionData {
                index: 1,
                event_cid: Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )
                .unwrap(),
                init: ConclusionInit {
                    stream_cid: instance_stream_id.cid,
                    stream_type: StreamIdType::ModelInstanceDocument as u8,
                    controller: "did:key:alice".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:alice".to_vec()),
                        ("model".to_string(), model_stream_id.to_vec()),
                        ("model_version".to_string(), model_stream_id.cid.to_bytes()),
                    ],
                },
                previous: vec![],
                data: r#"{"metadata":{"foo":1,"shouldIndex":true},"content":{"creator":"alice","red":255,"green":255,"blue":255}}"#.into(),
            }),
            ConclusionEvent::Time(ConclusionTime {
                index: 2,
                event_cid: Cid::from_str(
                    "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                ).unwrap(),
                init: ConclusionInit {
                    stream_cid: instance_stream_id.cid,
                    stream_type: StreamIdType::ModelInstanceDocument as u8,
                    controller: "did:key:alice".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:alice".to_vec()),
                        ("model".to_string(), model_stream_id.to_vec()),
                        ("model_version".to_string(), model_stream_id.cid.to_bytes()),
                    ],
                },
                previous: vec![Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                ).unwrap()],
            }),
            ConclusionEvent::Data(ConclusionData {
                index: 3,
                event_cid: Cid::from_str(
                    "baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em",
                )
                .unwrap(),
                init: ConclusionInit {
                    stream_cid: instance_stream_id.cid,
                    stream_type: StreamIdType::ModelInstanceDocument as u8,
                    controller: "did:key:alice".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:alice".to_vec()),
                        ("model".to_string(), model_stream_id.to_vec()),
                        ("model_version".to_string(), model_stream_id.cid.to_bytes()),
                    ],
                },
                previous: vec![
                 Cid::from_str(
                    "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                )
                .unwrap(),
                ],
                    data:r#"{"metadata":{"foo":2},"content":[{"op":"replace", "path": "/red", "value":0}]}"#.into(),
            }),
        ]
    }
    // Append a data event to the list of events with the give data that chains off the event at
    // the specified index.
    // NOTE: This always adds the data event at the end of the list.
    fn append_data_at(events: &mut Vec<ConclusionEvent>, at: usize, cid: Cid, data: &str) {
        events.push(ConclusionEvent::Data(ConclusionData {
            index: events.iter().map(|e| e.index()).max().unwrap_or(0) + 1,
            event_cid: cid,
            init: events[at].init().clone(),
            previous: vec![events[at].event_cid()],
            data: data.into(),
        }));
    }
    // Append a data event to the list of events with the give data that chains off the last event.
    fn append_data(events: &mut Vec<ConclusionEvent>, cid: Cid, data: &str) {
        append_data_at(events, events.len() - 1, cid, data)
    }
    // Append a data event that is an init event for the stream
    fn append_init(events: &mut Vec<ConclusionEvent>, cid: Cid, init: ConclusionInit, data: &str) {
        events.push(ConclusionEvent::Data(ConclusionData {
            index: events.iter().map(|e| e.index()).max().unwrap_or(0) + 1,
            event_cid: cid,
            init,
            previous: vec![],
            data: data.into(),
        }));
    }

    #[test(tokio::test)]
    async fn single_init_event_simple() {
        let event_states = do_test(
            conclusion_events_to_record_batch(model_and_mids_events()[0..1].iter()).unwrap(),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+--------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                           | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors |
            +-------+-------------------------------------------------------------+-------------+-------------+--------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | 0     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62} | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"version":"2.0","name":"TestSmallModel","interface":false,"implements":[],"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"SmallModel","type":"object","properties":{"blue":{"type":"integer","format":"int32"},"creator":{"type":"string"},"green":{"type":"integer","format":"int32"},"red":{"type":"integer","format":"int32"}},"additionalProperties":false,"required":["creator","red","green","blue"]},"accountRelation":{"type":"list"},"relations":{},"views":{}}} |                   |
            +-------+-------------------------------------------------------------+-------------+-------------+--------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+"#]].assert_eq(&event_states.to_string());
    }

    #[test(tokio::test)]
    async fn single_init_event_projection() {
        let ctx = init().await.unwrap();
        let conclusion_events =
            conclusion_events_to_record_batch(&[ConclusionEvent::Data(ConclusionData {
                index: 0,
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
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:bob".to_vec()),
                        ("model".to_string(), b"model".to_vec()),
                    ],
                },
                previous: vec![],
                data: r#"{"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}"#.into(),
            })])
            .unwrap();

        let mut subscription = ctx
            .actor_handle
            .send(SubscribeSinceMsg {
                projection: Some(vec![0, 2, 3]),
                offset: None,
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
            +-------+-------------+-------------+
            | index | stream_type | controller  |
            +-------+-------------+-------------+
            | 0     | 2           | did:key:bob |
            +-------+-------------+-------------+"#]]
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
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | index | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                                                                                               | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | 0     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62}                                                                                                                                                                                     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"version":"2.0","name":"TestSmallModel","interface":false,"implements":[],"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"SmallModel","type":"object","properties":{"blue":{"type":"integer","format":"int32"},"creator":{"type":"string"},"green":{"type":"integer","format":"int32"},"red":{"type":"integer","format":"int32"}},"additionalProperties":false,"required":["creator","red","green","blue"]},"accountRelation":{"type":"list"},"relations":{},"views":{}}} |                   |
            | 1     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"creator":"alice","red":255,"green":255,"blue":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  |                   |
            | 2     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"creator":"alice","red":255,"green":255,"blue":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  |                   |
            | 3     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    |                   |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+"#]].assert_eq(&event_states.to_string());
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
            +-------+-------------------------------------------------------------+-------------+-------------+--------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                           | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors |
            +-------+-------------------------------------------------------------+-------------+-------------+--------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | 0     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62} | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"version":"2.0","name":"TestSmallModel","interface":false,"implements":[],"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"SmallModel","type":"object","properties":{"blue":{"type":"integer","format":"int32"},"creator":{"type":"string"},"green":{"type":"integer","format":"int32"},"red":{"type":"integer","format":"int32"}},"additionalProperties":false,"required":["creator","red","green","blue"]},"accountRelation":{"type":"list"},"relations":{},"views":{}}} |                   |
            +-------+-------------------------------------------------------------+-------------+-------------+--------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+"#]].assert_eq(&event_states.to_string());
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            Some(0),
            Some(conclusion_events_to_record_batch(&events[1..2]).unwrap()),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+----------------------------------------------------------------------------------------------------------+-------------------+
            | index | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                                                                                               | event_cid                                                   | event_type | data                                                                                                     | validation_errors |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+----------------------------------------------------------------------------------------------------------+-------------------+
            | 1     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"creator":"alice","red":255,"green":255,"blue":255}} |                   |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+----------------------------------------------------------------------------------------------------------+-------------------+"#]].assert_eq(&event_states.to_string());
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            Some(1),
            Some(conclusion_events_to_record_batch(&events[2..3]).unwrap()),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+----------------------------------------------------------------------------------------------------------+-------------------+
            | index | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                                                                                               | event_cid                                                   | event_type | data                                                                                                     | validation_errors |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+----------------------------------------------------------------------------------------------------------+-------------------+
            | 2     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"creator":"alice","red":255,"green":255,"blue":255}} |                   |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+----------------------------------------------------------------------------------------------------------+-------------------+"#]].assert_eq(&event_states.to_string());
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            Some(2),
            Some(conclusion_events_to_record_batch(&events[3..]).unwrap()),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------------------------------------------------+-------------------+
            | index | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                                                                                               | event_cid                                                   | event_type | data                                                                                                   | validation_errors |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------------------------------------------------+-------------------+
            | 3     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}} |                   |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------------------------------------------------+-------------------+"#]].assert_eq(&event_states.to_string());
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
            +-------+-------------------------------------------------------------+-------------+-------------+--------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                           | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors |
            +-------+-------------------------------------------------------------+-------------+-------------+--------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | 0     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62} | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"version":"2.0","name":"TestSmallModel","interface":false,"implements":[],"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"SmallModel","type":"object","properties":{"blue":{"type":"integer","format":"int32"},"creator":{"type":"string"},"green":{"type":"integer","format":"int32"},"red":{"type":"integer","format":"int32"}},"additionalProperties":false,"required":["creator","red","green","blue"]},"accountRelation":{"type":"list"},"relations":{},"views":{}}} |                   |
            +-------+-------------------------------------------------------------+-------------+-------------+--------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+"#]].assert_eq(&event_states.to_string());
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            Some(0),
            Some(conclusion_events_to_record_batch(&events[1..]).unwrap()),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+----------------------------------------------------------------------------------------------------------+-------------------+
            | index | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                                                                                               | event_cid                                                   | event_type | data                                                                                                     | validation_errors |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+----------------------------------------------------------------------------------------------------------+-------------------+
            | 1     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"creator":"alice","red":255,"green":255,"blue":255}} |                   |
            | 2     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"creator":"alice","red":255,"green":255,"blue":255}} |                   |
            | 3     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}   |                   |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+----------------------------------------------------------------------------------------------------------+-------------------+"#]].assert_eq(&event_states.to_string());
        ctx.shutdown().await.unwrap();
    }
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

        let ctx = init_with_concluder(mock_concluder).await.unwrap();
        let event_states = do_pass(ctx.actor_handle.clone(), None, None).await.unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | index | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                                                                                               | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | 0     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62}                                                                                                                                                                                     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"version":"2.0","name":"TestSmallModel","interface":false,"implements":[],"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"SmallModel","type":"object","properties":{"blue":{"type":"integer","format":"int32"},"creator":{"type":"string"},"green":{"type":"integer","format":"int32"},"red":{"type":"integer","format":"int32"}},"additionalProperties":false,"required":["creator","red","green","blue"]},"accountRelation":{"type":"list"},"relations":{},"views":{}}} |                   |
            | 1     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"creator":"alice","red":255,"green":255,"blue":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  |                   |
            | 2     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"creator":"alice","red":255,"green":255,"blue":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  |                   |
            | 3     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    |                   |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+"#]].assert_eq(&event_states.to_string());
        ctx.shutdown().await.unwrap();
    }

    #[test(tokio::test)]
    async fn sub_concluder_preexisting() {
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
                    offset: None,
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
                +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
                | index | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                                                                                               | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors |
                +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
                | 0     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62}                                                                                                                                                                                     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"version":"2.0","name":"TestSmallModel","interface":false,"implements":[],"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"SmallModel","type":"object","properties":{"blue":{"type":"integer","format":"int32"},"creator":{"type":"string"},"green":{"type":"integer","format":"int32"},"red":{"type":"integer","format":"int32"}},"additionalProperties":false,"required":["creator","red","green","blue"]},"accountRelation":{"type":"list"},"relations":{},"views":{}}} |                   |
                | 1     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"creator":"alice","red":255,"green":255,"blue":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  |                   |
                +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+"#]].assert_eq(&event_states.to_string());
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
                offset: Some(1),
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
                offset: None,
                limit: Some(4),
            })
            .await
            .unwrap()
            .unwrap();
        let mut batches = Vec::new();
        while let Some(batch) = subscription.try_next().await.unwrap() {
            tracing::debug!(schema = ?batch.schema(), "batch");
            batches.push(batch)
        }
        println!("{}", pretty_format_batches(&batches).unwrap());
        let event_states = pretty_event_states(batches).await.unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | index | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                                                                                               | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | 0     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62}                                                                                                                                                                                     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"version":"2.0","name":"TestSmallModel","interface":false,"implements":[],"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"SmallModel","type":"object","properties":{"blue":{"type":"integer","format":"int32"},"creator":{"type":"string"},"green":{"type":"integer","format":"int32"},"red":{"type":"integer","format":"int32"}},"additionalProperties":false,"required":["creator","red","green","blue"]},"accountRelation":{"type":"list"},"relations":{},"views":{}}} |                   |
            | 1     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"creator":"alice","red":255,"green":255,"blue":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  |                   |
            | 2     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"creator":"alice","red":255,"green":255,"blue":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  |                   |
            | 3     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    |                   |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+"#]].assert_eq(&event_states.to_string());
        ctx.shutdown().await.unwrap();
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
                offset: None,
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
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | index | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                                                                                               | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | 0     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62}                                                                                                                                                                                     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"version":"2.0","name":"TestSmallModel","interface":false,"implements":[],"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"SmallModel","type":"object","properties":{"blue":{"type":"integer","format":"int32"},"creator":{"type":"string"},"green":{"type":"integer","format":"int32"},"red":{"type":"integer","format":"int32"}},"additionalProperties":false,"required":["creator","red","green","blue"]},"accountRelation":{"type":"list"},"relations":{},"views":{}}} |                   |
            | 1     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"creator":"alice","red":255,"green":255,"blue":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  |                   |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+"#]].assert_eq(&event_states.to_string());
        ctx.shutdown().await.unwrap();
    }

    #[test(tokio::test)]
    async fn stream_state() {
        let ctx = init().await.unwrap();

        ctx.actor_handle
            .send(NewConclusionEventsMsg {
                events: conclusion_events_to_record_batch(&[
                    ConclusionEvent::Data(ConclusionData {
                        index: 0,
                        event_cid: Cid::from_str(
                            "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                        ).unwrap(),
                        init: ConclusionInit {
                            stream_cid: Cid::from_str(
                                "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                            ).unwrap(),
                            stream_type: StreamIdType::Model as u8,
                            controller: "did:key:bob".to_string(),
                            dimensions: vec![
                                ("controller".to_string(), b"did:key:bob".to_vec()),
                                ("model".to_string(), b"model".to_vec()),
                            ],
                        },
                        previous: vec![],
                        data: r#"{"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}"#.into(),
                    }),
                    ConclusionEvent::Time(ConclusionTime {
                        index: 1,
                        event_cid: Cid::from_str(
                            "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                        ).unwrap(),
                        init: ConclusionInit {
                            stream_cid: Cid::from_str(
                                "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                            ).unwrap(),
                            stream_type: StreamIdType::Model as u8,
                            controller: "did:key:bob".to_string(),
                            dimensions: vec![
                                ("controller".to_string(), b"did:key:bob".to_vec()),
                                ("model".to_string(), b"model".to_vec()),
                            ],
                        },
                        previous: vec![Cid::from_str(
                            "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                        ).unwrap()],
                    }),
                    ConclusionEvent::Data(ConclusionData {
                        index: 2,
                        event_cid: Cid::from_str(
                            "baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du",
                        ).unwrap(),
                        init: ConclusionInit {
                            stream_cid: Cid::from_str(
                                "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                            ).unwrap(),
                            stream_type: StreamIdType::Model as u8,
                            controller: "did:key:bob".to_string(),
                            dimensions: vec![
                                ("controller".to_string(), b"did:key:bob".to_vec()),
                                ("model".to_string(), b"model".to_vec()),
                            ],
                        },
                        previous: vec![Cid::from_str(
                            "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                        ).unwrap()],
                        data: r#"{"metadata":{"shouldIndex":false},"content":[{"op":"replace", "path": "/a", "value":1}]}"#.into(),
                    }),
                ]).unwrap(),
            })
        .await
        .unwrap()
        .unwrap();

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
            .unwrap()
            .unwrap();
        expect![[r#"
            StreamState {
                id: "k2t6wz4yhfp1qrwhmopv1j2nctz0n36uj4ufi9fd9otwfvdgg2nbcn9hdcokjx",
                event_cid: "baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du",
                controller: "did:key:bob",
                dimensions: {
                    "controller": "uZGlkOmtleTpib2I",
                    "model": "ubW9kZWw",
                },
                data: "ueyJtZXRhZGF0YSI6eyJmb28iOjEsInNob3VsZEluZGV4IjpmYWxzZX0sImNvbnRlbnQiOnsiYSI6MX19",
            }
        "#]].assert_debug_eq(&state);
        expect![[r#"{"metadata":{"foo":1,"shouldIndex":false},"content":{"a":1}}"#]]
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

    #[test(tokio::test)]
    async fn update_model() {
        let mut events = model_and_mids_events()[0..1].to_vec();
        append_data(
            &mut events,
            Cid::from_str("baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm").unwrap(),
            // Adds alpha property to model
            r#"{"metadata":{"foo":2},"content":[{"op":"add", "path": "/schema/properties/alpha", "value":{"type":"integer","format":"int32"}}]}"#,
        );
        let event_states = do_test(conclusion_events_to_record_batch(&events).unwrap())
            .await
            .unwrap();

        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+--------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                           | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | validation_errors |
            +-------+-------------------------------------------------------------+-------------+-------------+--------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | 0     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62} | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"version":"2.0","name":"TestSmallModel","interface":false,"implements":[],"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"SmallModel","type":"object","properties":{"blue":{"type":"integer","format":"int32"},"creator":{"type":"string"},"green":{"type":"integer","format":"int32"},"red":{"type":"integer","format":"int32"}},"additionalProperties":false,"required":["creator","red","green","blue"]},"accountRelation":{"type":"list"},"relations":{},"views":{}}}                                             |                   |
            | 1     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62} | baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm | 0          | {"metadata":{"foo":2,"shouldIndex":true},"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"alpha":{"format":"int32","type":"integer"},"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}}} |                   |
            +-------+-------------------------------------------------------------+-------------+-------------+--------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn invalid_instance_data() {
        let mut events = model_and_mids_events();
        append_data(
            &mut events,
            Cid::from_str("baeabeicklrck72l22zzc324b7ordbhq2amnhhivabosngfkbhk2nmuyqom").unwrap(),
            r#"{"metadata":{"foo":2},"content":[{"op":"add", "path": "/color", "value":"00FFFF"}]}"#,
        );
        let event_states = do_test(conclusion_events_to_record_batch(&events).unwrap())
            .await
            .unwrap();

        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller    | dimensions                                                                                                                                                                                                               | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | validation_errors                                                                                  |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+
            | 0     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob   | {controller: 6469643a6b65793a626f62}                                                                                                                                                                                     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"version":"2.0","name":"TestSmallModel","interface":false,"implements":[],"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"SmallModel","type":"object","properties":{"blue":{"type":"integer","format":"int32"},"creator":{"type":"string"},"green":{"type":"integer","format":"int32"},"red":{"type":"integer","format":"int32"}},"additionalProperties":false,"required":["creator","red","green","blue"]},"accountRelation":{"type":"list"},"relations":{},"views":{}}} |                                                                                                    |
            | 1     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"creator":"alice","red":255,"green":255,"blue":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  |                                                                                                    |
            | 2     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"creator":"alice","red":255,"green":255,"blue":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                  |                                                                                                    |
            | 3     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeibrtuyyqwd6y4aa62qxaimjhafielf7fc22fa5b7i7vptcu5263em | 0          | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                                    |                                                                                                    |
            | 4     | baeabeicdwdrilh6gazn6a7eruxbt5q46cquzimxsk52vcwobfvjhlndafm | 3           | did:key:alice | {controller: 6469643a6b65793a616c696365, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeicklrck72l22zzc324b7ordbhq2amnhhivabosngfkbhk2nmuyqom | 0          | {"metadata":{"foo":2,"shouldIndex":true},"content":{"blue":255,"color":"00FFFF","creator":"alice","green":255,"red":0}}                                                                                                                                                                                                                                                                                                                                                                                                                                   | [Failed to validate schema:  'Additional properties are not allowed ('color' was unexpected)' at ] |
            +-------+-------------------------------------------------------------+-------------+---------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn update_model_with_new_instances() {
        let mut events = model_and_mids_events()[0..1].to_vec();
        let model_update =
            Cid::from_str("baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm").unwrap();
        append_data(
            &mut events,
            model_update,
            // Adds alpha property to model
            r#"{"metadata":{"foo":2},"content":[{"op":"add", "path": "/schema/properties/alpha", "value":{"type":"number"}}]}"#,
        );
        let model_stream_id =
            StreamId::from_str("k2t6wz4yhfp1pc9l42mm6vh20xmhm9ac7cznnpu4xcxe4jds13l9sjknm1accd")
                .unwrap();
        append_init(
            &mut events,
            Cid::from_str("baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm").unwrap(),
            ConclusionInit {
                stream_cid: model_stream_id.cid,
                stream_type: StreamIdType::ModelInstanceDocument as u8,
                controller: "did:key:sue".to_owned(),
                dimensions: vec![
                    ("controller".to_string(), b"did:key:sue".to_vec()),
                    ("model".to_string(), model_stream_id.to_vec()),
                    // NOTE: here we still reference the original model so this event will pass
                    // validation as it still uses the old schema.
                    ("model_version".to_string(), model_stream_id.cid.to_bytes()),
                ],
            },
            r#"{"metadata":{"foo":2},"content":{"creator":"sue","red":0,"green":0,"blue":255}}"#,
        );
        append_init(
            &mut events,
            Cid::from_str("baeabeic4t36e3okvapq2uusuc2inb6h6kbzkseba3fcjzzwpa3d7vvf4ii").unwrap(),
            ConclusionInit {
                stream_cid: model_stream_id.cid,
                stream_type: StreamIdType::ModelInstanceDocument as u8,
                controller: "did:key:kev".to_owned(),
                dimensions: vec![
                    ("controller".to_string(), b"did:key:kev".to_vec()),
                    ("model".to_string(), model_stream_id.to_vec()),
                    // NOTE: here we still reference the original model so this event will fail
                    // validation as it uses the new schema.
                    ("model_version".to_string(), model_stream_id.cid.to_bytes()),
                ],
            },
            r#"{"metadata":{"foo":2},"content":{"creator":"kev","red":0,"green":0,"blue":255,"alpha":0.75}}"#,
        );
        append_init(
            &mut events,
            Cid::from_str("baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe").unwrap(),
            ConclusionInit {
                stream_cid: model_stream_id.cid,
                stream_type: StreamIdType::ModelInstanceDocument as u8,
                controller: "did:key:jim".to_owned(),
                dimensions: vec![
                    ("controller".to_string(), b"did:key:jim".to_vec()),
                    ("model".to_string(), model_stream_id.to_vec()),
                    // NOTE: here we reference the new model version so this event will pass
                    // validation as it uses the new schema.
                    ("model_version".to_string(), model_update.to_bytes()),
                ],
            },
            r#"{"metadata":{"foo":2},"content":{"creator":"jim","red":0,"green":0,"blue":255,"alpha":0.75}}"#,
        );
        let event_states = do_test(conclusion_events_to_record_batch(&events).unwrap())
            .await
            .unwrap();

        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                                                                                                                                                                                           | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | validation_errors                                                                                  |
            +-------+-------------------------------------------------------------+-------------+-------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+
            | 0     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62}                                                                                                                                                                                 | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"version":"2.0","name":"TestSmallModel","interface":false,"implements":[],"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"SmallModel","type":"object","properties":{"blue":{"type":"integer","format":"int32"},"creator":{"type":"string"},"green":{"type":"integer","format":"int32"},"red":{"type":"integer","format":"int32"}},"additionalProperties":false,"required":["creator","red","green","blue"]},"accountRelation":{"type":"list"},"relations":{},"views":{}}}                           |                                                                                                    |
            | 1     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62}                                                                                                                                                                                 | baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm | 0          | {"metadata":{"foo":2,"shouldIndex":true},"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"alpha":{"type":"number"},"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}}} |                                                                                                    |
            | 2     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 3           | did:key:sue | {controller: 6469643a6b65793a737565, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm | 0          | {"metadata":{"foo":2},"content":{"creator":"sue","red":0,"green":0,"blue":255}}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |                                                                                                    |
            | 3     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 3           | did:key:kev | {controller: 6469643a6b65793a6b6576, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 01001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd} | baeabeic4t36e3okvapq2uusuc2inb6h6kbzkseba3fcjzzwpa3d7vvf4ii | 0          | {"metadata":{"foo":2},"content":{"creator":"kev","red":0,"green":0,"blue":255,"alpha":0.75}}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | [Failed to validate schema:  'Additional properties are not allowed ('alpha' was unexpected)' at ] |
            | 4     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 3           | did:key:jim | {controller: 6469643a6b65793a6a696d, model: ce010201001220809c5470e3635e495f5a98437de616b6612da8b3753fc2ee34a8324ab68585fd, model_version: 010012201548068fec3901c0076ad0971a52d556099f2c3ce2b509bb1a49e77f20fc9863} | baeabeify7qxwjujhkxhui4atl3a6r5bo6ggx7ehwuajnzh4ohcnqumtvoe | 0          | {"metadata":{"foo":2},"content":{"creator":"jim","red":0,"green":0,"blue":255,"alpha":0.75}}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |                                                                                                    |
            +-------+-------------------------------------------------------------+-------------+-------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn invalid_update_model_new_required() {
        let mut events = model_and_mids_events()[0..1].to_vec();
        append_data(
            &mut events,
            Cid::from_str("baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm").unwrap(),
            // Adds alpha property to model
            r#"{"metadata":{"foo":2},"content":[{"op":"add", "path": "/schema/properties/alpha", "value":{"type":"integer","format":"int32"}},{"op":"replace","path":"/schema/required","value":["creator","red","green","blue","alpha"]}]}"#,
        );
        let event_states = do_test(conclusion_events_to_record_batch(&events).unwrap())
            .await
            .unwrap();

        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+--------------------------------------+-------------------------------------------------------------+------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                           | event_cid                                                   | event_type | data                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | validation_errors |
            +-------+-------------------------------------------------------------+-------------+-------------+--------------------------------------+-------------------------------------------------------------+------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+
            | 0     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62} | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"version":"2.0","name":"TestSmallModel","interface":false,"implements":[],"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"SmallModel","type":"object","properties":{"blue":{"type":"integer","format":"int32"},"creator":{"type":"string"},"green":{"type":"integer","format":"int32"},"red":{"type":"integer","format":"int32"}},"additionalProperties":false,"required":["creator","red","green","blue"]},"accountRelation":{"type":"list"},"relations":{},"views":{}}}                                                     |                   |
            | 1     | baeabeieatrkhby3dlzev6wuyin66mfvwmew2rm3vh7bo4nfigjflnbmf7u | 2           | did:key:bob | {controller: 6469643a6b65793a626f62} | baeabeiavjadi73bzahaao2wqs4nffvkwbgpsyphcwue3wgsj457sb7eymm | 0          | {"metadata":{"foo":2,"shouldIndex":true},"content":{"accountRelation":{"type":"list"},"implements":[],"interface":false,"name":"TestSmallModel","relations":{},"schema":{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"alpha":{"format":"int32","type":"integer"},"blue":{"format":"int32","type":"integer"},"creator":{"type":"string"},"green":{"format":"int32","type":"integer"},"red":{"format":"int32","type":"integer"}},"required":["creator","red","green","blue","alpha"],"title":"SmallModel","type":"object"},"version":"2.0","views":{}}} | SHOULD FAIL       |
            +-------+-------------------------------------------------------------+-------------+-------------+--------------------------------------+-------------------------------------------------------------+------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------------+"#]].assert_eq(&event_states.to_string());
    }
}
