//! Aggregation functions for Ceramic Model Instance Document streams.
//!
//! Applies each new event to the previous state of the stream producing the stream state at each
//! event in the stream.
mod ceramic_patch;
mod metrics;
#[cfg(any(test, feature = "mock"))]
pub mod mock;

use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use arrow::{
    array::{Array as _, ArrayAccessor as _, BinaryArray, RecordBatch},
    compute::concat_batches,
    datatypes::Int32Type,
};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use ceramic_actor::{actor_envelope, Actor, Handler, Message};
use ceramic_core::StreamId;
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
        col, dml::InsertOp, expr::WindowFunction, lit, Expr, ExprFunctionExt as _,
        WindowFunctionDefinition, UNNAMED_TABLE,
    },
    physical_plan::stream::RecordBatchStreamAdapter,
    prelude::SessionContext,
    sql::TableReference,
};
use futures::TryStreamExt as _;
use shutdown::{Shutdown, ShutdownSignal};
use tokio::{select, sync::broadcast, task::JoinHandle};
use tracing::{debug, error, instrument};

use crate::{
    cache_table::CacheTable,
    concluder::ConcluderHandle,
    metrics::Metrics,
    schemas,
    since::{rows_since, StreamTable, StreamTableSource},
    PipelineContext, Result, SessionContextRef,
};
// Use the SubscribeSinceMsg so its clear its a message for this actor
pub use crate::since::SubscribeSinceMsg;

const EVENT_STATES_TABLE: &str = "ceramic.v0.event_states";
const EVENT_STATES_STREAM_TABLE: &str = "ceramic.v0.event_states_stream";
const EVENT_STATES_MEM_TABLE: &str = "ceramic._internal.event_states_mem";
const EVENT_STATES_PERSISTENT_TABLE: &str = "ceramic._internal.event_states_persistent";

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
        // Register aggregator tables
        let file_format = ParquetFormat::default().with_enable_pruning(true);

        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet")
            .with_file_sort_order(vec![vec![col("index").sort(true, true)]]);

        // Set the path within the bucket for the event_states table
        let event_states_object_store_path = EVENT_STATES_TABLE.replace('.', "/") + "/";
        let mut url = ctx.object_store_url.clone();
        url.set_path(&event_states_object_store_path);
        // Register event_states_persistent as a listing table
        ctx.session().register_table(
            EVENT_STATES_PERSISTENT_TABLE,
            Arc::new(ListingTable::try_new(
                ListingTableConfig::new(ListingTableUrl::parse(url)?)
                    .with_listing_options(listing_options)
                    .with_schema(schemas::event_states()),
            )?),
        )?;

        ctx.session().register_table(
            EVENT_STATES_MEM_TABLE,
            Arc::new(CacheTable::try_new(
                schemas::event_states(),
                vec![vec![RecordBatch::new_empty(schemas::event_states())]],
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
                EVENT_STATES_STREAM_TABLE,
                Arc::new(StreamTable::new(handle.clone())),
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
                            error!(%err, "concluder subscription loop failed to process new conclusion events");
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
    ])?;
    if let Some(offset) = offset {
        event_states = event_states.filter(col("index").gt(lit(offset)))?;
    }
    Ok(event_states.execute_stream().await?)
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
        let batch =
            process_conclusion_events_batch(self.ctx.clone(), message.events, self.max_cached_rows)
                .await?;
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

// Process a batch of conclusion events, producing a new document state for each input event.
// The conclusion event must be ordered by "index".
// The output event states are ordered by "index".
#[instrument(skip(ctx, conclusion_events))]
async fn process_conclusion_events_batch(
    ctx: SessionContextRef,
    conclusion_events: RecordBatch,
    max_cached_rows: usize,
) -> Result<RecordBatch> {
    let event_states = ctx
        .table(EVENT_STATES_TABLE)
        .await?
        .select_columns(&["stream_cid", "event_cid", "data"])
        .context("reading event_states")?;

    // Construct a column for the field in the UNNAMED_TABLE.
    fn anon_col(field: &str) -> Expr {
        Expr::Column(Column {
            relation: Some(TableReference::Bare {
                table: UNNAMED_TABLE.into(),
            }),
            name: field.into(),
        })
    }

    let new_event_states = ctx
        .read_batch(conclusion_events)?
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
        ])?
        .join_on(
            event_states,
            JoinType::Left,
            [col("previous").eq(col(EVENT_STATES_TABLE.to_string() + ".event_cid"))],
        )?
        .select(vec![
            anon_col("index").alias("index"),
            anon_col("event_type").alias("event_type"),
            anon_col("stream_cid").alias("stream_cid"),
            anon_col("stream_type").alias("stream_type"),
            anon_col("controller").alias("controller"),
            anon_col("dimensions").alias("dimensions"),
            anon_col("event_cid").alias("event_cid"),
            col("previous"),
            col(EVENT_STATES_TABLE.to_string() + ".data").alias("previous_data"),
            anon_col("data").alias("data"),
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
            "new_data",
        ])?
        .with_column_renamed("new_data", "data")?
        .sort(vec![col("index").sort(true, true)])?
        .collect()
        .await?;

    // Concatenating batches requires allocating a new batch, so we do this now so we can reuse the
    // vector of batches for writing to the table and only have to copy the data once.
    let new_event_states_batch = concat_batches(&schemas::event_states(), &new_event_states)?;

    // Write states to the in memory event_states table
    ctx.read_batches(new_event_states)?
        .write_table(EVENT_STATES_MEM_TABLE, DataFrameWriteOptions::new())
        .await
        .context("computing data")?;

    let count = ctx.table(EVENT_STATES_MEM_TABLE).await?.count().await?;
    // If we have enough data cached in memory write it out to persistent store
    if count >= max_cached_rows {
        ctx.table(EVENT_STATES_MEM_TABLE)
            .await?
            .write_table(EVENT_STATES_PERSISTENT_TABLE, DataFrameWriteOptions::new())
            .await?;
        // Clear all data in the memory batch, by writing an empty batch
        ctx.read_batch(RecordBatch::new_empty(schemas::event_states()))?
            .write_table(
                EVENT_STATES_MEM_TABLE,
                DataFrameWriteOptions::new().with_insert_operation(InsertOp::Overwrite),
            )
            .await?;
    }
    Ok(new_event_states_batch)
}

#[async_trait]
impl StreamTableSource for AggregatorHandle {
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
    use test_log::test;

    use crate::{
        cid_string::CidString, concluder::mock::MockConcluder, conclusion_events_to_record_batch,
        pipeline_ctx, tests::TestContext, ConclusionData, ConclusionEvent, ConclusionInit,
        ConclusionTime,
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
        let cid_string = Arc::new(ScalarUDF::from(CidString::new()));
        let event_states = SessionContext::new()
            .read_batches(batches)?
            .select(vec![
                col("index"),
                Expr::ScalarFunction(ScalarFunction::new_udf(
                    cid_string.clone(),
                    vec![col("stream_cid")],
                ))
                .alias("stream_cid"),
                col("stream_type"),
                col("controller"),
                col("dimensions"),
                Expr::ScalarFunction(ScalarFunction::new_udf(cid_string, vec![col("event_cid")]))
                    .alias("event_cid"),
                col("event_type"),
                cast(col("data"), DataType::Utf8).alias("data"),
            ])?
            .sort(vec![col("index").sort(true, true)])?
            .collect()
            .await?;
        Ok(pretty_format_batches(&event_states)?)
    }

    #[test(tokio::test)]
    async fn single_init_event_simple() {
        let event_states = do_test(
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
            .unwrap(),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                        |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
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
    async fn multiple_data_events() {
        let event_states = do_test(conclusion_events_to_record_batch(&[
            ConclusionEvent::Data(ConclusionData {
                index: 1,
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
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                ).unwrap()],
                data:
                    r#"{"metadata":{"foo":2},"content":[{"op":"replace", "path": "/a", "value":1}]}"#
                        .into(),
            }),
        ]).unwrap())
        .await.unwrap();

        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                        |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}} |
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | 0          | {"metadata":{"foo":2,"shouldIndex":true},"content":{"a":1}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
    }
    #[test(tokio::test)]
    async fn multiple_data_and_time_events() {
        let event_states = do_test(conclusion_events_to_record_batch(&[
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
        ]).unwrap())
        .await.unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                         |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}  |
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}  |
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | 0          | {"metadata":{"foo":1,"shouldIndex":false},"content":{"a":1}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
    }

    #[test(tokio::test)]
    async fn multiple_single_event_passes() {
        // Test multiple passes where a single event for the stream is present in the conclusion
        // events for each pass.
        let ctx = init().await.unwrap();
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            None,
            Some(
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
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                        |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            Some(0),
            Some(
                conclusion_events_to_record_batch(&[ConclusionEvent::Time(ConclusionTime {
                    index: 1,
                    event_cid: Cid::from_str(
                        "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
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
                    previous: vec![Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )
                    .unwrap()],
                })])
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                        |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            Some(1),
            Some(conclusion_events_to_record_batch(&[ConclusionEvent::Data(ConclusionData {
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
            })]).unwrap()),
        )
        .await.unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                         |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | 0          | {"metadata":{"foo":1,"shouldIndex":false},"content":{"a":1}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
        ctx.shutdown().await.unwrap();
    }
    #[test(tokio::test)]
    async fn multiple_passes() {
        let ctx = init().await.unwrap();
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            None,
            Some(
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
                .unwrap(),
            ),
        )
        .await
        .unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                        |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
        let event_states = do_pass(
            ctx.actor_handle.clone(),
            Some(0),
            Some(conclusion_events_to_record_batch(&[
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
            ]).unwrap()),
        )
        .await.unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                         |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}  |
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | 0          | {"metadata":{"foo":1,"shouldIndex":false},"content":{"a":1}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
        ctx.shutdown().await.unwrap();
    }
    #[test(tokio::test)]
    async fn sub_concluder() {
        // This test ensures that the logic that polls the concluder actor to check for new events
        // finds them and passes them into the aggregator actor.
        let conclusion_events = conclusion_events_to_record_batch(&[
            ConclusionEvent::Data(ConclusionData {
                index: 1,
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
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                ).unwrap()],
                data: r#"{"metadata":{"shouldIndex":false},"content":[{"op":"replace", "path": "/a", "value":1}]}"#.into(),
            }),
            ConclusionEvent::Data(ConclusionData {
                index: 3,
                event_cid: Cid::from_str(
                    "baeabeic2caccyfigwadnncpyko7hcdbr66dkf4jyzeoh4dbhfebp77hchu",
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
                    "baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du",
                ).unwrap()],
                data: r#"{"metadata":{},"content":[{"op":"replace", "path": "/a", "value":2}]}"#.into(),
            }),
        ]);

        // Setup mock that returns the conclusion_events
        let mut mock_concluder = MockConcluder::new();
        mock_concluder
            .expect_handle_subscribe_since()
            .once()
            .return_once(|_msg| {
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    schemas::conclusion_events(),
                    stream::iter(conclusion_events.into_iter().map(|e| Ok(e))),
                )))
            });

        let ctx = init_with_concluder(mock_concluder).await.unwrap();
        let event_states = do_pass(ctx.actor_handle.clone(), None, None).await.unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                         |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}  |
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | 0          | {"metadata":{"foo":1,"shouldIndex":false},"content":{"a":1}} |
            | 3     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeic2caccyfigwadnncpyko7hcdbr66dkf4jyzeoh4dbhfebp77hchu | 0          | {"metadata":{"foo":1,"shouldIndex":false},"content":{"a":2}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
        ctx.shutdown().await.unwrap();
    }

    #[test(tokio::test)]
    async fn sub_concluder_preexisting() {
        // This test ensures that the logic that polls the concluder actor starts at the correct
        // offset when there is preexisting data.

        // Use the same object store in both instances of the actor as that is where data is
        // physically stored.
        let object_store = Arc::new(InMemory::new());
        {
            let first_events = conclusion_events_to_record_batch(&[
            ConclusionEvent::Data(ConclusionData {
                index: 1,
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
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                ).unwrap()],
                data: r#"{"metadata":{"shouldIndex":false},"content":[{"op":"replace", "path": "/a", "value":1}]}"#.into(),
            }),
        ]);

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
                        stream::iter(first_events.into_iter().map(|e| Ok(e))),
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
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                         |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}  |
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | 0          | {"metadata":{"foo":1,"shouldIndex":false},"content":{"a":1}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
            ctx.shutdown().await.unwrap();
        }

        // Now we have existing data, start a new aggregator actor and ensure that it starts
        // where it left off.
        // Setup mock that returns a second batch of conclusion_events
        let second_events =
            conclusion_events_to_record_batch(&[ConclusionEvent::Data(ConclusionData {
                index: 3,
                event_cid: Cid::from_str(
                    "baeabeic2caccyfigwadnncpyko7hcdbr66dkf4jyzeoh4dbhfebp77hchu",
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
                previous: vec![Cid::from_str(
                    "baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du",
                )
                .unwrap()],
                data: r#"{"metadata":{},"content":[{"op":"replace", "path": "/a", "value":2}]}"#
                    .into(),
            })]);
        let mut mock_concluder = MockConcluder::new();
        mock_concluder
            .expect_handle_subscribe_since()
            .once()
            .with(predicate::eq(SubscribeSinceMsg {
                projection: None,
                offset: Some(2),
                limit: None,
            }))
            .return_once(|_msg| {
                Ok(Box::pin(RecordBatchStreamAdapter::new(
                    schemas::conclusion_events(),
                    stream::iter(second_events.into_iter().map(|e| Ok(e))),
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
                limit: None,
            })
            .await
            .unwrap()
            .unwrap();
        // The fact there are two batches is an implementation detail.
        // Unfortunately we need to poll the stream for the exact number of batches we expect
        // becuase the stream is unbounded.
        let first_batch = subscription
            .try_next()
            .await
            .unwrap()
            .ok_or_else(|| anyhow::anyhow!("no events found"))
            .unwrap();
        let second_batch = subscription
            .try_next()
            .await
            .unwrap()
            .ok_or_else(|| anyhow::anyhow!("no events found"))
            .unwrap();
        let event_states = pretty_event_states(vec![first_batch, second_batch])
            .await
            .unwrap();
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                         |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}  |
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | 0          | {"metadata":{"foo":1,"shouldIndex":false},"content":{"a":1}} |
            | 3     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeic2caccyfigwadnncpyko7hcdbr66dkf4jyzeoh4dbhfebp77hchu | 0          | {"metadata":{"foo":1,"shouldIndex":false},"content":{"a":2}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
        ctx.shutdown().await.unwrap();
    }

    #[test(tokio::test)]
    async fn subscribe_aggregator_limit() {
        let ctx = init().await.unwrap();

        let conclusion_events= conclusion_events_to_record_batch(&[
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
        ]).unwrap();

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
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                        |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}} |
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
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
}
