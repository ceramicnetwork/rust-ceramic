//! Conflict resolution functions for Ceramic Model Instance Document streams.
//!
//! Subscribes to aggregated events and produces the tips and canonical tip for each stream.
//!

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
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use ceramic_actor::{actor_envelope, Actor, Handler, Message};
use ceramic_core::StreamId;
use cid::Cid;
use datafusion::{
    common::{
        cast::{
            as_binary_array, as_dictionary_array, as_map_array, as_string_array, as_uint64_array,
        },
        exec_datafusion_err,
    },
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    execution::SendableRecordBatchStream,
    functions_aggregate::expr_fn::last_value,
    logical_expr::{col, lit, ExprFunctionExt as _},
    physical_plan::stream::RecordBatchStreamAdapter,
    prelude::{wildcard, SessionContext},
};
use futures::TryStreamExt as _;
use shutdown::{Shutdown, ShutdownSignal};
use tokio::{select, sync::broadcast, task::JoinHandle};
use tracing::{debug, error, instrument};

use crate::{
    aggregator::AggregatorHandle,
    cache_table::CacheTable,
    metrics::Metrics,
    schemas,
    since::{rows_since, FeedTable, FeedTableSource},
    PipelineContext, Result, SessionContextRef,
};
// Use the SubscribeSinceMsg so its clear its a message for this actor
pub use crate::since::SubscribeSinceMsg;

const STREAM_TIPS_TABLE: &str = "ceramic.v0.stream_tips";
const STREAM_TIPS_MEM_TABLE: &str = "ceramic._internal.stream_tips_mem";
const STREAM_TIPS_PERSISTENT_TABLE: &str = "ceramic._internal.stream_tips_persistent";
// Path within the object store where the stream tips table is stored
// This path should be updated when the underlying storage structure changes. (i.e. the parition
// columns change). The revision is a physical versioning number and not directly associated with
// the logical schema version of the table. There are many cases where the physical revision may
// need to change while the logical version remains the same.
const STREAM_TIPS_TABLE_OBJECT_STORE_PATH: &str = "ceramic/rev0/stream_tips/";

const STREAM_STATES_TABLE: &str = "ceramic.v0.stream_states";
const STREAM_STATES_FEED_TABLE: &str = "ceramic.v0.stream_states_feed";
const STREAM_STATES_MEM_TABLE: &str = "ceramic._internal.stream_states_mem";
const STREAM_STATES_PERSISTENT_TABLE: &str = "ceramic._internal.stream_states_persistent";
// Path within the object store where the stream states table is stored
// This path should be updated when the underlying storage structure changes. (i.e. the parition
// columns change). The revision is a physical versioning number and not directly associated with
// the logical schema version of the table. There are many cases where the physical revision may
// need to change while the logical version remains the same.
const STREAM_STATES_TABLE_OBJECT_STORE_PATH: &str = "ceramic/rev0/stream_states/";

// Maximum number of rows to cache in memory before writing to object store.
const DEFAULT_MAX_CACHED_ROWS: usize = 10_000;

/// Resolver is responsible for computing the tips of a stream and resolving the set of tips to a
/// canonical tip.
/// The resolver only operates on model and model instance stream types.
#[derive(Actor)]
pub struct Resolver {
    ctx: SessionContextRef,
    broadcast_tx: broadcast::Sender<RecordBatch>,
    max_cached_rows: usize,
    order: u64,
}

impl std::fmt::Debug for Resolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Resolver")
            .field("ctx", &"Pipeline(SessionContext)")
            .field("broadcast_tx", &self.broadcast_tx)
            .finish()
    }
}

impl Resolver {
    /// Create a new resolver actor and spawn its tasks.
    pub async fn spawn_new(
        size: usize,
        ctx: &PipelineContext,
        max_cached_rows: Option<usize>,
        aggregator: AggregatorHandle,
        metrics: Metrics,
        shutdown: Shutdown,
    ) -> Result<(ResolverHandle, Vec<JoinHandle<()>>)> {
        // Register stream_tips tables
        let file_format = ParquetFormat::default().with_enable_pruning(true);
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![("stream_cid_partition".to_owned(), DataType::Int32)])
            .with_file_sort_order(vec![vec![col("stream_cid").sort(true, true)]]);

        // Set the path within the bucket for the event_states table
        let mut stream_tips_url = ctx.object_store_url.clone();
        stream_tips_url.set_path(STREAM_TIPS_TABLE_OBJECT_STORE_PATH);
        // Register event_states_persistent as a listing table
        ctx.session().register_table(
            STREAM_TIPS_PERSISTENT_TABLE,
            Arc::new(ListingTable::try_new(
                ListingTableConfig::new(ListingTableUrl::parse(stream_tips_url)?)
                    .with_listing_options(listing_options)
                    // Use the non partitioned schema as the parquet files themselves do not
                    // contain the partition columns.
                    .with_schema(schemas::event_states()),
            )?),
        )?;

        ctx.session().register_table(
            STREAM_TIPS_MEM_TABLE,
            Arc::new(CacheTable::try_new(
                schemas::event_states_partitioned(),
                vec![vec![RecordBatch::new_empty(
                    schemas::event_states_partitioned(),
                )]],
            )?),
        )?;

        ctx.session().register_table(
            STREAM_TIPS_TABLE,
            ctx.session()
                .table(STREAM_TIPS_MEM_TABLE)
                .await?
                .union(ctx.session().table(STREAM_TIPS_PERSISTENT_TABLE).await?)?
                .into_view(),
        )?;

        // Register stream_states tables
        let file_format = ParquetFormat::default().with_enable_pruning(true);
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![("stream_cid_partition".to_owned(), DataType::Int32)])
            .with_file_sort_order(vec![vec![col("stream_cid").sort(true, true)]]);

        // Set the path within the bucket for the event_states table
        let mut stream_states_url = ctx.object_store_url.clone();
        stream_states_url.set_path(STREAM_STATES_TABLE_OBJECT_STORE_PATH);
        // Register event_states_persistent as a listing table
        ctx.session().register_table(
            STREAM_STATES_PERSISTENT_TABLE,
            Arc::new(ListingTable::try_new(
                ListingTableConfig::new(ListingTableUrl::parse(stream_states_url)?)
                    .with_listing_options(listing_options)
                    // Use the non partitioned schema as the parquet files themselves do not
                    // contain the partition columns.
                    .with_schema(schemas::event_states()),
            )?),
        )?;

        ctx.session().register_table(
            STREAM_STATES_MEM_TABLE,
            Arc::new(CacheTable::try_new(
                schemas::event_states_partitioned(),
                vec![vec![RecordBatch::new_empty(
                    schemas::event_states_partitioned(),
                )]],
            )?),
        )?;

        ctx.session().register_table(
            STREAM_STATES_TABLE,
            ctx.session()
                .table(STREAM_STATES_MEM_TABLE)
                .await?
                .union(ctx.session().table(STREAM_STATES_PERSISTENT_TABLE).await?)?
                .into_view(),
        )?;

        // Query for max event_state_order and stream_state_order in persistent stream_states, this is where we should start
        // the new order values.
        let batches = ctx
            .session()
            .table(STREAM_STATES_PERSISTENT_TABLE)
            .await?
            .aggregate(
                vec![],
                vec![
                    datafusion::functions_aggregate::min_max::max(col("event_state_order"))
                        .alias("max_event_state_order"),
                    datafusion::functions_aggregate::min_max::max(col("stream_state_order"))
                        .alias("max_stream_state_order"),
                ],
            )?
            .collect()
            .await?;
        let max_event_state_order = batches.first().and_then(|batch| {
            batch
                .column_by_name("max_event_state_order")
                .and_then(|col| {
                    as_uint64_array(&col)
                        .ok()
                        .and_then(|col| col.is_valid(0).then(|| col.value(0)))
                })
        });
        let max_stream_state_order = batches.first().and_then(|batch| {
            batch
                .column_by_name("max_stream_state_order")
                .and_then(|col| {
                    as_uint64_array(&col)
                        .ok()
                        .and_then(|col| col.is_valid(0).then(|| col.value(0)))
                })
        });

        // Spawn actor
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(1_000);
        let resolver = Resolver {
            ctx: ctx.session(),
            broadcast_tx,
            max_cached_rows: max_cached_rows.unwrap_or(DEFAULT_MAX_CACHED_ROWS),
            order: max_stream_state_order.unwrap_or_default(),
        };

        let (handle, task_handle) = Self::spawn(size, resolver, metrics, shutdown.wait_fut());
        let h = handle.clone();
        let sub_handle = tokio::spawn(async move {
            if let Err(err) =
                aggregator_subscription(shutdown.wait_fut(), aggregator, h, max_event_state_order)
                    .await
            {
                error!(%err, "resolver actor aggregator_subscription task failed");
            }
        });

        ctx.session()
            .register_table(
                STREAM_STATES_FEED_TABLE,
                Arc::new(FeedTable::new(handle.clone())),
            )
            .expect("should be able to register table");

        Ok((handle, vec![task_handle, sub_handle]))
    }

    // Process batch of new event states.
    // Return any new stream states.
    #[instrument(skip(self, event_states), err)]
    async fn process_event_states_batch(
        &mut self,
        event_states: RecordBatch,
    ) -> Result<RecordBatch> {
        // Psuedocode of the data flow of conflict resolution.
        // The process involves two tables and two udf.
        // The first table and udf are responsible for simply determining the new tips.
        // This should be possible with just information about the previous pointers.
        // (TODO: The previous value is not preserved from conclusion_events to event_states but is
        // straightforward to do)
        //
        // The second table and udf then does actual conflict resolution given all the tips. This can then
        // use time information event height and cids to have a deterministic and canonical tip for
        // a stream.
        //
        // event_states
        //  // Left join the new events with existing known stream tips
        //  >> join(stream_tips)
        //  // Now we have a new event and a list of existing tips. Pass all of this information
        //  // into a udf. That UDF will produce a new row that contains a list of the new tips.
        //  // The new tips may be the same, more or less than before.
        //  // This should be possible without knowing the contents of the events themselves
        //  >> compute_new_tips_udf()
        //  // Write the new tips row per stream
        //  >> write_to(stream_tips)
        //  // Unnest the list of tips so we have a row per tip per stream
        //  >> unnest(tips)
        //  // Join the tips with the event_states table as the tips are just cids
        //  >> join(event_states)
        //  // Now we have a set of rows per stream one per tip.
        //  // Use an aggregate udf to resolve the conflicts producing a single row per stream
        //  >> aggregate(resolve_conflicts_udf(), group_by(stream_cid))
        //  // Write these final rows to the stream_states table and return them as a batch from
        //  // this function.
        //  >> write_to(stream_states)
        //
        //  See process_conclusion_event_batch() as an example of how to build up these data flow
        //  pipelines.
        todo!()
    }
}

async fn aggregator_subscription(
    mut shutdown: ShutdownSignal,
    aggregator: AggregatorHandle,
    resolver: ResolverHandle,
    offset: Option<u64>,
) -> Result<()> {
    debug!(?offset, "starting aggregator subscription");
    let mut rx = aggregator
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
                        if let Err(err) = resolver.send(NewEventStatesMsg { events: batch }).await? {
                            error!(?err, "aggregator subscription loop failed to process new event states");
                        }
                    }
                    // Subscription has finished, this means we are shutting down.
                    Ok(None) => { break },
                    Err(err) => {
                        error!(%err, "aggregator subscription loop failed to retrieve new event states.");
                    }
                }
            }
        }
    }
    Ok(())
}

actor_envelope! {
    ResolverEnvelope,
    ResolverActor,
    ResolverRecorder,
    // Subscribe to the stream_states table, we do not yet expose a subscription to stream_tips
    SubscribeSince => SubscribeSinceMsg,
    NewEventStates => NewEventStatesMsg,
    StreamState => StreamStateMsg,
}

#[async_trait]
impl Handler<SubscribeSinceMsg> for Resolver {
    async fn handle(
        &mut self,
        message: SubscribeSinceMsg,
    ) -> <SubscribeSinceMsg as Message>::Result {
        let subscription = self.broadcast_tx.subscribe();
        let ctx = self.ctx.clone();
        rows_since(
            schemas::stream_states(),
            "stream_state_order",
            message.projection,
            message.offset,
            message.limit,
            Box::pin(RecordBatchStreamAdapter::new(
                schemas::stream_states(),
                tokio_stream::wrappers::BroadcastStream::new(subscription)
                    .map_err(|err| exec_datafusion_err!("{err}")),
            )),
            // Future Optimization can be to send the projection and limit into the events_since call.
            stream_states_since(&ctx, message.offset).await?,
        )
    }
}

async fn stream_states_since(
    ctx: &SessionContext,
    offset: Option<u64>,
) -> Result<SendableRecordBatchStream> {
    let mut stream_states = ctx
        .table(STREAM_STATES_TABLE)
        .await?
        .select(vec![wildcard()])?
        // Do not return the partition columns
        .drop_columns(&["stream_cid_partition"])?;
    if let Some(offset) = offset {
        stream_states = stream_states.filter(col("stream_state_order").gt(lit(offset)))?;
    }
    Ok(stream_states
        .sort(vec![col("stream_state_order").sort(true, true)])?
        .execute_stream()
        .await?)
}

/// Inform the resolver about new event states.
#[derive(Debug)]
pub struct NewEventStatesMsg {
    events: RecordBatch,
}
impl Message for NewEventStatesMsg {
    type Result = anyhow::Result<()>;
}

#[async_trait]
impl Handler<NewEventStatesMsg> for Resolver {
    async fn handle(
        &mut self,
        message: NewEventStatesMsg,
    ) -> <NewEventStatesMsg as Message>::Result {
        debug!(event_count = message.events.num_rows(), "new event states");
        let batch = self.process_event_states_batch(message.events).await?;
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
impl Handler<StreamStateMsg> for Resolver {
    #[instrument(skip(self), ret, err)]
    async fn handle(&mut self, message: StreamStateMsg) -> <StreamStateMsg as Message>::Result {
        let id = message.id;
        let state_batch = self
            .ctx
            .table(STREAM_STATES_TABLE)
            .await
            .context("table not found {STREAM_STATES_TABLE}")?
            .select(vec![
                col("stream_state_order"),
                col("stream_cid"),
                col("event_cid"),
                col("dimensions"),
                col("controller"),
                col("data"),
            ])
            .context("invalid select")?
            .filter(col("stream_cid").eq(lit(id.cid.to_bytes())))
            .context("invalid filter")?
            .aggregate(
                vec![col("stream_cid"), col("controller")],
                vec![
                    last_value(vec![col("data")])
                        .order_by(vec![col("stream_state_order").sort(true, true)])
                        .build()
                        .context("invalid last_value data query")?
                        .alias("data"),
                    last_value(vec![col("event_cid")])
                        .order_by(vec![col("stream_state_order").sort(true, true)])
                        .build()
                        .context("invalid last_value event_cid query")?
                        .alias("event_cid"),
                    last_value(vec![col("dimensions")])
                        .order_by(vec![col("stream_state_order").sort(true, true)])
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
impl FeedTableSource for ResolverHandle {
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
