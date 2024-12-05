//! Aggregation functions for Ceramic Model Instance Document streams.
//!
//! Applies each new event to the previous state of the stream producing the stream state at each
//! event in the stream.
mod ceramic_patch;

use anyhow::Context;
use arrow::array::{RecordBatch, UInt64Array};
use ceramic_patch::CeramicPatch;
use datafusion::{
    common::JoinType,
    dataframe::{DataFrame, DataFrameWriteOptions},
    datasource::{provider_as_source, MemTable},
    execution::context::SessionContext,
    functions_aggregate::min_max::max,
    functions_array::extract::array_element,
    logical_expr::{
        col, expr::WindowFunction, lit, Expr, ExprFunctionExt as _, LogicalPlanBuilder,
        WindowFunctionDefinition,
    },
    physical_plan::collect_partitioned,
};
use std::{future::Future, sync::Arc};
use tracing::{debug, error, instrument, Level};

use crate::{
    schemas, Result, CONCLUSION_EVENTS_TABLE, EVENT_STATES_MEM_TABLE,
    EVENT_STATES_PERSISTENT_TABLE, EVENT_STATES_TABLE,
};

// Maximum number of rows to fetch per pass of the aggregator.
// Minimum number of rows to have processed before writing a batch to object store.
const BATCH_SIZE: usize = 10_000;

pub async fn run(ctx: SessionContext, shutdown_signal: impl Future<Output = ()>) -> Result<()> {
    run_continuous_stream(ctx, shutdown_signal, BATCH_SIZE).await?;
    Ok(())
}

async fn run_continuous_stream(
    ctx: SessionContext,
    shutdown_signal: impl Future<Output = ()>,
    limit: usize,
) -> Result<()> {
    let mut processor = ContinuousStreamProcessor::new(ctx).await?;
    let mut shutdown_signal = Box::pin(shutdown_signal);

    loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                debug!("Received shutdown signal, stopping continuous stream processing");
                break;
            }
            result = processor.process_batch(limit) => {
                match result {
                    Ok(()) => {
                        // Batch processed successfully, continue to next iteration
                        continue;
                    }
                    Err(err) => {
                        error!(%err, "error processing batch");
                        return Err(err);
                    }
                }
            }
        }
    }
    Ok(())
}

/// Represents a processor for continuous stream processing of conclusion event data.
struct ContinuousStreamProcessor {
    ctx: SessionContext,
    last_processed_index: Option<u64>,
}

impl ContinuousStreamProcessor {
    async fn new(ctx: SessionContext) -> Result<Self> {
        let max_index = ctx
            .table(EVENT_STATES_TABLE)
            .await?
            .select_columns(&["index"])?
            .aggregate(vec![], vec![max(col("index"))])?
            .collect()
            .await?;

        if max_index.is_empty() {
            return Ok(Self {
                ctx,
                last_processed_index: None,
            });
        }

        Ok(Self {
            ctx,
            last_processed_index: max_index
                .first()
                .and_then(|batch| batch.column(0).as_any().downcast_ref::<UInt64Array>())
                .and_then(|index| index.iter().next().flatten()),
        })
    }

    #[instrument(skip(self), ret ( level = Level::DEBUG ))]
    async fn process_batch(&mut self, limit: usize) -> Result<()> {
        // Fetch the conclusion events DataFrame
        let mut conclusion_events = self.ctx.table(CONCLUSION_EVENTS_TABLE).await?.select(vec![
            col("index"),
            col("stream_cid"),
            col("stream_type"),
            col("controller"),
            col("dimensions"),
            col("event_cid"),
            col("event_type"),
            col("data"),
            col("previous"),
        ])?;
        if let Some(last_index) = self.last_processed_index {
            conclusion_events = conclusion_events.filter(col("index").gt(lit(last_index)))?;
        }
        let batch = conclusion_events.limit(0, Some(limit))?;

        // Caching the data frame to use it to calculate the max index
        // We need to cache it because we do 2 passes over the data frame, once for process_conclusion_events_batch and once for calculating the max index
        // We are not using batch.cache() because this loses table name information
        let batch_plan = batch.clone().create_physical_plan().await?;
        let task_ctx = Arc::new(batch.task_ctx());
        let partitions = collect_partitioned(batch_plan.clone(), task_ctx).await?;
        let cached_memtable = MemTable::try_new(batch_plan.schema(), partitions)?;
        let df = DataFrame::new(
            self.ctx.state(),
            LogicalPlanBuilder::scan(
                CONCLUSION_EVENTS_TABLE,
                provider_as_source(Arc::new(cached_memtable)),
                None,
            )?
            .build()?,
        );
        process_conclusion_events_batch(self.ctx.clone(), df.clone(), limit).await?;

        // Fetch the highest index from the cached DataFrame
        let highest_index = df
            .select_columns(&["index"])?
            .aggregate(vec![], vec![max(col("index"))])?
            .collect()
            .await?;

        if let Some(batch) = highest_index.first() {
            if let Some(max_index) = batch.column(0).as_any().downcast_ref::<UInt64Array>() {
                if let Some(max_value) = max_index.iter().next().flatten() {
                    self.last_processed_index = Some(max_value);
                }
            }
        }

        Ok(())
    }
}

// Process a batch of conclusion events, producing a new document state for each input event.
// The session context must have a registered a `event_states` table with appropriate schema.
//
// The events in the conclusion_events batch have a schema of the conclusion_events table.
#[instrument(skip(ctx, conclusion_events), ret ( level = Level::DEBUG ))]
async fn process_conclusion_events_batch(
    ctx: SessionContext,
    conclusion_events: DataFrame,
    max_cached_rows: usize,
) -> Result<()> {
    let event_states = ctx
        .table(EVENT_STATES_TABLE)
        .await?
        .select_columns(&["stream_cid", "event_cid", "data"])
        .context("reading event_states")?;

    conclusion_events
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
            col(CONCLUSION_EVENTS_TABLE.to_string() + ".index").alias("index"),
            col(CONCLUSION_EVENTS_TABLE.to_string() + ".event_type").alias("event_type"),
            col(CONCLUSION_EVENTS_TABLE.to_string() + ".stream_cid").alias("stream_cid"),
            col(CONCLUSION_EVENTS_TABLE.to_string() + ".stream_type").alias("stream_type"),
            col(CONCLUSION_EVENTS_TABLE.to_string() + ".controller").alias("controller"),
            col(CONCLUSION_EVENTS_TABLE.to_string() + ".dimensions").alias("dimensions"),
            col(CONCLUSION_EVENTS_TABLE.to_string() + ".event_cid").alias("event_cid"),
            col("previous"),
            col(EVENT_STATES_TABLE.to_string() + ".data").alias("previous_data"),
            col(CONCLUSION_EVENTS_TABLE.to_string() + ".data").alias("data"),
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
        // Write states to the in memory event_states table
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
                DataFrameWriteOptions::new().with_overwrite(true),
            )
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr as _, time::Duration};

    use super::*;

    use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
    use arrow_schema::DataType;
    use ceramic_core::StreamIdType;
    use cid::Cid;
    use datafusion::{
        datasource::{provider_as_source, MemTable},
        logical_expr::{cast, expr::ScalarFunction, LogicalPlanBuilder, ScalarUDF},
    };
    use expect_test::{expect, Expect};
    use object_store::memory::InMemory;
    use test_log::test;
    use tokio::sync::oneshot;

    use crate::{
        cid_string::CidString, conclusion_events_to_record_batch, schemas, session_from_config,
        tests::MockConclusionFeed, ConclusionData, ConclusionEvent, ConclusionFeedSource,
        ConclusionInit, ConclusionTime, Config, CONCLUSION_EVENTS_TABLE,
    };

    async fn do_test(conclusion_events: RecordBatch) -> anyhow::Result<impl std::fmt::Display> {
        do_pass(init_ctx().await?, conclusion_events, 1_000).await
    }

    async fn init_ctx() -> anyhow::Result<SessionContext> {
        session_from_config(Config {
            conclusion_feed: MockConclusionFeed::new().into(),
            object_store: Arc::new(InMemory::new()),
        })
        .await
    }

    async fn init_ctx_cont(conclusion_events: RecordBatch) -> anyhow::Result<SessionContext> {
        session_from_config(Config {
            conclusion_feed: ConclusionFeedSource::<MockConclusionFeed>::InMemory(
                MemTable::try_new(schemas::conclusion_events(), vec![vec![conclusion_events]])?,
            ),
            object_store: Arc::new(InMemory::new()),
        })
        .await
    }

    async fn do_pass(
        ctx: SessionContext,
        conclusion_events: RecordBatch,
        max_cached_rows: usize,
    ) -> anyhow::Result<impl std::fmt::Display> {
        // Setup conclusion_events table from RecordBatch
        let provider =
            MemTable::try_new(conclusion_events.schema(), vec![vec![conclusion_events]])?;
        let conclusion_events = DataFrame::new(
            ctx.state(),
            LogicalPlanBuilder::scan(
                CONCLUSION_EVENTS_TABLE,
                provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()?,
        );
        process_conclusion_events_batch(ctx.clone(), conclusion_events, max_cached_rows).await?;
        let cid_string = Arc::new(ScalarUDF::from(CidString::new()));
        let event_states = ctx
            .table(EVENT_STATES_TABLE)
            .await?
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

    async fn do_run_continuous(
        conclusion_events: RecordBatch,
        expected_batches: Expect,
    ) -> anyhow::Result<()> {
        let ctx = init_ctx_cont(conclusion_events).await?;
        let (shutdown_signal_tx, shutdown_signal_rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(run_continuous_stream(
            ctx.clone(),
            async move {
                let _ = shutdown_signal_rx.await;
            },
            1,
        ));
        let mut retries = 5;
        let mut batches = String::new();
        while retries > 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            retries -= 1;
            let cid_string = Arc::new(ScalarUDF::from(CidString::new()));
            let event_states = ctx
                .table(EVENT_STATES_TABLE)
                .await?
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
                    Expr::ScalarFunction(ScalarFunction::new_udf(
                        cid_string,
                        vec![col(EVENT_STATES_TABLE.to_string() + ".event_cid")],
                    ))
                    .alias("event_cid"),
                    col("event_type"),
                    cast(col("data"), DataType::Utf8).alias("data"),
                ])?
                .sort(vec![col("index").sort(true, true)])?
                .collect()
                .await?;
            batches = pretty_format_batches(&event_states)?.to_string();
            if expected_batches.data() == batches {
                break;
            }
        }
        expected_batches.assert_eq(&batches);
        let _ = shutdown_signal_tx.send(());
        handle.await?
    }

    #[tokio::test]
    async fn single_init_event() -> anyhow::Result<()> {
        let event_states = do_test(conclusion_events_to_record_batch(&[
            ConclusionEvent::Data(ConclusionData {
                index: 0,
                event_cid: Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?,
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )?,
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
        ])?)
        .await?;
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                        |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
        Ok(())
    }
    #[test(tokio::test)]
    async fn multiple_data_events() -> anyhow::Result<()> {
        let event_states = do_test(conclusion_events_to_record_batch(&[
            ConclusionEvent::Data(ConclusionData {
                index: 1,
                event_cid: Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?,
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )?,
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
                )?,
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )?,
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:bob".to_vec()),
                        ("model".to_string(), b"model".to_vec()),
                    ],
                },
                previous: vec![Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?],
                data:
                    r#"{"metadata":{"foo":2},"content":[{"op":"replace", "path": "/a", "value":1}]}"#
                        .into(),
            }),
        ])?)
        .await?;

        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                        |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}} |
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | 0          | {"metadata":{"foo":2,"shouldIndex":true},"content":{"a":1}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
        Ok(())
    }
    #[test(tokio::test)]
    async fn multiple_data_and_time_events() -> anyhow::Result<()> {
        let event_states = do_test(conclusion_events_to_record_batch(&[
            ConclusionEvent::Data(ConclusionData {
                index: 0,
                event_cid: Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?,
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )?,
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
                )?,
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )?,
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:bob".to_vec()),
                        ("model".to_string(), b"model".to_vec()),
                    ],
                },
                previous: vec![Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?],
            }),
            ConclusionEvent::Data(ConclusionData {
                index: 2,
                event_cid: Cid::from_str(
                    "baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du",
                )?,
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )?,
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:bob".to_vec()),
                        ("model".to_string(), b"model".to_vec()),
                    ],
                },
                previous: vec![Cid::from_str(
                    "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                )?],
                data: r#"{"metadata":{"shouldIndex":false},"content":[{"op":"replace", "path": "/a", "value":1}]}"#.into(),
            }),
        ])?)
        .await?;
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                         |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}  |
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}  |
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | 0          | {"metadata":{"foo":1,"shouldIndex":false},"content":{"a":1}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
        Ok(())
    }

    #[test(tokio::test)]
    async fn multiple_single_event_passes() -> anyhow::Result<()> {
        // Test multiple passes where a single event for the stream is present in the conclusion
        // events for each pass.
        let ctx = init_ctx().await?;
        let event_states = do_pass(
            ctx.clone(),
            conclusion_events_to_record_batch(&[ConclusionEvent::Data(ConclusionData {
                index: 0,
                event_cid: Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?,
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )?,
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:bob".to_vec()),
                        ("model".to_string(), b"model".to_vec()),
                    ],
                },
                previous: vec![],
                data: r#"{"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}"#.into(),
            })])?,
            1_000,
        )
        .await?;
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                        |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
        let event_states = do_pass(
            ctx.clone(),
            conclusion_events_to_record_batch(&[ConclusionEvent::Time(ConclusionTime {
                index: 1,
                event_cid: Cid::from_str(
                    "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                )?,
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )?,
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:bob".to_vec()),
                        ("model".to_string(), b"model".to_vec()),
                    ],
                },
                previous: vec![Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?],
            })])?,
            1_000,
        )
        .await?;
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                        |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}} |
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
        let event_states = do_pass(
            ctx,
            conclusion_events_to_record_batch(&[ConclusionEvent::Data(ConclusionData {
                index: 2,
                event_cid: Cid::from_str(
                    "baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du",
                )?,
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )?,
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:bob".to_vec()),
                        ("model".to_string(), b"model".to_vec()),
                    ],
                },
                previous: vec![Cid::from_str(
                    "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                )?],
                data: r#"{"metadata":{"shouldIndex":false},"content":[{"op":"replace", "path": "/a", "value":1}]}"#.into(),
            })])?,
            1_000,
        )
        .await?;
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                         |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}  |
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}  |
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | 0          | {"metadata":{"foo":1,"shouldIndex":false},"content":{"a":1}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
        Ok(())
    }
    #[test(tokio::test)]
    async fn multiple_passes() -> anyhow::Result<()> {
        let ctx = init_ctx().await?;
        let event_states = do_pass(
            ctx.clone(),
            conclusion_events_to_record_batch(&[ConclusionEvent::Data(ConclusionData {
                index: 0,
                event_cid: Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?,
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )?,
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:bob".to_vec()),
                        ("model".to_string(), b"model".to_vec()),
                    ],
                },
                previous: vec![],
                data: r#"{"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}"#.into(),
            })])?,
            1_000,
        )
        .await?;
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                        |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+-------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
        let event_states = do_pass(
            ctx.clone(),
            conclusion_events_to_record_batch(&[
                ConclusionEvent::Time(ConclusionTime {
                    index: 1,
                    event_cid: Cid::from_str(
                        "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                    )?,
                    init: ConclusionInit {
                        stream_cid: Cid::from_str(
                            "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                        )?,
                        stream_type: StreamIdType::Model as u8,
                        controller: "did:key:bob".to_string(),
                        dimensions: vec![
                            ("controller".to_string(), b"did:key:bob".to_vec()),
                            ("model".to_string(), b"model".to_vec()),
                        ],
                    },
                    previous: vec![Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )?],
                }),
                ConclusionEvent::Data(ConclusionData {
                    index: 2,
                    event_cid: Cid::from_str(
                        "baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du",
                    )?,
                    init: ConclusionInit {
                        stream_cid: Cid::from_str(
                            "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                        )?,
                        stream_type: StreamIdType::Model as u8,
                        controller: "did:key:bob".to_string(),
                        dimensions: vec![
                            ("controller".to_string(), b"did:key:bob".to_vec()),
                            ("model".to_string(), b"model".to_vec()),
                        ],
                    },
                    previous: vec![Cid::from_str(
                        "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                    )?],
                    data: r#"{"metadata":{"shouldIndex":false},"content":[{"op":"replace", "path": "/a", "value":1}]}"#.into(),
                }),
            ])?,
            1_000,
        )
        .await?;
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                         |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}  |
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | 1          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}  |
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | 0          | {"metadata":{"foo":1,"shouldIndex":false},"content":{"a":1}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+"#]].assert_eq(&event_states.to_string());
        Ok(())
    }
    #[test(tokio::test)]
    async fn multiple_data_events_continuous() -> anyhow::Result<()> {
        do_run_continuous(conclusion_events_to_record_batch(&[
            ConclusionEvent::Data(ConclusionData {
                index: 1,
                event_cid: Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?,
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )?,
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
                )?,
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )?,
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:bob".to_vec()),
                        ("model".to_string(), b"model".to_vec()),
                    ],
                },
                previous: vec![Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?],
                data: r#"{"metadata":{"shouldIndex":false},"content":[{"op":"replace", "path": "/a", "value":1}]}"#.into(),
            }),
            ConclusionEvent::Data(ConclusionData {
                index: 3,
                event_cid: Cid::from_str(
                    "baeabeic2caccyfigwadnncpyko7hcdbr66dkf4jyzeoh4dbhfebp77hchu",
                )?,
                init: ConclusionInit {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )?,
                    stream_type: StreamIdType::Model as u8,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![
                        ("controller".to_string(), b"did:key:bob".to_vec()),
                        ("model".to_string(), b"model".to_vec()),
                    ],
                },
                previous: vec![Cid::from_str(
                    "baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du",
                )?],
                data: r#"{"metadata":{},"content":[{"op":"replace", "path": "/a", "value":2}]}"#.into(),
            }),
        ])?,
        expect![[r#"
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | index | stream_cid                                                  | stream_type | controller  | dimensions                                              | event_cid                                                   | event_type | data                                                         |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | 0          | {"metadata":{"foo":1,"shouldIndex":true},"content":{"a":0}}  |
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | 0          | {"metadata":{"foo":1,"shouldIndex":false},"content":{"a":1}} |
            | 3     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 2           | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeic2caccyfigwadnncpyko7hcdbr66dkf4jyzeoh4dbhfebp77hchu | 0          | {"metadata":{"foo":1,"shouldIndex":false},"content":{"a":2}} |
            +-------+-------------------------------------------------------------+-------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+------------+--------------------------------------------------------------+"#]],
            )
        .await
    }
}
