//! Aggregation functions for Ceramic Model Instance Document streams.
//!
//! Applies each new event to the previous state of the stream producing the stream state at each
//! event in the stream.
mod ceramic_patch;

use std::{any::Any, future::Future, sync::Arc};

use anyhow::{Context as _, Result};
use arrow::datatypes::{DataType, Field, Fields, SchemaBuilder};
use arrow_flight::sql::client::FlightSqlServiceClient;
use ceramic_patch::CeramicPatch;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider},
    common::JoinType,
    dataframe::{DataFrame, DataFrameWriteOptions},
    datasource::{file_format::parquet::ParquetFormat, listing::ListingOptions},
    error::DataFusionError,
    execution::context::SessionContext,
    functions_array::extract::array_element,
    logical_expr::{
        col, expr::WindowFunction, lit, Cast, Expr, ExprFunctionExt as _, WindowFunctionDefinition,
    },
    sql::TableReference,
};
use datafusion_federation::sql::{SQLFederationProvider, SQLSchemaProvider};
use datafusion_flight_sql_table_provider::FlightSQLExecutor;
use object_store::aws::AmazonS3Builder;
use tonic::transport::Endpoint;
use url::Url;

pub struct Config {
    pub flight_sql_endpoint: String,
    pub aws_bucket: String,
}

pub async fn run(
    config: impl Into<Config>,
    shutdown_signal: impl Future<Output = ()>,
) -> Result<()> {
    let config = config.into();

    // Create federated datafusion state
    let state = datafusion_federation::default_session_state();
    let client = new_client(config.flight_sql_endpoint.clone()).await?;
    let executor = Arc::new(FlightSQLExecutor::new(config.flight_sql_endpoint, client));
    let provider = Arc::new(SQLFederationProvider::new(executor));
    let schema_provider = Arc::new(
        SQLSchemaProvider::new_with_tables(provider, vec!["conclusion_feed".to_string()]).await?,
    );

    // Create datafusion context
    let ctx = SessionContext::new_with_state(state);

    // Register s3 object store
    let s3 = AmazonS3Builder::from_env()
        .with_bucket_name(&config.aws_bucket)
        .build()?;
    let mut url = Url::parse("s3://")?;
    url.set_host(Some(&config.aws_bucket))?;
    ctx.register_object_store(&url, Arc::new(s3));

    // Register federated catalog
    ctx.register_catalog("ceramic", Arc::new(SQLCatalog { schema_provider }));

    // Configure doc_state listing table
    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

    // Set the path within the bucket for the doc_state table
    url.set_path("/ceramic/v0/doc_state/");
    ctx.register_listing_table(
        "doc_state",
        url.to_string(),
        listing_options,
        Some(Arc::new(
            SchemaBuilder::from(&Fields::from([
                Arc::new(Field::new("index", DataType::UInt64, false)),
                Arc::new(Field::new("stream_cid", DataType::Binary, false)),
                Arc::new(Field::new("event_type", DataType::UInt8, false)),
                Arc::new(Field::new("controller", DataType::Utf8, false)),
                Arc::new(Field::new(
                    "dimensions",
                    DataType::Map(
                        Field::new(
                            "entries",
                            DataType::Struct(
                                vec![
                                    Field::new("key", DataType::Utf8, false),
                                    Field::new(
                                        "value",
                                        DataType::Dictionary(
                                            Box::new(DataType::Int32),
                                            Box::new(DataType::Binary),
                                        ),
                                        true,
                                    ),
                                ]
                                .into(),
                            ),
                            false,
                        )
                        .into(),
                        false,
                    ),
                    true,
                )),
                Arc::new(Field::new("event_cid", DataType::Binary, false)),
                Arc::new(Field::new("state", DataType::Utf8, true)),
            ]))
            .finish(),
        )),
        None,
    )
    .await?;

    let conclusion_feed = ctx
        .table(TableReference::full("ceramic", "v0", "conclusion_feed"))
        .await?;

    // TODO call this in a loop, see AES-291
    process_feed_batch(ctx, conclusion_feed).await?;

    shutdown_signal.await;
    Ok(())
}

/// Creates a new [FlightSqlServiceClient] for the passed endpoint. Completes the relevant auth configurations
/// or handshake as appropriate for the passed [FlightSQLAuth] variant.
async fn new_client(dsn: String) -> Result<FlightSqlServiceClient<tonic::transport::Channel>> {
    let endpoint = Endpoint::new(dsn).map_err(tx_error_to_df)?;
    let channel = endpoint.connect().await.map_err(tx_error_to_df)?;
    Ok(FlightSqlServiceClient::new(channel))
}

fn tx_error_to_df(err: tonic::transport::Error) -> DataFusionError {
    DataFusionError::External(format!("failed to connect: {err:?}").into())
}

// Process events from the conclusion feed, producing a new document state for each input event.
// The session context must have a registered a `doc_state` table with stream_cid, event_cid, and
// state columns.
//
// The events in the conclusion feed must:
//  * have stream_cid, event_cid, previous, and data columns,
//  * have previous CIDs that either already exist in `doc_state` or be contained within the
//  current conclusion_feed batch,
//  * be valid JSON patch data documents.
async fn process_feed_batch(ctx: SessionContext, conclusion_feed: DataFrame) -> Result<()> {
    let doc_state = ctx
        .table("doc_state")
        .await?
        .select_columns(&["stream_cid", "event_cid", "state"])
        .context("reading doc_state")?;

    conclusion_feed
        // MID only ever use the first previous, so we can optimize the join by selecting the
        // first element of the previous array.
        .select(vec![
            col("index"),
            col("event_type"),
            col("stream_cid"),
            col("controller"),
            col("dimensions"),
            col("event_cid"),
            Expr::Cast(Cast::new(Box::new(col("data")), DataType::Utf8)).alias("data"),
            array_element(col("previous"), lit(1)).alias("previous"),
        ])?
        .join_on(
            doc_state,
            JoinType::Left,
            [col("previous").eq(col("doc_state.event_cid"))],
        )?
        // Project joined columns to just the ones we need
        .select(vec![
            col("conclusion_feed.index").alias("index"),
            col("conclusion_feed.event_type").alias("event_type"),
            col("conclusion_feed.stream_cid").alias("stream_cid"),
            col("conclusion_feed.controller").alias("controller"),
            col("conclusion_feed.dimensions").alias("dimensions"),
            col("conclusion_feed.event_cid").alias("event_cid"),
            col("previous"),
            col("doc_state.state").alias("previous_state"),
            col("data"),
        ])?
        .window(vec![Expr::WindowFunction(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(Arc::new(CeramicPatch::new_udwf())),
            vec![
                col("event_cid"),
                col("previous"),
                col("previous_state"),
                col("data"),
            ],
        ))
        .partition_by(vec![col("stream_cid")])
        .order_by(vec![col("index").sort(true, true)])
        .build()?
        .alias("new_state")])?
        // Rename columns to match doc_state table schema
        .select_columns(&[
            "index",
            "stream_cid",
            "event_type",
            "controller",
            "dimensions",
            "event_cid",
            "new_state",
        ])?
        .with_column_renamed("new_state", "state")?
        // Write states to the doc_state table
        .write_table("doc_state", DataFrameWriteOptions::new())
        .await
        .context("computing states")?;
    Ok(())
}

struct SQLCatalog {
    schema_provider: Arc<SQLSchemaProvider>,
}

impl CatalogProvider for SQLCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["v0".to_string()]
    }

    fn schema(&self, _name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(self.schema_provider.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr as _;

    use super::*;

    use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
    use ceramic_arrow_test::CidString;
    use ceramic_core::StreamIdType;
    use ceramic_flight::{
        conclusion_events_to_record_batch, ConclusionData, ConclusionEvent, ConclusionInit,
        ConclusionTime,
    };
    use cid::Cid;
    use datafusion::{
        common::Constraints,
        logical_expr::{
            expr::ScalarFunction, CreateMemoryTable, DdlStatement, EmptyRelation, LogicalPlan,
            ScalarUDF,
        },
    };
    use expect_test::expect;
    use test_log::test;

    async fn do_test(conclusion_feed: RecordBatch) -> anyhow::Result<impl std::fmt::Display> {
        do_pass(init_ctx().await?, conclusion_feed).await
    }
    async fn init_ctx() -> anyhow::Result<SessionContext> {
        let ctx = SessionContext::new();
        ctx.execute_logical_plan(LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(
            CreateMemoryTable {
                name: "doc_state".into(),
                constraints: Constraints::empty(),
                input: LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(
                        SchemaBuilder::from(&Fields::from([
                            Arc::new(Field::new("index", DataType::UInt64, false)),
                            Arc::new(Field::new("stream_cid", DataType::Binary, false)),
                            Arc::new(Field::new("event_type", DataType::UInt8, false)),
                            Arc::new(Field::new("controller", DataType::Utf8, false)),
                            Arc::new(Field::new(
                                "dimensions",
                                DataType::Map(
                                    Field::new(
                                        "entries",
                                        DataType::Struct(
                                            vec![
                                                Field::new("key", DataType::Utf8, false),
                                                Field::new(
                                                    "value",
                                                    DataType::Dictionary(
                                                        Box::new(DataType::Int32),
                                                        Box::new(DataType::Binary),
                                                    ),
                                                    true,
                                                ),
                                            ]
                                            .into(),
                                        ),
                                        false,
                                    )
                                    .into(),
                                    false,
                                ),
                                true,
                            )),
                            Arc::new(Field::new("event_cid", DataType::Binary, false)),
                            Arc::new(Field::new("state", DataType::Utf8, false)),
                        ]))
                        .finish()
                        .try_into()
                        .unwrap(),
                    ),
                })
                .into(),
                if_not_exists: false,
                or_replace: false,
                column_defaults: vec![],
            },
        )))
        .await?;
        Ok(ctx)
    }
    async fn do_pass(
        ctx: SessionContext,
        conclusion_feed: RecordBatch,
    ) -> anyhow::Result<impl std::fmt::Display> {
        let conclusion_feed = ctx.read_batch(conclusion_feed)?;
        process_feed_batch(ctx.clone(), conclusion_feed).await?;
        let cid_string = Arc::new(ScalarUDF::from(CidString::new()));
        let doc_state = ctx
            .table("doc_state")
            .await?
            .select(vec![
                col("index"),
                Expr::ScalarFunction(ScalarFunction::new_udf(
                    cid_string.clone(),
                    vec![col("stream_cid")],
                ))
                .alias("stream_cid"),
                col("event_type"),
                col("controller"),
                col("dimensions"),
                Expr::ScalarFunction(ScalarFunction::new_udf(cid_string, vec![col("event_cid")]))
                    .alias("event_cid"),
                col("state"),
            ])?
            .collect()
            .await?;
        Ok(pretty_format_batches(&doc_state)?)
    }

    #[test(tokio::test)]
    async fn single_init_event() -> anyhow::Result<()> {
        let doc_state = do_test(conclusion_events_to_record_batch(&[
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
                data: r#"{"a":0}"#.into(),
            }),
        ])?)
        .await?;
        expect![[r#"
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | index | stream_cid                                                  | event_type | controller  | dimensions                                              | event_cid                                                   | state   |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 0          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
        Ok(())
    }
    #[test(tokio::test)]
    async fn multiple_data_events() -> anyhow::Result<()> {
        let doc_state = do_test(conclusion_events_to_record_batch(&[
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
                data: r#"{"a":0}"#.into(),
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
                data: r#"[{"op":"replace", "path": "/a", "value":1}]"#.into(),
            }),
        ])?)
        .await?;
        expect![[r#"
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | index | stream_cid                                                  | event_type | controller  | dimensions                                              | event_cid                                                   | state   |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 0          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 0          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | {"a":1} |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
        Ok(())
    }
    #[test(tokio::test)]
    async fn multiple_data_and_time_events() -> anyhow::Result<()> {
        let doc_state = do_test(conclusion_events_to_record_batch(&[
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
                data: r#"{"a":0}"#.into(),
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
                data: r#"[{"op":"replace", "path": "/a", "value":1}]"#.into(),
            }),
        ])?)
        .await?;
        expect![[r#"
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | index | stream_cid                                                  | event_type | controller  | dimensions                                              | event_cid                                                   | state   |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 0          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 1          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | {"a":0} |
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 0          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | {"a":1} |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
        Ok(())
    }

    #[test(tokio::test)]
    async fn multiple_single_event_passes() -> anyhow::Result<()> {
        // Test multiple passes where a single event for the stream is present in the conclusion
        // feed for each pass.
        let ctx = init_ctx().await?;
        let doc_state = do_pass(
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
                data: r#"{"a":0}"#.into(),
            })])?,
        )
        .await?;
        expect![[r#"
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | index | stream_cid                                                  | event_type | controller  | dimensions                                              | event_cid                                                   | state   |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 0          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
        let doc_state = do_pass(
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
        )
        .await?;
        expect![[r#"
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | index | stream_cid                                                  | event_type | controller  | dimensions                                              | event_cid                                                   | state   |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 0          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 1          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | {"a":0} |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
        let doc_state = do_pass(
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
                data: r#"[{"op":"replace", "path": "/a", "value":1}]"#.into(),
            })])?,
        )
        .await?;
        expect![[r#"
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | index | stream_cid                                                  | event_type | controller  | dimensions                                              | event_cid                                                   | state   |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 0          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 1          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | {"a":0} |
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 0          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | {"a":1} |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
        Ok(())
    }
    #[test(tokio::test)]
    async fn multiple_passes() -> anyhow::Result<()> {
        let ctx = init_ctx().await?;
        let doc_state = do_pass(
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
                data: r#"{"a":0}"#.into(),
            })])?,
        )
        .await?;
        expect![[r#"
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | index | stream_cid                                                  | event_type | controller  | dimensions                                              | event_cid                                                   | state   |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 0          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
        let doc_state = do_pass(
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
                    data: r#"[{"op":"replace", "path": "/a", "value":1}]"#.into(),
                }),
            ])?,
        )
        .await?;
        expect![[r#"
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | index | stream_cid                                                  | event_type | controller  | dimensions                                              | event_cid                                                   | state   |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+
            | 0     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 0          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            | 1     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 1          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | {"a":0} |
            | 2     | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | 0          | did:key:bob | {controller: 6469643a6b65793a626f62, model: 6d6f64656c} | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | {"a":1} |
            +-------+-------------------------------------------------------------+------------+-------------+---------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
        Ok(())
    }
}
