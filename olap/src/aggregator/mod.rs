mod ceramic_car;
mod ceramic_patch;

use std::{future::Future, sync::Arc};

use anyhow::{Context as _, Result};
use arrow::datatypes::{DataType, Field, Fields, SchemaBuilder};
use ceramic_car::CeramicCar;
use ceramic_patch::CeramicPatch;
use datafusion::{
    common::{Column, JoinType},
    dataframe::{DataFrame, DataFrameWriteOptions},
    datasource::{file_format::parquet::ParquetFormat, listing::ListingOptions},
    execution::{context::SessionContext, options::ParquetReadOptions},
    functions_array::extract::array_element,
    logical_expr::{
        col,
        expr::{ScalarFunction, WindowFunction},
        Expr, ExprFunctionExt as _, WindowFunctionDefinition,
    },
    scalar::ScalarValue,
    sql::TableReference,
};
use tracing::debug;

pub async fn run(_shutdown_signal: impl Future<Output = ()>) -> Result<()> {
    // Create datafusion context
    let ctx = SessionContext::new();

    // Configure doc_state listing table
    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options =
        ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

    ctx.register_listing_table(
        "doc_state",
        "./db/doc_state",
        listing_options,
        Some(Arc::new(
            SchemaBuilder::from(&Fields::from([
                Arc::new(Field::new("stream_cid", DataType::Binary, false)),
                Arc::new(Field::new("event_cid", DataType::Binary, false)),
                Arc::new(Field::new("state", DataType::Utf8, false)),
            ]))
            .finish(),
        )),
        None,
    )
    .await
    .unwrap();

    // Hack in conclusion feed
    // TODO replace with federated Flight SQL call
    let conclusion_feed = ctx
        .read_parquet("db/events.parquet", ParquetReadOptions::default())
        .await?
        .select(vec![Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(CeramicCar::new().into()),
            vec![col("car")],
        ))
        .alias("commit")])?
        .unnest_columns(&["commit"])?
        .with_column_renamed("commit.stream_cid", "stream_cid")?
        .with_column_renamed("commit.event_cid", "event_cid")?
        .with_column_renamed("commit.previous", "previous")?
        .with_column_renamed("commit.data", "data")?;

    debug!("cache");
    let conclusion_feed = conclusion_feed
        .cache()
        .await
        .context("computing conclusion feed")?;

    debug!("write");
    conclusion_feed
        .clone()
        .write_parquet(
            "db/conclusion_feed.parquet",
            DataFrameWriteOptions::new(),
            None,
        )
        .await
        .context("writing conclusion feed")?;

    debug!("process_feed_batch");
    // TODO call this in a loop
    process_feed_batch(ctx, conclusion_feed).await?;

    //shutdown_signal.await;
    Ok(())
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
    let doc_state =
        ctx.table("doc_state")
            .await?
            .select_columns(&["stream_cid", "event_cid", "state"])?;

    conclusion_feed
        // MID only ever use the first previous, so we can optimize the join by selecting the
        // first element of the previous array.
        .select(vec![
            col("stream_cid"),
            col("event_cid"),
            array_element(col("previous"), Expr::Literal(ScalarValue::Int8(Some(1))))
                .alias("previous"),
            col("data"),
        ])?
        .join_on(
            doc_state,
            JoinType::Left,
            [col("previous").eq(col("doc_state.event_cid"))],
        )?
        // Project joined columns to just the ones we need
        .select(vec![
            col(Column {
                relation: Some(TableReference::from("?table?")),
                name: "stream_cid".to_string(),
            })
            .alias("stream_cid"),
            col(Column {
                relation: Some(TableReference::from("?table?")),
                name: "event_cid".to_string(),
            })
            .alias("event_cid"),
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
        //todo order_by event_height
        .build()?
        .alias("new_state")])?
        // Rename columns to match doc_state table schema
        .select_columns(&["stream_cid", "event_cid", "new_state"])?
        .with_column_renamed("new_state", "state")?
        // Write states to the doc_state table
        .write_table("doc_state", DataFrameWriteOptions::new())
        .await
        .context("computing states")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{any::Any, str::FromStr as _};

    use super::*;

    use arrow::{
        array::{RecordBatch, StringBuilder},
        util::pretty::pretty_format_batches,
    };
    use ceramic_flight::{
        conclusion_events_to_record_batch, ConclusionData, ConclusionEvent, ConclusionInit,
        StreamType, TimeEvent,
    };
    use cid::Cid;
    use datafusion::{
        common::{cast::as_binary_array, exec_datafusion_err, Constraints},
        logical_expr::{
            ColumnarValue, CreateMemoryTable, DdlStatement, EmptyRelation, LogicalPlan, ScalarUDF,
            ScalarUDFImpl, Signature, TypeSignature, Volatility,
        },
    };
    use expect_test::expect;
    use test_log::test;

    #[derive(Debug)]
    pub struct CidString {
        signature: Signature,
    }

    impl CidString {
        pub fn new() -> Self {
            Self {
                signature: Signature::new(
                    TypeSignature::Exact(vec![DataType::Binary]),
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for CidString {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn name(&self) -> &str {
            "cid_string"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _args: &[DataType]) -> datafusion::common::Result<DataType> {
            Ok(DataType::Utf8)
        }
        fn invoke(&self, args: &[ColumnarValue]) -> datafusion::common::Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(args)?;
            let cids = as_binary_array(&args[0])?;
            let mut strs = StringBuilder::new();
            for cid in cids {
                if let Some(cid) = cid {
                    strs.append_value(
                        Cid::read_bytes(cid)
                            .map_err(|err| exec_datafusion_err!("Error {err}"))?
                            .to_string(),
                    );
                } else {
                    strs.append_null()
                }
            }
            Ok(ColumnarValue::Array(Arc::new(strs.finish())))
        }
    }
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
                            Arc::new(Field::new("stream_cid", DataType::Binary, false)),
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
                Expr::ScalarFunction(ScalarFunction::new_udf(
                    cid_string.clone(),
                    vec![col("stream_cid")],
                ))
                .alias("stream_cid"),
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
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )?,
                event_cid: Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?,
                init: ConclusionInit {
                    stream_type: StreamType::ModelInstanceDocument,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![],
                },
                previous: vec![],
                data: r#"{"a":0}"#.to_string(),
            }),
        ])?)
        .await?;
        expect![[r#"
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | stream_cid                                                  | event_cid                                                   | state   |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
        Ok(())
    }
    #[test(tokio::test)]
    async fn multiple_data_events() -> anyhow::Result<()> {
        let doc_state = do_test(conclusion_events_to_record_batch(&[
            ConclusionEvent::Data(ConclusionData {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )?,
                event_cid: Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?,
                init: ConclusionInit {
                    stream_type: StreamType::ModelInstanceDocument,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![],
                },
                previous: vec![],
                data: r#"{"a":0}"#.to_string(),
            }),
            ConclusionEvent::Data(ConclusionData {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )?,
                event_cid: Cid::from_str(
                    "baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du",
                )?,
                init: ConclusionInit {
                    stream_type: StreamType::ModelInstanceDocument,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![],
                },
                previous: vec![Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?],
                data: r#"[{"op":"replace", "path": "/a", "value":1}]"#.to_string(),
            }),
        ])?)
        .await?;
        expect![[r#"
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | stream_cid                                                  | event_cid                                                   | state   |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | {"a":1} |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
        Ok(())
    }
    #[test(tokio::test)]
    async fn multiple_data_and_time_events() -> anyhow::Result<()> {
        let doc_state = do_test(conclusion_events_to_record_batch(&[
            ConclusionEvent::Data(ConclusionData {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )?,
                event_cid: Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?,
                init: ConclusionInit {
                    stream_type: StreamType::ModelInstanceDocument,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![],
                },
                previous: vec![],
                data: r#"{"a":0}"#.to_string(),
            }),
            ConclusionEvent::Time(TimeEvent {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )?,
                event_cid: Cid::from_str(
                    "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                )?,
                init: ConclusionInit {
                    stream_type: StreamType::ModelInstanceDocument,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![],
                },
                previous: vec![Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?],
            }),
            ConclusionEvent::Data(ConclusionData {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )?,
                event_cid: Cid::from_str(
                    "baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du",
                )?,
                init: ConclusionInit {
                    stream_type: StreamType::ModelInstanceDocument,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![],
                },
                previous: vec![Cid::from_str(
                    "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                )?],
                data: r#"[{"op":"replace", "path": "/a", "value":1}]"#.to_string(),
            }),
        ])?)
        .await?;
        expect![[r#"
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | stream_cid                                                  | event_cid                                                   | state   |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | {"a":0} |
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | {"a":1} |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
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
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )?,
                event_cid: Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?,
                init: ConclusionInit {
                    stream_type: StreamType::ModelInstanceDocument,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![],
                },
                previous: vec![],
                data: r#"{"a":0}"#.to_string(),
            })])?,
        )
        .await?;
        expect![[r#"
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | stream_cid                                                  | event_cid                                                   | state   |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
        let doc_state = do_pass(
            ctx.clone(),
            conclusion_events_to_record_batch(&[ConclusionEvent::Time(TimeEvent {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )?,
                event_cid: Cid::from_str(
                    "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                )?,
                init: ConclusionInit {
                    stream_type: StreamType::ModelInstanceDocument,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![],
                },
                previous: vec![Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?],
            })])?,
        )
        .await?;
        expect![[r#"
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | stream_cid                                                  | event_cid                                                   | state   |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | {"a":0} |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
        let doc_state = do_pass(
            ctx,
            conclusion_events_to_record_batch(&[ConclusionEvent::Data(ConclusionData {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )?,
                event_cid: Cid::from_str(
                    "baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du",
                )?,
                init: ConclusionInit {
                    stream_type: StreamType::ModelInstanceDocument,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![],
                },
                previous: vec![Cid::from_str(
                    "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                )?],
                data: r#"[{"op":"replace", "path": "/a", "value":1}]"#.to_string(),
            })])?,
        )
        .await?;
        expect![[r#"
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | stream_cid                                                  | event_cid                                                   | state   |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | {"a":0} |
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | {"a":1} |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
        Ok(())
    }
    #[test(tokio::test)]
    async fn multiple_passes() -> anyhow::Result<()> {
        let ctx = init_ctx().await?;
        let doc_state = do_pass(
            ctx.clone(),
            conclusion_events_to_record_batch(&[ConclusionEvent::Data(ConclusionData {
                stream_cid: Cid::from_str(
                    "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                )?,
                event_cid: Cid::from_str(
                    "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                )?,
                init: ConclusionInit {
                    stream_type: StreamType::ModelInstanceDocument,
                    controller: "did:key:bob".to_string(),
                    dimensions: vec![],
                },
                previous: vec![],
                data: r#"{"a":0}"#.to_string(),
            })])?,
        )
        .await?;
        expect![[r#"
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | stream_cid                                                  | event_cid                                                   | state   |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
        let doc_state = do_pass(
            ctx.clone(),
            conclusion_events_to_record_batch(&[
                ConclusionEvent::Time(TimeEvent {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )?,
                    event_cid: Cid::from_str(
                        "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                    )?,
                    init: ConclusionInit {
                        stream_type: StreamType::ModelInstanceDocument,
                        controller: "did:key:bob".to_string(),
                        dimensions: vec![],
                    },
                    previous: vec![Cid::from_str(
                        "baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi",
                    )?],
                }),
                ConclusionEvent::Data(ConclusionData {
                    stream_cid: Cid::from_str(
                        "baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu",
                    )?,
                    event_cid: Cid::from_str(
                        "baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du",
                    )?,
                    init: ConclusionInit {
                        stream_type: StreamType::ModelInstanceDocument,
                        controller: "did:key:bob".to_string(),
                        dimensions: vec![],
                    },
                    previous: vec![Cid::from_str(
                        "baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq",
                    )?],
                    data: r#"[{"op":"replace", "path": "/a", "value":1}]"#.to_string(),
                }),
            ])?,
        )
        .await?;
        expect![[r#"
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | stream_cid                                                  | event_cid                                                   | state   |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeials2i6o2ppkj55kfbh7r2fzc73r2esohqfivekpag553lyc7f6bi | {"a":0} |
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeihyzbu2wxx4yj37mozb76gkxln2dt5zxxasivhuzbnxiqd5w4xygq | {"a":0} |
            | baeabeif2fdfqe2hu6ugmvgozkk3bbp5cqi4udp5rerjmz4pdgbzf3fvobu | baeabeifwi4ddwafoqe6htkx3g5gtjz5adapj366w6mraut4imk2ljwu3du | {"a":1} |
            +-------------------------------------------------------------+-------------------------------------------------------------+---------+"#]].assert_eq(&doc_state.to_string());
        Ok(())
    }
}
