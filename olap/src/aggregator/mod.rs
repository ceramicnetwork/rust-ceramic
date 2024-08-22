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
            array_element(col("previous"), Expr::Literal(ScalarValue::Int8(Some(0))))
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
    use super::*;

    use arrow::array::RecordBatch;
    use datafusion::{
        common::Constraints,
        logical_expr::{CreateMemoryTable, DdlStatement, EmptyRelation, LogicalPlan},
    };
    use test_log::test;

    #[test(tokio::test)]
    async fn single_init_event() -> anyhow::Result<()> {
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
        let batch = RecordBatch::new_empty(Arc::new(
            SchemaBuilder::from(&Fields::from([
                Arc::new(Field::new("stream_cid", DataType::Binary, false)),
                Arc::new(Field::new("event_cid", DataType::Binary, false)),
                Arc::new(Field::new("previous", DataType::Binary, false)),
                Arc::new(Field::new("data", DataType::Utf8, false)),
            ]))
            .finish(),
        ));
        let conclusion_feed = ctx.read_batch(batch)?;
        process_feed_batch(ctx.clone(), conclusion_feed).await?;
        ctx.table("doc_state").await?.show().await?;
        Ok(())
    }
}
