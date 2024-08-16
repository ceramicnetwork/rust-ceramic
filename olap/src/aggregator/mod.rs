mod ceramic_car;
mod ceramic_patch;

use std::{future::Future, sync::Arc};

use anyhow::Result;
use ceramic_car::CeramicCar;
use ceramic_patch::CeramicPatch;
use datafusion::{
    common::JoinType,
    dataframe::DataFrameWriteOptions,
    execution::{context::SessionContext, options::ParquetReadOptions},
    logical_expr::{col, expr::ScalarFunction, unnest, Expr},
};

pub async fn run(shutdown_signal: impl Future<Output = ()>) -> Result<()> {
    // Create datafusion context
    let ctx = SessionContext::new();
    let conclusion_feed = ctx
        .read_parquet("db/events.parquet", ParquetReadOptions::default())
        .await?
        .select(vec![Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(CeramicCar::new().into()),
            vec![col("car")],
        ))
        .alias("commit")])?;
    conclusion_feed
        .write_parquet(
            "db/conclusion_feed.parquet",
            DataFrameWriteOptions::new(),
            None,
        )
        .await?;

    //    let conclusion_feed = ctx
    //        .read_parquet("db/conclusion_feed.parquet", ParquetReadOptions::default())
    //        .await?
    //        .select(vec![
    //            col("multi_prev"),
    //            Expr::ScalarFunction(ScalarFunction::new_udf(
    //                Arc::new(CeramicPatch::new().into()),
    //                vec![col("payload"), col("state")],
    //            )),
    //        ])?;
    //    let doc_state = ctx
    //        .read_parquet("db/doc_state/*.parquet", ParquetReadOptions::default())
    //        .await?
    //        .select(vec![col("id"), col("state")])?;
    //
    //    conclusion_feed
    //        .join_on(
    //            doc_state,
    //            JoinType::Left,
    //            [col("id").in_list(vec![col("multi_prev")], false)],
    //        )?
    //        .write_parquet(
    //            "db/doc_state/new.parquet",
    //            DataFrameWriteOptions::new().with_overwrite(true),
    //            None,
    //        )
    //        .await?;
    //
    shutdown_signal.await;
    Ok(())
}
