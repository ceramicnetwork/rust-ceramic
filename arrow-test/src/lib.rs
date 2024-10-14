//! Common utilities for testing APIs using Arrow [`RecordBatch`]es and related data structures.
#![warn(missing_docs)]

use std::sync::Arc;

use ceramic_pipeline::cid_string::{CidString, CidStringList};
use datafusion::{
    arrow::{datatypes::DataType, record_batch::RecordBatch},
    dataframe::DataFrame,
    execution::context::SessionContext,
    logical_expr::{col, expr::ScalarFunction, Cast, Expr, ScalarUDF},
};

/// Applies various transformations on a record batch of conclusion_feed data to make it easier to
/// read.
/// Useful in conjuction with expect_test.
pub async fn pretty_feed_from_batch(batch: RecordBatch) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_batch("conclusion_feed", batch).unwrap();

    pretty_feed(ctx.table("conclusion_feed").await.unwrap()).await
}

/// Applies various transformations on a dataframe of conclusion_feed data to make it easier to
/// read.
/// Useful in conjuction with expect_test.
pub async fn pretty_feed(conclusion_feed: DataFrame) -> Vec<RecordBatch> {
    let cid_string = Arc::new(ScalarUDF::from(CidString::new()));
    let cid_string_list = Arc::new(ScalarUDF::from(CidStringList::new()));
    conclusion_feed
        .select(vec![
            col("index"),
            col("event_type"),
            Expr::ScalarFunction(ScalarFunction::new_udf(
                cid_string.clone(),
                vec![col("stream_cid")],
            ))
            .alias("stream_cid"),
            col("stream_type"),
            col("controller"),
            col("dimensions"),
            Expr::ScalarFunction(ScalarFunction::new_udf(
                cid_string.clone(),
                vec![col("event_cid")],
            ))
            .alias("event_cid"),
            Expr::Cast(Cast::new(Box::new(col("data")), DataType::Utf8)).alias("data"),
            Expr::ScalarFunction(ScalarFunction::new_udf(
                cid_string_list,
                vec![col("previous")],
            ))
            .alias("previous"),
        ])
        .unwrap()
        .sort(vec![col("index").sort(true, true)])
        .unwrap()
        .collect()
        .await
        .unwrap()
}
