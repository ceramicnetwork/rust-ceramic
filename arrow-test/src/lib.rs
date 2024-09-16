//! Common utilities for testing APIs using Arrow [`RecordBatch`]es and related data structures.
#![warn(missing_docs)]

use std::{any::Any, sync::Arc};

use arrow::{array::StringBuilder, datatypes::DataType, record_batch::RecordBatch};
use cid::Cid;
use datafusion::{
    common::{cast::as_binary_array, exec_datafusion_err},
    dataframe::DataFrame,
    execution::context::SessionContext,
    functions_aggregate::expr_fn::array_agg,
    logical_expr::{
        col, expr::ScalarFunction, Cast, ColumnarValue, Expr, ScalarUDF, ScalarUDFImpl, Signature,
        TypeSignature, Volatility,
    },
};

/// ScalarUDF to convert a binary CID into a string for easier inspection.
#[derive(Debug)]
pub struct CidString {
    signature: Signature,
}

impl Default for CidString {
    fn default() -> Self {
        Self::new()
    }
}

impl CidString {
    /// Construct new instance
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
    conclusion_feed
        .unnest_columns(&["previous"])
        .unwrap()
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
            Expr::ScalarFunction(ScalarFunction::new_udf(cid_string, vec![col("previous")]))
                .alias("previous"),
        ])
        .unwrap()
        .aggregate(
            vec![
                col("index"),
                col("event_type"),
                col("stream_cid"),
                col("stream_type"),
                col("controller"),
                col("dimensions"),
                col("event_cid"),
                col("data"),
            ],
            vec![array_agg(col("previous")).alias("previous")],
        )
        .unwrap()
        .sort(vec![col("index").sort(true, true)])
        .unwrap()
        .collect()
        .await
        .unwrap()
}
