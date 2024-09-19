//! Common utilities for testing APIs using Arrow [`RecordBatch`]es and related data structures.
#![warn(missing_docs)]

use std::{any::Any, sync::Arc};

use arrow::{
    array::{ArrayIter, ListBuilder, StringBuilder},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use cid::Cid;
use datafusion::{
    common::{
        cast::{as_binary_array, as_list_array},
        exec_datafusion_err,
    },
    dataframe::DataFrame,
    execution::context::SessionContext,
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

/// ScalarUDF to convert a binary CID into a string for easier inspection.
#[derive(Debug)]
pub struct CidStringList {
    signature: Signature,
}

impl Default for CidStringList {
    fn default() -> Self {
        Self::new()
    }
}

impl CidStringList {
    /// Construct new instance
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::new_list(DataType::Binary, true)]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CidStringList {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_cid_string"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::new_list(DataType::Utf8, true))
    }
    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let all_cids = as_list_array(&args[0])?;
        let mut strs = ListBuilder::new(StringBuilder::new());
        for cids in ArrayIter::new(all_cids) {
            if let Some(cids) = cids {
                let cids = as_binary_array(&cids)?;
                for cid in cids {
                    if let Some(cid) = cid {
                        strs.values().append_value(
                            Cid::read_bytes(cid)
                                .map_err(|err| exec_datafusion_err!("Error {err}"))?
                                .to_string(),
                        );
                    } else {
                        strs.values().append_null()
                    }
                }
                strs.append(true)
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
