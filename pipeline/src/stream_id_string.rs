//! Provides a scalar udf implementation that converts StreamId bytes to a utf8 string.

use std::{any::Any, sync::Arc};

use ceramic_core::StreamId;
use datafusion::{
    arrow::{
        array::{ArrayIter, ListBuilder, StringBuilder},
        datatypes::DataType,
    },
    common::{
        cast::{as_binary_array, as_list_array},
        exec_datafusion_err,
    },
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

/// ScalarUDF to convert a binary StreamId into a string for easier inspection.
#[derive(Debug)]
pub struct StreamIdString {
    signature: Signature,
}

impl Default for StreamIdString {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamIdString {
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

impl ScalarUDFImpl for StreamIdString {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "stream_id_string"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let stream_ids = as_binary_array(&args[0])?;
        let mut strs = StringBuilder::new();
        for stream_id in stream_ids {
            if let Some(stream_id) = stream_id {
                strs.append_value(
                    StreamId::try_from(stream_id)
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

/// ScalarUDF to convert a list of binary StreamIds into a list of strings for easier inspection.
#[derive(Debug)]
pub struct StreamIdStringList {
    signature: Signature,
}

impl Default for StreamIdStringList {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamIdStringList {
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

impl ScalarUDFImpl for StreamIdStringList {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_stream_id_string"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _args: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::new_list(DataType::Utf8, true))
    }
    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let all_stream_ids = as_list_array(&args[0])?;
        let mut strs = ListBuilder::new(StringBuilder::new());
        for stream_ids in ArrayIter::new(all_stream_ids) {
            if let Some(stream_ids) = stream_ids {
                let stream_ids = as_binary_array(&stream_ids)?;
                for stream_id in stream_ids {
                    if let Some(stream_id) = stream_id {
                        strs.values().append_value(
                            StreamId::try_from(stream_id)
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

#[cfg(test)]
mod tests {
    use super::{StreamIdString, StreamIdStringList};

    use std::{str::FromStr as _, sync::Arc};

    use arrow::{
        array::{ArrayRef, ListBuilder},
        util::pretty::pretty_format_batches,
    };
    use ceramic_core::StreamId;
    use datafusion::{
        arrow::array::{BinaryBuilder, StructArray},
        logical_expr::{expr::ScalarFunction, ScalarUDF},
        prelude::{col, Expr, SessionContext},
    };
    use expect_test::expect;
    use test_log::test;

    #[test(tokio::test)]
    async fn stream_id_string() -> anyhow::Result<()> {
        let mut stream_ids = BinaryBuilder::new();
        stream_ids.append_value(
            StreamId::from_str("k2t6wz4yhfp1mpv0qjl1aipdhcekgsyj8lpb2rv42q0k457bvedqsa7scmtsi4")?
                .to_vec(),
        );
        stream_ids.append_value(
            StreamId::from_str("k2t6wz4yhfp1m6aqsagtswyvan3kfnmgu84bzbr99nh0px38swqyegskd2agtj")?
                .to_vec(),
        );
        stream_ids.append_value(
            StreamId::from_str("k2t6wzhjp5kk1fuyrvdp2ednbfeawsdr1agd6qsoaim5vyliu5rex69dfyvr13")?
                .to_vec(),
        );
        stream_ids.append_value(
            StreamId::from_str("k2t6wzhjp5kk2g9iztfm7ol8lrbxoabkh3rn1u3nzm7mffdri2a1gpg5kcuge6")?
                .to_vec(),
        );
        stream_ids.append_value(
            StreamId::from_str("k2t6wzhjp5kk2efsq5cvws7s0nrcdn1de9h0rfemmz5d6vcavm340yv2sxxd9m")?
                .to_vec(),
        );
        let batch = StructArray::try_from(vec![(
            "stream_id",
            Arc::new(stream_ids.finish()) as ArrayRef,
        )])?;
        let stream_id_string = Arc::new(ScalarUDF::from(StreamIdString::new()));
        let ctx = SessionContext::new();
        let output = ctx
            .read_batch(batch.into())?
            .select(vec![Expr::ScalarFunction(ScalarFunction::new_udf(
                stream_id_string,
                vec![col("stream_id")],
            ))])?
            .collect()
            .await?;
        let output = pretty_format_batches(&output)?;
        expect![[r#"
            +----------------------------------------------------------------+
            | stream_id_string(?table?.stream_id)                            |
            +----------------------------------------------------------------+
            | k2t6wz4yhfp1mpv0qjl1aipdhcekgsyj8lpb2rv42q0k457bvedqsa7scmtsi4 |
            | k2t6wz4yhfp1m6aqsagtswyvan3kfnmgu84bzbr99nh0px38swqyegskd2agtj |
            | k2t6wzhjp5kk1fuyrvdp2ednbfeawsdr1agd6qsoaim5vyliu5rex69dfyvr13 |
            | k2t6wzhjp5kk2g9iztfm7ol8lrbxoabkh3rn1u3nzm7mffdri2a1gpg5kcuge6 |
            | k2t6wzhjp5kk2efsq5cvws7s0nrcdn1de9h0rfemmz5d6vcavm340yv2sxxd9m |
            +----------------------------------------------------------------+"#]]
        .assert_eq(&output.to_string());
        Ok(())
    }
    #[test(tokio::test)]
    async fn stream_id_string_list() -> anyhow::Result<()> {
        let mut stream_ids = ListBuilder::new(BinaryBuilder::new());
        stream_ids.values().append_value(
            StreamId::from_str("k2t6wz4yhfp1mpv0qjl1aipdhcekgsyj8lpb2rv42q0k457bvedqsa7scmtsi4")?
                .to_vec(),
        );
        stream_ids.values().append_value(
            StreamId::from_str("k2t6wz4yhfp1m6aqsagtswyvan3kfnmgu84bzbr99nh0px38swqyegskd2agtj")?
                .to_vec(),
        );
        stream_ids.append(true);
        stream_ids.values().append_value(
            StreamId::from_str("k2t6wzhjp5kk1fuyrvdp2ednbfeawsdr1agd6qsoaim5vyliu5rex69dfyvr13")?
                .to_vec(),
        );
        stream_ids.values().append_value(
            StreamId::from_str("k2t6wzhjp5kk2g9iztfm7ol8lrbxoabkh3rn1u3nzm7mffdri2a1gpg5kcuge6")?
                .to_vec(),
        );
        stream_ids.values().append_value(
            StreamId::from_str("k2t6wzhjp5kk2efsq5cvws7s0nrcdn1de9h0rfemmz5d6vcavm340yv2sxxd9m")?
                .to_vec(),
        );
        stream_ids.append(true);
        let batch = StructArray::try_from(vec![(
            "stream_ids",
            Arc::new(stream_ids.finish()) as ArrayRef,
        )])?;
        let stream_id_string_list = Arc::new(ScalarUDF::from(StreamIdStringList::new()));
        let ctx = SessionContext::new();
        let output = ctx
            .read_batch(batch.into())?
            .select(vec![Expr::ScalarFunction(ScalarFunction::new_udf(
                stream_id_string_list,
                vec![col("stream_ids")],
            ))])?
            .collect()
            .await?;
        let output = pretty_format_batches(&output)?;
        expect![[r#"
            +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            | array_stream_id_string(?table?.stream_ids)                                                                                                                                                       |
            +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
            | [k2t6wz4yhfp1mpv0qjl1aipdhcekgsyj8lpb2rv42q0k457bvedqsa7scmtsi4, k2t6wz4yhfp1m6aqsagtswyvan3kfnmgu84bzbr99nh0px38swqyegskd2agtj]                                                                 |
            | [k2t6wzhjp5kk1fuyrvdp2ednbfeawsdr1agd6qsoaim5vyliu5rex69dfyvr13, k2t6wzhjp5kk2g9iztfm7ol8lrbxoabkh3rn1u3nzm7mffdri2a1gpg5kcuge6, k2t6wzhjp5kk2efsq5cvws7s0nrcdn1de9h0rfemmz5d6vcavm340yv2sxxd9m] |
            +--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+"#]].assert_eq(&output.to_string());
        Ok(())
    }
}
