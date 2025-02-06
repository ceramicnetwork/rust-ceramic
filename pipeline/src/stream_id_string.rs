//! Provides a scalar udf implementation that converts StreamId bytes to a utf8 string.

use std::{any::Any, sync::Arc};

use arrow::{
    array::{Array, AsArray as _, GenericByteArray, GenericListArray},
    datatypes::{GenericBinaryType, GenericStringType},
};
use arrow_schema::Field;
use ceramic_core::StreamId;
use datafusion::{
    arrow::{array::StringBuilder, datatypes::DataType},
    common::{
        cast::{as_binary_array, as_list_array},
        exec_datafusion_err,
    },
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

// Number of bytes in a typical Ceramic Stream ID as a UTF8 string.
const STREAM_ID_STRING_BYTES: usize = 63;

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
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Binary]),
                    // Enumerate all possible dictionary key types.
                    // We handle dictionary types explicitly because we know that we are a 1:1
                    // transformation and can preserve the dictionary encoding.
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::Int8),
                        Box::new(DataType::Binary),
                    )]),
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::Int16),
                        Box::new(DataType::Binary),
                    )]),
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::Int32),
                        Box::new(DataType::Binary),
                    )]),
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::Int64),
                        Box::new(DataType::Binary),
                    )]),
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::UInt8),
                        Box::new(DataType::Binary),
                    )]),
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::UInt16),
                        Box::new(DataType::Binary),
                    )]),
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::UInt32),
                        Box::new(DataType::Binary),
                    )]),
                    TypeSignature::Exact(vec![DataType::Dictionary(
                        Box::new(DataType::UInt64),
                        Box::new(DataType::Binary),
                    )]),
                ],
                Volatility::Immutable,
            ),
        }
    }
    fn map_ids(
        &self,
        stream_ids: &GenericByteArray<GenericBinaryType<i32>>,
        number_rows: usize,
    ) -> datafusion::common::Result<GenericByteArray<GenericStringType<i32>>> {
        let mut strs =
            StringBuilder::with_capacity(number_rows, STREAM_ID_STRING_BYTES * number_rows);
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
        Ok(strs.finish())
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
    fn return_type(&self, args: &[DataType]) -> datafusion::common::Result<DataType> {
        if let DataType::Dictionary(key, _value) = &args[0] {
            Ok(DataType::Dictionary(
                key.to_owned(),
                Box::new(DataType::Utf8),
            ))
        } else {
            Ok(DataType::Utf8)
        }
    }
    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        if let Some(dict) = args[0].as_any_dictionary_opt() {
            let stream_ids = as_binary_array(dict.values())?;
            let ids = self.map_ids(stream_ids, number_rows)?;
            Ok(ColumnarValue::Array(dict.with_values(Arc::new(ids))))
        } else {
            let stream_ids = as_binary_array(&args[0])?;
            let ids = self.map_ids(stream_ids, number_rows)?;
            Ok(ColumnarValue::Array(Arc::new(ids)))
        }
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
    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        _number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let all_stream_ids = as_list_array(&args[0])?;
        // The list structure is not modified.
        // We can map over the values array and reuse the list offsets.
        let values = all_stream_ids.values();
        let stream_id_count = values.len();
        let mut new_values =
            StringBuilder::with_capacity(stream_id_count, STREAM_ID_STRING_BYTES * stream_id_count);
        let stream_ids = as_binary_array(&values)?;
        for stream_id in stream_ids {
            if let Some(stream_id) = stream_id {
                new_values.append_value(
                    StreamId::try_from(stream_id)
                        .map_err(|err| exec_datafusion_err!("Error {err}"))?
                        .to_string(),
                );
            } else {
                new_values.append_null()
            }
        }
        let new_list = GenericListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
            all_stream_ids.offsets().to_owned(),
            Arc::new(new_values.finish()),
            all_stream_ids.nulls().cloned(),
        )?;

        Ok(ColumnarValue::Array(Arc::new(new_list)))
    }
}

#[cfg(test)]
mod tests {
    use super::{StreamIdString, StreamIdStringList};

    use std::{str::FromStr as _, sync::Arc};

    use arrow::{
        array::{ArrayRef, BinaryDictionaryBuilder, ListBuilder},
        datatypes::Int32Type,
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
    async fn stream_id_string_dict() -> anyhow::Result<()> {
        let mut stream_ids = BinaryDictionaryBuilder::<Int32Type>::new();
        let si4 =
            StreamId::from_str("k2t6wz4yhfp1mpv0qjl1aipdhcekgsyj8lpb2rv42q0k457bvedqsa7scmtsi4")?
                .to_vec();
        let gtj =
            StreamId::from_str("k2t6wz4yhfp1m6aqsagtswyvan3kfnmgu84bzbr99nh0px38swqyegskd2agtj")?
                .to_vec();
        let r13 =
            StreamId::from_str("k2t6wzhjp5kk1fuyrvdp2ednbfeawsdr1agd6qsoaim5vyliu5rex69dfyvr13")?
                .to_vec();

        stream_ids.append_value(si4.clone());
        stream_ids.append_value(gtj.clone());
        stream_ids.append_value(gtj);
        stream_ids.append_value(r13);
        stream_ids.append_value(si4.clone());
        stream_ids.append_value(si4);

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
            | k2t6wz4yhfp1m6aqsagtswyvan3kfnmgu84bzbr99nh0px38swqyegskd2agtj |
            | k2t6wzhjp5kk1fuyrvdp2ednbfeawsdr1agd6qsoaim5vyliu5rex69dfyvr13 |
            | k2t6wz4yhfp1mpv0qjl1aipdhcekgsyj8lpb2rv42q0k457bvedqsa7scmtsi4 |
            | k2t6wz4yhfp1mpv0qjl1aipdhcekgsyj8lpb2rv42q0k457bvedqsa7scmtsi4 |
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
