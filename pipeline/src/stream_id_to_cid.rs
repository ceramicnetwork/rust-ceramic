//! Provides a scalar udf implementation that converts StreamId bytes to Cid bytes dropping the
//! stream type.

use std::{any::Any, sync::Arc};

use arrow::{
    array::{AsArray as _, BinaryBuilder, GenericByteArray},
    datatypes::GenericBinaryType,
};
use ceramic_core::StreamId;
use datafusion::{
    arrow::datatypes::DataType,
    common::{cast::as_binary_array, exec_datafusion_err},
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

make_udf_expr_and_func!(
    StreamIdToCid,
    stream_id_to_cid,
    stream_ids,
    "transforms a binary stream id to binary cid dropping the stream type.",
    stream_id_to_cid_udf
);

/// ScalarUDF to convert a binary StreamId into a string for easier inspection.
#[derive(Debug)]
pub struct StreamIdToCid {
    signature: Signature,
}

impl Default for StreamIdToCid {
    fn default() -> Self {
        Self::new()
    }
}

// Number of bytes in a typical Ceramic CID
const CID_BYTES: usize = 36;

impl StreamIdToCid {
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
    ) -> datafusion::common::Result<GenericByteArray<GenericBinaryType<i32>>> {
        let mut cids = BinaryBuilder::with_capacity(number_rows, CID_BYTES * number_rows);
        for stream_id in stream_ids {
            if let Some(stream_id) = stream_id {
                cids.append_value(
                    StreamId::try_from(stream_id)
                        .map_err(|err| exec_datafusion_err!("Error {err}"))?
                        .cid
                        .to_bytes(),
                );
            } else {
                cids.append_null()
            }
        }
        Ok(cids.finish())
    }
}

impl ScalarUDFImpl for StreamIdToCid {
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
                Box::new(DataType::Binary),
            ))
        } else {
            Ok(DataType::Binary)
        }
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        let number_rows = args.number_rows;
        let args = ColumnarValue::values_to_arrays(&args.args)?;

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

#[cfg(test)]
mod tests {

    use std::{str::FromStr as _, sync::Arc};

    use arrow::{
        array::{ArrayRef, BinaryDictionaryBuilder},
        datatypes::Int32Type,
        util::pretty::pretty_format_batches,
    };
    use arrow_schema::DataType;
    use ceramic_core::{StreamId, StreamIdType};
    use cid::Cid;
    use datafusion::{
        arrow::array::{BinaryBuilder, StructArray},
        prelude::{cast, col, SessionContext},
    };
    use expect_test::expect;
    use test_log::test;

    use crate::cid_string::cid_string;

    #[test(tokio::test)]
    async fn stream_id_to_cid() -> anyhow::Result<()> {
        let mut stream_ids = BinaryBuilder::new();
        stream_ids.append_value(
            StreamId {
                r#type: StreamIdType::Model,
                cid: Cid::from_str("baeabeiesm6etvlv3hwg7qamyqcqz2yzd3nrmkxjdsyn3nabdmmnvd3xq3u")?,
            }
            .to_vec(),
        );
        stream_ids.append_value(
            StreamId {
                r#type: StreamIdType::Model,
                cid: Cid::from_str("baeabeigafqyw7xmnk2nzh3d6bp5q5ba6rdnwg4kf6qn3z5vjeyqdvjb5w4")?,
            }
            .to_vec(),
        );
        stream_ids.append_value(
            StreamId {
                r#type: StreamIdType::Model,
                cid: Cid::from_str("baeabeibznwjbtkcon2waqbplr6tn5acuijknyhvwvgk2v3wpkd3zwyfmxi")?,
            }
            .to_vec(),
        );
        stream_ids.append_value(
            StreamId {
                r#type: StreamIdType::Model,
                cid: Cid::from_str("baeabeibyadj7ljmkn7jk3inxoxw2quvbrdofygq3qsxgb4bro7yqbd522e")?,
            }
            .to_vec(),
        );
        stream_ids.append_value(
            StreamId {
                r#type: StreamIdType::Model,
                cid: Cid::from_str("baeabeicpx4fgpnkizilh5j4ktex7lqrdduirxcw3ebrewicxhytqmmelqi")?,
            }
            .to_vec(),
        );
        let batch = StructArray::try_from(vec![(
            "stream_id",
            Arc::new(stream_ids.finish()) as ArrayRef,
        )])?;
        let ctx = SessionContext::new();
        let output = ctx
            .read_batch(batch.into())?
            .select(vec![cid_string(super::stream_id_to_cid(col("stream_id")))])?
            .collect()
            .await?;
        let output = pretty_format_batches(&output)?;
        expect![[r#"
            +-------------------------------------------------------------+
            | cid_string(stream_id_string(?table?.stream_id))             |
            +-------------------------------------------------------------+
            | baeabeiesm6etvlv3hwg7qamyqcqz2yzd3nrmkxjdsyn3nabdmmnvd3xq3u |
            | baeabeigafqyw7xmnk2nzh3d6bp5q5ba6rdnwg4kf6qn3z5vjeyqdvjb5w4 |
            | baeabeibznwjbtkcon2waqbplr6tn5acuijknyhvwvgk2v3wpkd3zwyfmxi |
            | baeabeibyadj7ljmkn7jk3inxoxw2quvbrdofygq3qsxgb4bro7yqbd522e |
            | baeabeicpx4fgpnkizilh5j4ktex7lqrdduirxcw3ebrewicxhytqmmelqi |
            +-------------------------------------------------------------+"#]]
        .assert_eq(&output.to_string());
        Ok(())
    }
    #[test(tokio::test)]
    async fn stream_id_to_cid_dict() -> anyhow::Result<()> {
        let mut stream_ids = BinaryDictionaryBuilder::<Int32Type>::new();
        let q3u = StreamId {
            r#type: StreamIdType::Model,
            cid: Cid::from_str("baeabeiesm6etvlv3hwg7qamyqcqz2yzd3nrmkxjdsyn3nabdmmnvd3xq3u")?,
        }
        .to_vec();
        let lqi = StreamId {
            r#type: StreamIdType::Model,
            cid: Cid::from_str("baeabeicpx4fgpnkizilh5j4ktex7lqrdduirxcw3ebrewicxhytqmmelqi")?,
        }
        .to_vec();
        let mxi = StreamId {
            r#type: StreamIdType::Model,
            cid: Cid::from_str("baeabeibznwjbtkcon2waqbplr6tn5acuijknyhvwvgk2v3wpkd3zwyfmxi")?,
        }
        .to_vec();

        stream_ids.append_value(q3u.clone());
        stream_ids.append_value(lqi.clone());
        stream_ids.append_value(lqi);
        stream_ids.append_value(mxi);
        stream_ids.append_value(q3u.clone());
        stream_ids.append_value(q3u);

        let batch = StructArray::try_from(vec![(
            "stream_id",
            Arc::new(stream_ids.finish()) as ArrayRef,
        )])?;
        let ctx = SessionContext::new();
        let output = ctx
            .read_batch(batch.into())?
            .select(vec![cid_string(cast(
                super::stream_id_to_cid(col("stream_id")),
                DataType::Binary,
            ))])?
            .collect()
            .await?;
        let output = pretty_format_batches(&output)?;
        expect![[r#"
            +-------------------------------------------------------------+
            | cid_string(stream_id_string(?table?.stream_id))             |
            +-------------------------------------------------------------+
            | baeabeiesm6etvlv3hwg7qamyqcqz2yzd3nrmkxjdsyn3nabdmmnvd3xq3u |
            | baeabeicpx4fgpnkizilh5j4ktex7lqrdduirxcw3ebrewicxhytqmmelqi |
            | baeabeicpx4fgpnkizilh5j4ktex7lqrdduirxcw3ebrewicxhytqmmelqi |
            | baeabeibznwjbtkcon2waqbplr6tn5acuijknyhvwvgk2v3wpkd3zwyfmxi |
            | baeabeiesm6etvlv3hwg7qamyqcqz2yzd3nrmkxjdsyn3nabdmmnvd3xq3u |
            | baeabeiesm6etvlv3hwg7qamyqcqz2yzd3nrmkxjdsyn3nabdmmnvd3xq3u |
            +-------------------------------------------------------------+"#]]
        .assert_eq(&output.to_string());
        Ok(())
    }
}
